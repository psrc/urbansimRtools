# Script to do spatial joins for data requests - user directs path to custom geography shapefile and it joins to LUV-it parcel points_joined
# Still a big work in progress
# Mark Simonson 1-2-24

## Helpful resource links

## https://gis.stackexchange.com/questions/19064/opening-shapefile-in-r
## https://ryanpeek.org/2019-04-29-spatial-joins-in-r/
## https://tmieno2.github.io/R-as-GIS-for-Economists/int-vv.html
## https://cengel.github.io/R-spatial/spatialops.html.  For CRS, need it in proj4 format
## https://github.com/psrc/luv/blob/master/LUVit/assemble.R  Examples for adding sheets to Excel workbook and export
## https://uw-madison-datascience.github.io/r-raster-vector-geospatial/08-vector-plot-shapefiles-custom-legend/index.html plotting
## https://psrc.github.io/intro-leaflet/  # Craigs lessons from 2021
## https://learn.r-journalism.com/en/mapping/leaflet_maps/leaflet/
## https://rstudio.github.io/leaflet/colors.html ## Coloring the leaflet circles by sa_id

library(sf)
library(leaflet)
library(dplyr)
library(ggplot2)
library(data.table)
library(tmap)
library(openxlsx2)
library(RColorBrewer)
library(wesanderson)

## Standard Inputs - don't need to change

## Hana:  Use file.path to set a directory and use that instead of having to change multiple times

luvit_parcel_points <- read_sf(dsn = "J:/Projects/LandUseVision/LUV.3/Data_Requests/Core_GIS_shapefiles", layer ="luvit_parcels")
sourcedir_flat_indicators <- "J:/Projects/LandUseVision/LUV.3/Data_Requests/flattened_parcel_files"
sourcedir_spreadsheet_template <- "J:/Projects/LandUseVision/LUV.3/Data_Requests"
dr_template <- "LUVit_DR_Template_2.xlsx"

## User inputs:  Polygons provided by requester - the tabulation geography - and the LUV-it parcel centroid file

dr_polygons <- read_sf(dsn = "J:/Projects/LandUseVision/LUV.3/Data_Requests/Orting_RCooper_Parametrix_Dec18/GIS", layer ="Orting_Water_System_Area")
sourcedir_request_storage <- "J:/Projects/LandUseVision/LUV.3/Data_Requests/Orting_RCooper_Parametrix_Dec18"
years <- c(2044)  # Just for testing purposes, single year
#years <- c(2018,2020,2025,2030,2035,2040,2044,2050)
unique_field_name <- "OBJECTID"
output_file_name <- "TEST_LUVit_DR_Orting_Parametrix_Dec2023_Working.xlsx"

##  Check projections of imported files with plot commands for visual confirmation

#print(c("projection system of data request polygons: ",st_crs(dr_polygons)))
#print(c("projection system of luv-it parcel centroids: ",st_crs(luvit_parcel_points)))

#plot(dr_polygons$geometry)
#plot(luvit_parcel_points$geometry)

## Convert polygon file to same projection system as the LUV-it parcel centroids

dr_polygons_crs <- st_transform(dr_polygons,st_crs(luvit_parcel_points))

## QC checks of plots and coordinate systems after conversion

#plot(dr_polygons_crs$geometry)
#print(st_crs(dr_polygons))
#print(st_crs(luvit_parcel_points))

## Spatial join parcel points to polygons

points_joined <- st_join(luvit_parcel_points, left=TRUE, dr_polygons_crs)

## Subsetting parcel centroids to only those that fall within study area - note OJBECTID will need to be edited to line up with 
## the polygon file used for this request

points_joined <- points_joined %>% 
  rename(
    sa_id = unique_field_name
  )

points_joined_in_dr_polygons <- points_joined %>% filter(!is.na(sa_id))

## Extract just the columns needed from the subset of parcels in study area

dr_parcels <- data.frame(points_joined_in_dr_polygons %>% 
                           select(parcel_id, x_coord_sp, y_coord_sp, sa_id))

## Rename key id or primary key field in polygon file to 'sa_id' for 'study area id'

## Import flattened parcel indicator files, join with parcels in study area, and summarize total and by study area polygons

setwd(sourcedir_flat_indicators)
results_total_sa <- NULL
results_by_sa <- NULL

for(year in years){
  print(year)
  luvit <- data.frame(fread(paste0("parcel__dataset_table__luvit_parcels_flattened__",year,".csv"), header = TRUE, sep = ","))
  dr_parcels_processed <- left_join(dr_parcels,luvit,by = join_by(parcel_id == parcel_id))
  summary_total_sa <- dr_parcels_processed %>% 
    summarise(year = year, hhs = sum(hhs_all),hh_pop = sum(population), jobs = sum(jobs_all), res_con = sum(res_con_jobs_all),
      man_wtu = sum(man_wtu_jobs_all), ret_foodserv = sum(ret_foodserv_jobs_all), fire_services = sum(fire_services_jobs_all),
      gov = sum(gov_jobs_all), educ = sum(educ_jobs_all))
  results_total_sa <- rbind(results_total_sa,summary_total_sa)
  summary_by_sa <- dr_parcels_processed %>% 
    group_by(sa_id) %>%
    summarise(year = year, hhs = sum(hhs_all),hh_pop = sum(population), jobs = sum(jobs_all), res_con = sum(res_con_jobs_all),
              man_wtu = sum(man_wtu_jobs_all), ret_foodserv = sum(ret_foodserv_jobs_all), fire_services = sum(fire_services_jobs_all),
              gov = sum(gov_jobs_all), educ = sum(educ_jobs_all))
  results_by_sa <- rbind(results_by_sa,summary_by_sa)
}

## Export results to a copy of the template spreadsheet renamed and stored in data request working directory

setwd(sourcedir_spreadsheet_template)
wb <- wb_load(dr_template)
wb <- wb_add_worksheet(wb,"Summary_total_SA")
wb <- wb_add_worksheet(wb,"Summary_by_SA")
wb <- wb_add_data(wb,"Summary_total_SA",results_total_sa)
wb <- wb_add_data(wb,"Summary_by_SA",results_by_sa)
setwd(sourcedir_request_storage)
wb$save(output_file_name)

## Convert parcels back to sf compatible file
dr_parcels_2 <- st_as_sf(dr_parcels)

ggplot() + 
  geom_sf(data = dr_polygons_crs) +
  geom_sf(data = dr_parcels_2, aes(color = sa_id)) 
## Two different ways of plotting parcels by sa_id across color spectrum for QC
#  scale_color_gradient(colours = wesanderson::wes_palette("Zissou1", 100, type = "continuous"))
#  scale_color_gradient(low = "yellow", high = "brown")

## TODO - move to Leaflet to add OpenStreetMap reference layers
## Code below this line not working yet

dr_polygons <- dr_polygons %>% 
  st_transform(4326)

dr_parcels_2 <- dr_parcels_2 %>%
  st_transform(4326)

pal <- colorFactor(topo.colors(10), dr_parcels_2$sa_id)


dr_map <- leaflet() %>% 
   # addTiles() %>% 
   addProviderTiles(providers$CartoDB.DarkMatter) %>%
    addEasyButton(easyButton(
    icon="fa-globe", title="Region",
    onClick=JS("function(btn, map){map.setView([47.615,-122.257],8.5); }"))) %>% 

  addPolygons(data=dr_polygons,
              weight = 1,
              ) %>%   

  addCircles(data=dr_parcels_2,
             weight = 2,
             radius = 2,
             fill = TRUE,
             opacity = 1,
             popup = ~sa_id,
             color = ~pal(sa_id),
             group = (dr_parcels_2))
  
dr_map



