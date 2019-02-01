# Tool for creating quick interactive polygon maps  
# Hana Sevcikova, PSRC
# 01/31/2019

library(rgdal)
library(tmap)
library(tmaptools)

create_shape <- function(df, geo.id, layer, layer.geo.id = geo.id, path.to.shpfiles) {
  # create an sp object
  shp <- readOGR(dsn = path.to.shpfiles, layer = layer)
  shp <- spTransform(shp, CRSobj=CRS("+init=epsg:4326"))
  # merge with data
  merge(shp, df, by.x = layer.geo.id, by.y = geo.id)
}

map_polygon <- function(shp, map.attribute, interactive = TRUE, ...) {
  if(interactive) tmap_mode("view")
  else tmap_mode("plot")
  tm_basemap("OpenStreetMap") + tm_shape(shp) + tm_fill(col = map.attribute, ...)
}

# toy example with city data 
exdata <- data.frame(city_id = c(9, 13, 15, 59), 
                     city_name = c("Seattle", "Bellevue", "Redmond", "Tacoma"),
                     indicator = c(100, 70, 50, 120))

# first merge data with the shapefile 
shp <- create_shape(exdata, 
                    geo.id = "city_id", # column in exdata used for the join 
                    layer = "JURIS_2014_WGS84", 
                    layer.geo.id = "city_id", # column in the shapefile used for the join 
                    path.to.shpfiles = "J:/Projects/ShapefilesWG84" # where are the shapefiles located
                    )
# Map the column "indicator"
m <- map_polygon(shp, "indicator", 
                 # further arguments to the function tm_fill, see ?tm_fill
                 alpha = 0.7, style = "pretty", popup.vars = c("city_name", "indicator")
                 )
print(m)
