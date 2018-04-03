# Script for computing new development of DUs and job_capacity by city & county.
# Can be used to obtain maximum capacity, when applied to a run 
# with unlimited capacity.
# It uses an indicator of new buildings and subtracts existing 
# development from the redeveloped parcels.
# Hana Sevcikova
# 2018/04/03

library(data.table)

# The data directory requires the following csv files (exported from the base year):
# buildings2014.csv, parcels2014.csv, cities.csv, building_sqft_per_job.csv
data.dir <- "../data"

# Location of the new_buildings indicator
indicator.dir <- "~/d6$/opusgit/urbansim_data/data/psrc_parcel/runs/run_100.run_2018_04_03_09_51/indicators"

# End year of the simulation
year <- 2015

# Write results into a file? If yes, into what file?
write.file <- TRUE
file.name <- "UScapacity_cities.csv"
    
# load new buildings (copied from the indicators directory)
bld.new <- fread(file.path(indicator.dir, 
                         paste0("building__dataset_table__new_buildings__", year, ".tab")))

# load 2014 baseyar buildings
bld14 <- fread(file.path(data.dir, "buildings2014.csv"))

# load 2014 parcels, cities and building_sqft_per_job datasets
pcl <- fread(file.path(data.dir, "parcels2014.csv"))
cities <- fread(file.path(data.dir, "cities.csv"))
bsqft.per.job <- fread(file.path(data.dir, "building_sqft_per_job.csv"))

# select only relevant attributes
bld.new[, job_capacity := 0]
bld <- bld.new[, .(building_id, parcel_id, building_type_id, 
                 residential_units, non_residential_sqft, 
                 building_sqft, job_capacity)]

# add zone_id and merge with bsqft.per.job
bld <- merge(bld, pcl[,.(parcel_id, zone_id)], by="parcel_id")
bld <- merge(bld, bsqft.per.job, by=c("building_type_id", "zone_id"), all.x = TRUE)

# compute job capacity for non-res buildings
bld[!is.na(building_sqft_per_job), job_capacity := ifelse(building_type_id %in% c(19,4,12), 0, 
                                                          pmax(1, round(non_residential_sqft / building_sqft_per_job)))]

# sum over parcels
pclstock <- bld[, .(pcl_resunits = sum(residential_units), 
                    pcl_nonres_sqft=sum(non_residential_sqft),
                    pcl_bldsqft = sum(building_sqft), 
                    job_capacity = sum(job_capacity)), 
                by = parcel_id]

# add city_id
pclstock <- merge(pclstock, pcl[,.(parcel_id, city_id)], by = "parcel_id")

# sum new developemnt by city
city.stock <- pclstock[, .(DU = sum(pcl_resunits), 
                           nonres = sum(pcl_nonres_sqft),
                            job_capacity = sum(job_capacity, na.rm = TRUE)), 
                       by = "city_id"]

# base year buildings on parcels re-developed by 2050 
bld.base <- bld14[parcel_id %in% pclstock$parcel_id]
bld.base <- merge(bld.base, pcl[, .(parcel_id, zone_id, city_id)], 
                    by = "parcel_id")
# sum by city_id
city.stock.base <- bld.base[, .(resunits14 = sum(residential_units), 
                                nonres14 = sum(non_residential_sqft), 
                                jobcap14 = sum(job_capacity)),
                            by = city_id]

# assure all cities are included by merging with the cities table
# (it also sorts the cities which we want)
city.stock <- merge(cities[, .(city_id)], city.stock, 
                    by = "city_id", all.x = TRUE)
# replace NA with 0
for(col in c("DU", "nonres", "job_capacity"))
    city.stock[is.na(get(col)), (col) := 0]

# put base year data into the same order as new development
city.stock.base <- merge(city.stock[, .(city_id)], 
                         city.stock.base, 
                         by = "city_id", all.x = TRUE)
# replace NA with 0
for(col in c("resunits14", "nonres14", "jobcap14"))
    city.stock.base[is.na(get(col)), (col) := 0]

# get the final numbers by subtracting base from new development
total <- city.stock
total[, DU := round(DU - city.stock.base$resunits14)]
total[, job_capacity := job_capacity - city.stock.base$jobcap14]
total[, nonres := nonres - city.stock.base$nonres14]

# merge with city names and counties
total <- merge(cities[ ,.(city_id, county_id, city_name)], 
               total, by = "city_id")
# if nonres not required, delete that column
total[, nonres := NULL]

# summarize by counties
counties.df <- data.frame(county_id = c(33, 35, 53, 61),
                          county_name = c("King", "Kitsap", 
                                          "Pierce", "Snohomish"))
total.cnty <- merge(total, counties.df, by = "county_id")
total.cnty <- total.cnty[ , .(DU = sum(DU), job_capacity = sum(job_capacity)), 
                          by = c("county_id", "county_name")]

# write into a file
if(write.file)
    fwrite(total, file = file.name)

total
total.cnty
