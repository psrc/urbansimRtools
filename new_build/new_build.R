# Script for computing new development and total build of DUs and job_capacity by city & county.
# Can be used to obtain maximum capacity, when applied to a run 
# with unlimited capacity.
# It uses an indicator of new buildings. 
# For obtaining new development, it subtracts existing 
# development on redeveloped parcels from new build.
# For obtaining total build, it adds new development to existing build.
# Hana Sevcikova
# 2018/04/03

library(data.table)

# The data directory requires the following csv files (exported from the base year):
# buildings2014.csv, parcels2014.csv, cities.csv, building_sqft_per_job.csv
data.dir <- "../data"

# Location of the new_buildings indicator
indicator.dir <- "~/d6$/opusgit/urbansim_data/data/psrc_parcel/runs/run_101.run_2018_04_03_15_31/indicators"

# End year of the simulation
year <- 2015

# Write results into a file? If yes, into what file?
write.file <- TRUE
file.name.new <- "USadded_capacity_cities_run101.csv" # file name for new development
file.name <- "UStotal_capacity_cities_run101.csv" # file name for total capacity
    
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

# sum new development by city
city.new <- pclstock[, .(DU = sum(pcl_resunits), 
                         nonres = sum(pcl_nonres_sqft),
                         job_capacity = sum(job_capacity, na.rm = TRUE)), 
                       by = "city_id"]

# add zone_id and city_id to base-year buildings
bld.base <- merge(bld14, pcl[, .(parcel_id, zone_id, city_id)], 
                    by = "parcel_id")

# sum existing stock by city_id: all buildings
city.base <- bld.base[, .(resunits14 = sum(residential_units), 
                        nonres14 = sum(non_residential_sqft), 
                        jobcap14 = sum(job_capacity)),
                    by = "city_id"]

# sum existing stock by city_id: buildings on re-developed parcels only
city.base.redev <- bld.base[parcel_id %in% pclstock$parcel_id, 
                            .(resunits14 = sum(residential_units), 
                                nonres14 = sum(non_residential_sqft), 
                                jobcap14 = sum(job_capacity)),
                            by = city_id]

# assure all cities are included by merging with the cities table
# (it also sorts the cities which we want)
city.new <- merge(cities[, .(city_id)], city.new, 
                    by = "city_id", all.x = TRUE)
# replace NA with 0
for(col in c("DU", "nonres", "job_capacity"))
    city.new[is.na(get(col)), (col) := 0]

# put base year data into the same order as new development
city.base <- merge(city.new[, .(city_id)], city.base, 
                         by = "city_id", all.x = TRUE)
city.base.redev <- merge(city.new[, .(city_id)], city.base.redev, 
                   by = "city_id", all.x = TRUE)

# replace NA with 0
for(col in c("resunits14", "nonres14", "jobcap14")) {
    city.base[is.na(get(col)), (col) := 0]
    city.base.redev[is.na(get(col)), (col) := 0]
}

counties.df <- data.frame(county_id = c(33, 35, 53, 61),
                          county_name = c("King", "Kitsap", 
                                          "Pierce", "Snohomish"))
# New development
# ===============
# get the final numbers by subtracting base from new development
total.new <- copy(city.new)
total.new[, DU := round(DU - city.base.redev$resunits14)]
total.new[, job_capacity := job_capacity - city.base.redev$jobcap14]
total.new[, nonres := nonres - city.base.redev$nonres14]

# merge with city names and counties
total.new <- merge(cities[ ,.(city_id, county_id, fips_rgs_id, city_name)], 
               total.new, by = "city_id")

# summarize by counties
total.new.cnty <- merge(total.new, counties.df, by = "county_id")
total.new.cnty <- total.new.cnty[ , .(DU = sum(DU), job_capacity = sum(job_capacity)), 
                          by = c("county_id", "county_name")]

# Total stock
# ===========
# get total stock by adding base and new development from above
total <- copy(total.new)
total[, DU := round(DU + city.base$resunits14)]
total[, job_capacity := job_capacity + city.base$jobcap14]
total[, nonres := nonres + city.base$nonres14]

# summarize by counties
total.cnty <- merge(total, counties.df, by = "county_id")
total.cnty <- total.cnty[ , .(DU = sum(DU), job_capacity = sum(job_capacity)), 
                                  by = c("county_id", "county_name")]

# write into a file
if(write.file) {
    # if nonres not required, delete that column
    total.new[, nonres := NULL]
    total[, nonres := NULL]
    fwrite(total.new, file = file.name.new)
    fwrite(total, file = file.name)
}

total.new
total.new.cnty
total.cnty
