##################################################
# Script for aggregating capacity from parcel level
# to a higher geography level. 
# As input it uses the parcel file generated via parcels_capacity.R,
# or via merge_files_for_different_ratios.R (which merges multiple 
# parcel-level capacity files).
# Hana Sevcikova, PSRC, 2018-10-02
# updated on 2019-01-07 
##################################################


#input.file <- "CapacityPcl_res50-2018-10-02.csv"
#input.file <- "/Volumes/DataTeam/Projects/V2050/SEIS/Data_Support/script_input/CapacityIndicatorPcl_res50.csv"
input.file <- "CapacityPcl_res0-50-100-2019-01-07.csv"

# to which geography to aggregate
geography <- "city" 
geography <- "growth_center"
#geography <- NULL # set to NULL if aggregating to the whole region

save <- FALSE # save in csv file
out.file.prefix <- paste0("Capacity0-50-100_", if(is.null(geography)) "region" else geography, "-", Sys.Date())

# Where are csv lookup tables ("parcels_geos.csv", geo.file.name below)
#lookup.path <- "J:/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"
lookup.path <- "/Volumes/DataTeam/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"
compute.difference <- TRUE # should difference between end year and base be computed

# Merge with a dataset of the higher geography (should be csv)
# (set to NULL if no merging desired)
geo.file.name <- "cities_with_rgs.csv"
geo.file.name <- "growth_centers.csv"

# which attributes to extract from geo.file.name
geo.file.attributes <- c("county", "Juris") 
geo.file.attributes <- c("county")
geo.file.attributes <- c("name", "city_id")
###################

capacity.column.base <- c("DU", "NRSQF", "JOBSP", "BLSQF")

library(data.table)
geo.id <- if(is.null(geography)) c() else paste0(geography, "_id")

# read capacity file and geo lookup and merge with geo.id
pcls <- fread(input.file)
pcls.geo <- fread(file.path(lookup.path, "parcels_geos.csv"))
pcl <- merge(pcls, pcls.geo[, c("parcel_id", geo.id), with = FALSE], by = "parcel_id")

# aggregate
aggr <- pcl[, lapply(.SD, sum), by = geo.id, .SDcols = setdiff(colnames(pcl), c("parcel_id", geo.id))]

# compute difference columns
assign.difference <- function(colbase, dt) {
  for(col in grep(paste0(colbase, "capacity"), colnames(dt), value = TRUE)) {
    suffix <- strsplit(col, "capacity")[[1]]
    suffix <- if(length(suffix) > 1) suffix[2] else ""
    dt[[paste0(colbase, "diff", suffix)]] = dt[[col]] - dt[[paste0(colbase, "base")]]
  }
  dt
}
if(compute.difference) 
  for(col in capacity.column.base) aggr <- assign.difference(col, aggr)

# merge with a given geo file (e.g. to add geo names)
if(!is.null(geo.file.name) && !is.null(geography)) {
  geodf <- unique(fread(file.path(lookup.path, geo.file.name))[, c(geo.id, geo.file.attributes), with = FALSE])
  aggr <- merge(geodf[, c(geo.id, geo.file.attributes), with = FALSE], aggr, by = geo.id)
  # set to 0 where NAs
  for(colbase in capacity.column.base) {
    idx <- which(is.na(dt[[paste0(colbase, "base")]]))
    cols <- grep(paste0(colbase, "capacity|", colbase, "diff|", colbase, "base"), colnames(aggr), value = TRUE)
    for(col in cols) aggr[[col]][idx] = 0
  }
}

# save to csv
if(save)
  fwrite(aggr, file = paste0(out.file.prefix, ".csv"))

