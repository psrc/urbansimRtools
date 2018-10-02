##################################################
# Script for aggregating capacity from parcel level
# to a higher geography level. 
# It uses the parcel file generated with parcels_capacity.R
# as its input.
# Hana Sevcikova, PSRC, 2018-10-02
##################################################


input.file <- "CapacityPcl_res50-2018-10-02.csv"
#input.file <- "/Volumes/DataTeam/Projects/V2050/SEIS/Data_Support/script_input/CapacityIndicatorPcl_res50.csv"

# to which geography to aggregate
geography <- "county" 
geography <- NULL # set to NULL if aggregating to the whole region

save <- FALSE # save in csv file
out.file.prefix <- paste0("Capacity_", if(is.null(geography)) "region" else geography, "-", Sys.Date())

# Where are csv lookup tables ("parcels_geos.csv", geo.file.name below)
#lookup.path <- "J:/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"
lookup.path <- "/Volumes/DataTeam/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"

compute.difference <- TRUE # should difference between end year and base be computed

# Merge with a dataset of the higher geography (should be csv)
# (set to NULL if no merging desired)
geo.file.name <- "cities_with_rgs.csv"
geo.file.attributes <- c("county", "Juris") # which attributes to extract from geo.file.name
geo.file.attributes <- c("county")
###################

library(data.table)
geo.id <- if(is.null(geography)) c() else paste0(geography, "_id")

# read capacity file and geo lookup and merge with geo.id
pcls <- fread(input.file)
pcls.geo <- fread(file.path(lookup.path, "parcels_geos.csv"))
pcl <- merge(pcls, pcls.geo[, c("parcel_id", geo.id), with = FALSE], by = "parcel_id")

# aggregate
aggr <- pcl[, lapply(.SD, sum), by = geo.id, .SDcols = setdiff(colnames(pcl), c("parcel_id", geo.id))]

# compute differences
if(compute.difference) {
  aggr[, `:=`(DUdiff = DUcapacity - DUbase, NRSQFdiff = NRSQFcapacity - NRSQFbase, 
              JOBSPdiff = JOBSPcapacity - JOBSPbase, BLSQFdiff = BLSQFcapacity - BLSQFbase)]
}

# merge with a given geo file (e.g. to add geo names)
if(!is.null(geo.file.name) && !is.null(geography)) {
  geodf <- unique(fread(file.path(lookup.path, geo.file.name))[, c(geo.id, geo.file.attributes), with = FALSE])
  aggr <- merge(geodf[, c(geo.id, geo.file.attributes), with = FALSE], aggr, by = geo.id)
  aggr[is.na(DUbase), `:=`(DUbase = 0, DUcapacity = 0, DUdiff = 0)]
  aggr[is.na(NRSQFbase), `:=`(NRSQFbase = 0, NRSQFcapacity = 0, NRSQFdiff = 0)]
  aggr[is.na(JOBSPbase), `:=`(JOBSPbase = 0, JOBSPcapacity = 0, JOBSPdiff = 0)]
  aggr[is.na(BLSQFbase), `:=`(BLSQFbase = 0, BLSQFcapacity = 0, BLSQFdiff = 0)]
}

if(save)
  fwrite(aggr, file = paste0(out.file.prefix, ".csv"))

