##################################################
# Script for aggregating capacity from parcel level
# to a higher geography level. It allows to aggregate to sub-levels 
# within a geography, e.g. city and the tod within city. 
# One can also split the aggregation by parcels' density level.
# As input it uses the parcel file generated via parcels_capacity.R,
# or via merge_files_for_different_ratios.R (which merges multiple 
# parcel-level capacity files).
# Hana Sevcikova, PSRC, 2019-3-06
##################################################

input.file <- "J:/Projects/Parcel\ Data/Capacity/inputs/CapacityPcl_res0-100-2019-01-08.csv"
input.file <- "CapacityPcl_res0-100-2019-01-08.csv"

# to which geographies to aggregate
geography <- c("city", "tod") 
#geography <- "growth_center"
#geography <- NULL # set to NULL if aggregating to the whole region

# parcel density grouping (either NA, exact number or a right-open interval)
density.splits <- list(NA, 0, c(1e-6, 12), c(12, 50), c(50, 99999)) # NA means no split
#density.splits <- NA

# names used in the suffix of the output files (should be the same length as density.splits)
density.names <- c("all", "0", "lt12", "12_50", "50+")

save <- TRUE # save in csv file
output.dir <- paste0("capres", Sys.Date()) # directory for the outputs
# prefix of the output files
out.file.prefix <- paste0("Capacity0-100_", if(is.null(geography)) "region" else paste(geography, collapse = "_"))

# Where are csv lookup tables ("parcels_geos.csv", geo.file.name below)
#lookup.path <- "J:/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"
lookup.path <- "/Volumes/DataTeam/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"
parcel.lookup.name <- "parcels_geos_mic.csv"

compute.difference <- TRUE # should difference between end year and base be computed

# Merge with datasets of the higher geography (should be csv)
# (set to NULL if no merging desired)
# (list names must correspond to values in "geography")
geo.file.name <- list(city = "cities_with_rgs.csv", tod = "tods.csv")

# Which attributes to extract from geo.file.name
# (list names must correspond to values in "geography")
geo.file.attributes <- list(city = c("county", "Juris"), tod = "tod_name")

###################
# Start the actual computation
###################

library(data.table)

# read capacity file and geo lookup 
pcls <- fread(input.file)
pcls.geo <- fread(file.path(lookup.path, parcel.lookup.name))

capacity.column.base <- c("DU", "NRSQF", "JOBSP", "BLSQF")

geo.ids <- if(is.null(geography)) c() else paste0(geography, "_id")

# merge capacity dataset with geo.ids
pcl <- merge(pcls, pcls.geo[, c("parcel_id", "plan_type_id", geo.ids), with = FALSE], by = "parcel_id")

# compute difference columns
assign.difference <- function(colbase, dt) {
  for(col in grep(paste0(colbase, "capacity"), colnames(dt), value = TRUE)) {
    suffix <- strsplit(col, "capacity")[[1]]
    suffix <- if(length(suffix) > 1) suffix[2] else ""
    dt[[paste0(colbase, "diff", suffix)]] = dt[[col]] - dt[[paste0(colbase, "base")]]
  }
  dt
}

# extract maximum density if needed
if(any(!is.na(density.splits))) {
  # load constraints to get density
  constraints <- fread(file.path(lookup.path, "development_constraints.csv"))
  constr <- constraints[, .(max_dens = max(maximum)), by = .(plan_type_id, constraint_type)]
  # get parcels residential density
  pcl[constr[constraint_type == "units_per_acre"], max_density := i.max_dens, on = "plan_type_id"]
  pcl[is.na(max_density), max_density := 0]
}

# create the output directory if needed
if(save && !dir.exists(output.dir)) dir.create(output.dir)

# iterate over density splits, including no split
for(idens in seq_along(density.splits)) {
  dens <- density.splits[[idens]]
  # filter parcels
  if(length(dens) == 1) { # density is an exact number (no interval)
    if(!is.na(dens))
      tpcl <- pcl[max_density == dens]
    else tpcl <- copy(pcl) # all parcels (no split)
  } else tpcl <- pcl[max_density >= dens[1] & max_density < dens[2]] # density is right-open interval 
  
  # aggregate
  aggr <- tpcl[, lapply(.SD, sum), by = geo.ids, .SDcols = setdiff(colnames(tpcl), c("parcel_id", geo.ids))]

  if(compute.difference) 
    for(col in capacity.column.base) aggr <- assign.difference(col, aggr)

  # merge with a given geo file (e.g. to add geo names)
  if(!is.null(geo.file.name) && !is.null(geography)) {
    for(igeo in rev(seq_along(geography))) {
      geo <- geography[igeo]
      geodf <- unique(fread(file.path(lookup.path, geo.file.name[[geo]]))[, c(geo.ids[igeo], geo.file.attributes[[geo]]), 
                                                                          with = FALSE])
      aggr <- merge(geodf[, c(geo.ids[igeo], geo.file.attributes[[geo]]), with = FALSE], aggr, by = geo.ids[igeo],
                    sort = FALSE)
    }
    # set to 0 where NAs
    for(colbase in capacity.column.base) {
      idx <- which(is.na(aggr[[paste0(colbase, "base")]]))
      cols <- grep(paste0(colbase, "capacity|", colbase, "diff|", colbase, "base"), colnames(aggr), value = TRUE)
      for(col in cols) aggr[[col]][idx] = 0
    }
  }

  # save to csv
  if(save)
    fwrite(aggr, file = file.path(output.dir, paste0(out.file.prefix, "_", density.names[idens], 
                               "-", Sys.Date(), ".csv")))
}
