library(data.table)
library(readxl)

# Settings
#==========
# were are parcels and various Xwalk tables, exported from the base year DB
data.dir <- "../../data/BY2018"

# Results from the script compute_capacity_with_pop.R
capacity.file <- "RGC_capacity_data_2024-03-27.csv"

# which developable factor and residential ratio we want in the summary 
# (must be present in the capacity file; if not, rerun compute_capacity_with_pop.R
#  with the right settings)
developable.factor <- 2
res.ratios <- c(40, 50, 60)

# Load data
# ==========
# read capacity results
capacity <- fread(capacity.file)
# filter to RGCs only and desired developeble factor and res ratio
capacity <- capacity[growth_center_id > 0 & developfac == developable.factor & res_ratio %in% res.ratios]
capacity50 <- capacity[res_ratio == 50][, res_ratio := NULL]

# read parcels file and update with the updated RGCs
pcls <- fread(file.path(data.dir, "parcels.csv"))
pcls.upd <- fread(file.path(data.dir, "parcels18_with_updated_growth_center_id.csv"))
pcls <- merge(pcls, pcls.upd, by = "parcel_id")

# Processing
# aggregate RGCs parcels and acres
# TODO aggregate by county AND Metro/Urban 
aggr.size <- pcls[, .(Nparcels = .N, size = round(sum(as.double(parcel_sqft))/43560)), by = c("county_id")]

# TODO: add county_id to capacity50 and aggregate
#aggr.cap <- capacity50[, .()]



