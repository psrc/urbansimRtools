##################################################
# Script for computing capacity for each parcel given HB1110.
# It calls the parcels_capacity.R script and then 
# in addition, it increases the capacity for HB1110 
# parcels if needed.
# Hana Sevcikova, PSRC, updated on 2025-06-03
##################################################
library(data.table)

save <- TRUE
lookup.path <- "~/opus/urbansim_data/data/psrc_parcel/runs/run_36.2025_06_02_14_35_unlimited_1x/csv/2023"
file.prefix <- paste0("CapacityPclNoSampling_res50-", Sys.Date(), "_hb1110")

pclcap <- fread("CapacityPclNoSampling_res50-2025-06-03.csv")
pcl <- fread(file.path(lookup.path, "parcels.csv"))
setkey(pcl, parcel_id)
constraints <- fread(file.path(lookup.path, "development_constraints.csv"))

# add some parcel columns from the base year
pclcap[pcl, `:=`(plan_type_id = i.plan_type_id, hb_tier = i.hb_tier, hb_hct_buffer = i.hb_hct_buffer), 
       on = "parcel_id"]
# determine the generic land use type for each parcel
pclcap[constraints, plan_type_glu := i.generic_land_use_type_id, on = "plan_type_id"]

# overwrite the capacity for HB parcels that are zone SF or MF
pclcap[hb_tier == 1 & hb_hct_buffer == 1 & plan_type_glu %in% c(1,2), `:=`(DUcapacity = pmax(DUcapacity, 6), JOBSPcapacity = 0)]
pclcap[hb_tier == 1 & hb_hct_buffer == 0 & plan_type_glu %in% c(1,2), `:=`(DUcapacity = pmax(DUcapacity, 4), JOBSPcapacity = 0)]
pclcap[hb_tier == 2 & hb_hct_buffer == 1 & plan_type_glu %in% c(1,2), `:=`(DUcapacity = pmax(DUcapacity, 4), JOBSPcapacity = 0)]
pclcap[hb_tier == 2 & hb_hct_buffer == 0 & plan_type_glu %in% c(1,2), `:=`(DUcapacity = pmax(DUcapacity, 2), JOBSPcapacity = 0)]
pclcap[hb_tier == 3 & plan_type_glu %in% c(1,2), `:=`(DUcapacity = pmax(DUcapacity, 2), JOBSPcapacity = 0)]

# For mix-use, overwrite only parcels that have some DU capacity and do not zeroed out jobs
pclcap[hb_tier == 1 & hb_hct_buffer == 1 & plan_type_glu == 6 & DUcapacity > 0, `:=`(DUcapacity = pmax(DUcapacity, 6))]
pclcap[hb_tier == 1 & hb_hct_buffer == 0 & plan_type_glu == 6 & DUcapacity > 0, `:=`(DUcapacity = pmax(DUcapacity, 4))]
pclcap[hb_tier == 2 & hb_hct_buffer == 1 & plan_type_glu == 6 & DUcapacity > 0, `:=`(DUcapacity = pmax(DUcapacity, 4))]
pclcap[hb_tier == 2 & hb_hct_buffer == 0 & plan_type_glu == 6 & DUcapacity > 0, `:=`(DUcapacity = pmax(DUcapacity, 2))]
pclcap[hb_tier == 3 & plan_type_glu == 6 & DUcapacity > 0, `:=`(DUcapacity = pmax(DUcapacity, 2))]


pclcap[, `:=`(plan_type_id = NULL, plan_type_glu = NULL)]

# output results
if(save)
    fwrite(pclcap, file = paste0(file.prefix, ".csv"))

