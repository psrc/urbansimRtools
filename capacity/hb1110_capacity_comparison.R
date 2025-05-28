library(data.table)
library(openxlsx)

setwd('~/psrc/R/urbansimRtools/capacity')
lookup.path <- "~/opus/urbansim_data/data/psrc_parcel/runs/run_24.2025_05_13_17_21_unlimited_2x/csv/2023"

# lookup datasets
subregs <- fread("~/psrc/R/shinyserver/luf-dashboard/data/control_hcts.csv")
counties <- data.table(county_id = c(33, 35, 53, 61), county = c("King", "Kitsap", "Pierce", "Snohomish"))
constraints <- fread(file.path(lookup.path, "development_constraints.csv"))

subregs[, `:=`(subreg_id = control_hct_id, control_id = control_hct_id)]
subregs[control_id > 1000, control_id := control_id - 1000]
controls <- subregs[control_hct_id < 1000]
controls <- merge(controls, counties, by = "county_id")[, `:=`(lgarea_group = NULL, subreg_id = NULL, control_hct_id = NULL)]
setnames(controls, "control_hct_name", "name")

pcl1 <- fread("CapacityPclNoSampling_res50-2025-05-14.csv")
pcl2 <- fread("CapacityPclNoSampling_res50-2025-05-21_hb1110.csv")
allpcl <- fread(file.path(lookup.path, "parcels.csv"))

pcl1[subregs, control_id := i.control_id, on = "subreg_id"]
pcl2[subregs, control_id := i.control_id, on = "subreg_id"]

# tag parcels that are SF plan type
allpcl[constraints, plan_type_glu := i.generic_land_use_type_id, on = "plan_type_id"]
pcl1[allpcl, `:=`(is_sf_plan_type = i.plan_type_glu == 1, parcel_sqft = i.parcel_sqft), on = "parcel_id"]
pcl2[allpcl, `:=`(is_sf_plan_type = i.plan_type_glu == 1, parcel_sqft = i.parcel_sqft), on = "parcel_id"]

cap1 <- pcl1[is_sf_plan_type == TRUE, .(du1 = sum(round(DUcapacity))), by = "control_id"]
# taking the parcel size from the hb1110 dataset as it might have developed a slightly more parcels 
cap2 <- pcl2[is_sf_plan_type == TRUE, .(du2 = sum(round(DUcapacity)), 
                                        N = .N, acres = round(sum(parcel_sqft)/43560)), by = "control_id"]

cap <- merge(cap1, cap2, by = "control_id")
cap <- merge(cap, controls, by = "control_id")
cap[, `:=`(difdu = du2 - du1)]

capout <- cap[, .(control_id, name, county, Npcl = N, acres, DUcap = du1, DUcap_hb1110 = du2, 
                  change = difdu, percent_change = round(difdu/du1 * 100))][order(-change)]
capout.sum <- capout[, .(control_id = NA, name = "Region", county = "Region", 
                         Npcl = sum(Npcl), acres = sum(acres), DUcap = sum(DUcap), 
                         DUcap_hb1110 = sum(DUcap_hb1110))][, change := DUcap_hb1110 - DUcap][
                             , percent_change := round(change/DUcap * 100)]
capout <- rbind(capout.sum, capout)

options("openxlsx.numFmt" = "#,##0")
write.xlsx(capout, file = paste0("hb1110_capacity-", Sys.Date(), ".xlsx"))
