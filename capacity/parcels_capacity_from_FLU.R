##################################################
# Script for computing capacity for each parcel
# based on the FLU and development constraints only
# (i.e. no urbansim run is needed).
# The output has the same structure as the output of parcels_capacity.R.
# Hana Sevcikova, PSRC, 2026-10-05
##################################################

library(data.table)

### Users settings
##################
# Where is this script
setwd('~/psrc/R/urbansimRtools/capacity')

# save in csv file
save <- TRUE

# where are parcels, buildings and various Xwalk tables, exported from the base year DB
data.dir <- "../data/BY2023updgeo" # data with updated geographies
rds.data.dir <- "~/psrc/R/shinyserver/baseyear2023explorer/data" # needed only if update.plantype.from.rds is TRUE

# where are the constraints file and the flu file located
constr.dir <- "~/psrc/urbansim-baseyear-prep/future_land_use/dev_constraints"
flu.dir <- file.path(constr.dir, "../flu")

# when were the flu file and constraints file created
flu.date <- "2026-09-30"

lc.factor <- 1 # if LC is defined as percentage (then use 1/100) or proportion (then use 1)

# should the parcel file be updated with new plan_type_id
# (set it to FALSE, if the FLU's plan_type_id is the one attached to parcels)
update.plantype <- TRUE
# should the update be made using parcels in baseyear explorer
# (should be TRUE for the old FLU, i.e. 2023-01-10);
# if FALSE, the file prcls_ptid_final_{flu.date}.csv in constr.dir is used
update.plantype.from.rds <- FALSE

# share of residential capacity on mix-use parcels (in %);
# the non-residential capacity is taken as (100 - res.ratio)%
res.ratio <- 50

# parcel is developable if capacity > developable.factor * current_built;
# otherwise its capacity is set to the current built
developable.factor <- 1

# sqft per unit used to convert residential capacity into building sqft,
# by generic land use type of the binding residential constraint (1 = SF, 2 = MF, 6 = mix-use)
res.sqft.per.unit <- c(`1` = 1000, `2` = 500, `6` = 500)

# building types used to derive sqft per job for each non-res generic land use type
# (3 = office, 4 = commercial, 5 = industrial, 6 = mix-use);
# if multiple building types are given, their sqft per job is averaged
glu.job.building.types <- list(`3` = 13,         # office
                               `4` = 3,          # commercial
                               `5` = c(8, 21),   # industrial, warehousing
                               `6` = c(3, 13))   # commercial, office

# prefix of the output file name
file.prefix <- paste0("CapacityPclFLU_res", res.ratio, "_flu-", flu.date, "_", Sys.Date())
####### End users settings

source("capacity_functions.R")

# Load inputs
#==============
pcls <- fread(file.path(data.dir, "parcels.csv"))
setkey(pcls, "parcel_id")

# load constraints file
constr <- fread(file.path(constr.dir, paste0("devconstr_final_", flu.date, ".csv")))

# load the FLU file by plan_type_id
flu <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date, ".csv")))

# load sqft/job by building type and zone
job_sqft <- fread(file.path(data.dir, "building_sqft_per_job.csv"))

# load base year buildings
bldgs <- fread(file.path(data.dir, "buildings.csv"))

if(update.plantype){
    # assign plan_type_id that corresponds to the FLU
    if(update.plantype.from.rds) {
        pcls_upd <- readRDS(file.path(rds.data.dir, "parcels.rds"))[, .(parcel_id, plan_type_id)]
    } else {
        pcls_upd <- fread(file.path(constr.dir, paste0("prcls_ptid_final_", flu.date, ".csv")))
        if(! "parcel_id" %in% colnames(pcls_upd) && "PIN" %in% colnames(pcls_upd))
            setnames(pcls_upd, "PIN", "parcel_id")
    }
    pcls[pcls_upd, plan_type_id := i.plan_type_id, on = "parcel_id"]
}

# Base year stock
#=================
# impute missing sqft_per_unit and compute building_sqft in the base year buildings
bld <- copy(bldgs)
bld[residential_units > 0 & building_type_id == 19 & sqft_per_unit == 0, sqft_per_unit := 1000]
bld[residential_units > 0 & building_type_id != 19 & sqft_per_unit == 0, sqft_per_unit := 500]
bld[, building_sqft := residential_units * sqft_per_unit + non_residential_sqft]

# aggregate to parcels (job_capacity is taken from the buildings table)
pclstock <- bld[, .(DUbase = sum(residential_units), NRSQFbase = sum(non_residential_sqft),
                    JOBSPbase = sum(job_capacity), BLSQFbase = sum(building_sqft)),
                by = "parcel_id"]
setkey(pclstock, parcel_id)

# Capacity from FLU
#===================
# add generic land use type to the FLU dataset
flulc <- rbind(flu[Res_Use == 1 | Res_Use == "Y", .(plan_type_id, coverage = LC_Res, generic_land_use_type_id = 1)],
               flu[Res_Use == 1 | Res_Use == "Y", .(plan_type_id, coverage = LC_Res, generic_land_use_type_id = 2)],
               flu[Office_Use == 1 | Office_Use == "Y", .(plan_type_id, coverage = LC_Office, generic_land_use_type_id = 3)],
               flu[Comm_Use == 1 | Comm_Use == "Y", .(plan_type_id, coverage = LC_Comm, generic_land_use_type_id = 4)],
               flu[Indust_Use == 1 | Indust_Use == "Y", .(plan_type_id, coverage = LC_Indust, generic_land_use_type_id = 5)],
               flu[Mixed_Use == 1 | Mixed_Use == "Y", .(plan_type_id, coverage = LC_Mixed, generic_land_use_type_id = 6)]
)
# add the FLU info, including coverage, to the development constraints dataset
constr[flulc, coverage := i.coverage * lc.factor, on = .(plan_type_id, generic_land_use_type_id)]
constr[is.na(coverage), coverage := 1]

# assemble sqft per job by generic land use type, using the specified building types
glu.bt <- rbindlist(lapply(names(glu.job.building.types), function(glu) 
    data.table(generic_land_use_type_id = as.integer(glu), building_type_id = glu.job.building.types[[glu]])))
job_sqft_mean <- merge(job_sqft, glu.bt, by = "building_type_id", allow.cartesian = TRUE)[
    , .(building_sqft_per_job = mean(building_sqft_per_job)), by = .(zone_id, generic_land_use_type_id)]

# compute capacity for each parcel (at most one res and one non-res record per parcel)
pclwu <- compute.parcel.capacity(pcls, constr, job_sqft_mean, include.coverage = TRUE)

# convert into one record per parcel
capres <- pclwu[constraint_type != "far", .(parcel_id, mixed, DUcap = residential_units,
                                            res_glu = generic_land_use_type_id, has_res = TRUE)]
capnonres <- pclwu[constraint_type == "far", .(parcel_id, plan_type_id, zone_id, mixed, 
                                               NRSQFcap = building_sqft, has_nonres = TRUE)]

# sqft per job is averaged over all non-res generic land use types allowed by the parcel's plan type
# (rather than taking the one of the binding constraint, as several types often allow the same FAR)
allowed.glu <- unique(constr[constraint_type == "far", .(plan_type_id, generic_land_use_type_id)])
pt.zone <- unique(capnonres[, .(plan_type_id, zone_id)])
avg_job_sqft <- merge(merge(pt.zone, allowed.glu, by = "plan_type_id", allow.cartesian = TRUE),
                      job_sqft_mean, by = c("zone_id", "generic_land_use_type_id"))[
                          , .(building_sqft_per_job = mean(building_sqft_per_job)), by = .(plan_type_id, zone_id)]
capnonres[avg_job_sqft, building_sqft_per_job := i.building_sqft_per_job, on = c("plan_type_id", "zone_id")]
capnonres[, `:=`(plan_type_id = NULL, zone_id = NULL)]
cap <- merge(capres, capnonres, by = c("parcel_id", "mixed"), all = TRUE)
cap[is.na(DUcap), DUcap := 0]
cap[is.na(NRSQFcap), NRSQFcap := 0]
cap[is.na(has_res), has_res := FALSE]
cap[is.na(has_nonres), has_nonres := FALSE]

# split mix-use parcels using the residential ratio
cap[mixed == TRUE, `:=`(DUcap = res.ratio/100 * DUcap,
                        NRSQFcap = (1 - res.ratio/100) * NRSQFcap)]

# derive job capacity and building sqft
cap[, JOBSPcap := ifelse(NRSQFcap > 0 & !is.na(building_sqft_per_job), NRSQFcap / building_sqft_per_job, 0)]
cap[, BLSQFcap := NRSQFcap]
cap[DUcap > 0, BLSQFcap := BLSQFcap + DUcap * res.sqft.per.unit[as.character(res_glu)]]

# Combine capacity with base year stock
#=======================================
all.pcls <- merge(cap, pclstock, by = "parcel_id", all = TRUE)
for(col in c("DUbase", "NRSQFbase", "JOBSPbase", "BLSQFbase"))
    all.pcls[is.na(get(col)), (col) := 0]

# parcel is developable if all its FLU parts (res and/or non-res) exceed the current built
all.pcls[, developable := !is.na(mixed) &
             (!has_res | DUcap > developable.factor * DUbase) &
             (!has_nonres | NRSQFcap > developable.factor * NRSQFbase)]

# if developable take the FLU capacity, otherwise the current built
all.pcls[, `:=`(DUcapacity = ifelse(developable, DUcap, DUbase),
                NRSQFcapacity = ifelse(developable, NRSQFcap, NRSQFbase),
                JOBSPcapacity = ifelse(developable, JOBSPcap, JOBSPbase),
                BLSQFcapacity = ifelse(developable, BLSQFcap, BLSQFbase))]

respcl <- all.pcls[, .(parcel_id, DUbase, DUcapacity, NRSQFbase, NRSQFcapacity,
                       JOBSPbase, JOBSPcapacity, BLSQFbase, BLSQFcapacity)]
respcl <- merge(pcls[, .(parcel_id, plan_type_id, county_id, city_id, growth_center_id,
                         control_id, tod_id, subreg_id, hb_hct_buffer, hb_tier)],
                respcl, by = "parcel_id")

# print regional totals
print(respcl[, lapply(.SD, sum), .SDcols = DUbase:BLSQFcapacity])

# output results
if(save)
    fwrite(respcl, file = paste0(file.prefix, ".csv"))
