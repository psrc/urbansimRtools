library(data.table)

# Settings
#==========
# were are parcels, buildings and various Xwalk tables, exported from the base year DB
data.dir <- "../data/BY2023"
rds.data.dir <- "~/psrc/R/shinyserver/baseyear2023explorer/data" # directory with big binary datasets

# where are the constraints file and the flu file located
#constr.dir <- "J:\Staff\Christy\usim-baseyear\dev_constraints"
#flu.dir <- "J:\Staff\Christy\usim-baseyear\flu"
constr.dir <- "~/psrc/urbansim-baseyear-prep/future_land_use/dev_constraints"
flu.dir <- file.path(constr.dir, "..")
    
# when were the flu file and constraints file created
flu.date <- "2026-05-13"
lc.factor <- 1/100 # needed if LC is defined as percentage (i.e. 0-100)

# factors that will constrain development, 
# i.e. parcel is developable if capacity > developable.factor * current_built
developable.factors <- c(1,2,3)

# which residential ratios should be used for mix-use
res.ratios <- c(30, 40, 50, 60, 70)

# should the parcel file be updated with new plan_type_id
update.plantype <- TRUE

# Load inputs
#==============
# load constraints file
constr <- fread(file.path(constr.dir, paste0("devconstr_v2_", flu.date, ".csv")))

# load the FLU file by plan_type_id and constraints by LC
flu <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date, ".csv")))

# Load some of the 2023 datasets exported from the DB
pcls <- readRDS(file.path(rds.data.dir, "parcels.rds"))
#pcls <- fread(file.path(data.dir, "parcels.csv"))
job_sqft <- fread(file.path(data.dir, "building_sqft_per_job.csv"))
bts <- fread(file.path(data.dir, "building_types.csv"))

# load 2023 buildings
bldgs <- readRDS(file.path(rds.data.dir, "buildings.rds"))
#bldgs <- fread("~/psrc/urbansim-baseyear-prep/imputation/data2023/buildings_imputed_phase3_lodes_20240226.csv")

if(update.plantype){
  # load parcels with updated plan_type_id
  pcls_upd <- fread(file.path(constr.dir, paste0("prcls_ptid_v2_", flu.date, ".csv")))
  pcls[pcls_upd, plan_type_id := i.plan_type_id, on = "parcel_id"]
}

source("capacity_functions.R")

# start processing
#=================

# add generic land use type to the FLU dataset
flulc <- rbind(flu[Res_Use == 1, .(plan_type_id, coverage = LC_Res, generic_land_use_type_id = 1)],
               flu[Res_Use == 1, .(plan_type_id, coverage = LC_Res, generic_land_use_type_id = 2)],
               flu[Office_Use == 1, .(plan_type_id, coverage = LC_Office, generic_land_use_type_id = 3)],
               flu[Comm_Use == 1, .(plan_type_id, coverage = LC_Comm, generic_land_use_type_id = 4)],
               flu[Indust_Use == 1, .(plan_type_id, coverage = LC_Indust, generic_land_use_type_id = 5)],
               flu[Mixed_Use == 1, .(plan_type_id, coverage = LC_Mixed, generic_land_use_type_id = 6)]
)
# add the FLU info, including coverage, to the development constraints dataset
constr[flulc, coverage := i.coverage * lc.factor, 
       on = .(plan_type_id, generic_land_use_type_id)]
constr[is.na(coverage), coverage := 1] 

# assemble sqft per job
job_sqft[bts, generic_land_use_type_id := i.generic_building_type_id, on = "building_type_id"]
# take the mean over sectors by each zone and GLU type
job_sqft_mean <- job_sqft[, .(building_sqft_per_job = mean(building_sqft_per_job)), by = .(zone_id, generic_land_use_type_id)]
# impute sqft per job for mix-use by taking the average over sectors and three GLU types by each zone
jsf_nmu <- job_sqft[generic_land_use_type_id %in% c(3,4,5), .(building_sqft_per_job = mean(building_sqft_per_job)), by = .(zone_id)]
job_sqft_mean <- rbind(job_sqft_mean, jsf_nmu[, generic_land_use_type_id := 6])

# compute capacity for each parcel
pclwu <- compute.parcel.capacity(pcls, constr, job_sqft_mean, include.coverage = TRUE)

# get current built
pclbld <- bldgs[, .(residential_units = sum(residential_units), non_residential_sqft = sum(non_residential_sqft),
                    building_sqft = sum(residential_units * sqft_per_unit + non_residential_sqft)), by = .(parcel_id)]

# join current built with parcels' capacity
pclwu[pclbld, `:=`(residential_units_built = i.residential_units, 
                   non_residential_sqft_built = i.non_residential_sqft,
                   building_sqft_built = i.building_sqft), 
      on = "parcel_id"]
pclwu[is.na(residential_units_built), residential_units_built := 0]
pclwu[is.na(non_residential_sqft_built), non_residential_sqft_built := 0]
pclwu[is.na(building_sqft_built), building_sqft_built := 0]


# aggregate capacity for desired geography, using different residential ratios
# and developable factors
allres <- NULL
for(devfac in developable.factors){
    res <- aggregate.capacity(pclwu, by = "growth_center_id", developable.factor = devfac,
                              res.ratios = res.ratios)
    allres <- rbind(allres, res[, developfac := devfac])
}

# join with location info (i.e. geography names)
gcs <- fread(file.path(data.dir, "growth_centers.csv"))
allres[gcs, name := i.name, on = "growth_center_id"]

# save results
fwrite(allres, file = paste0("RGC_capacity_data_", Sys.Date(), ".csv"))


# choose one developable factor and subset results to it for plotting purposes
developable.factor <- developable.factors[1]
res <- allres[developfac == developable.factor]

# plot results
library(ggplot2)
g <- ggplot(res[growth_center_id %in% c(531, 515, 521)]) + geom_col(aes(x = type, y = percent_rem_cap, group = res_ratio, fill = res_ratio), position = "dodge") +
    facet_wrap(vars(name), ncol = 1, scales = "free") + xlab("") + ylab("percent undeveloped capacity")
print(g)

res2 <- melt(res, id.vars = c("growth_center_id", "name",  "type", "res_ratio"), variable.name = "indicator")
g1 <- ggplot(res2[growth_center_id %in% c(531, 515, 521) &  indicator %in% c("remaining_capacity", "total_capacity")]) + 
    geom_col(aes(x = indicator, y = value, group = res_ratio, fill = res_ratio), position = "dodge") +
    facet_grid(type ~ name, scales = "free") + xlab("") + ylab("capacity")
print(g1)

reseb <- res2[growth_center_id > 0 &  indicator %in% c("remaining_capacity", "total_capacity")][type == "non-residential-jobs" & indicator == "total_capacity", value := NA]
reseb <- dcast(reseb, name + type + indicator ~ res_ratio, value.var = "value")
#reseb[type == "non-residential-jobs" & indicator == "total_capacity", value := NA]
gall <- ggplot(reseb, aes(x = name, group = indicator, color = indicator)) + 
    geom_errorbar(aes(ymin = `40`, ymax = `60`), position = position_dodge(width=0.3), na.rm = TRUE)  + 
    geom_point(aes(y = `50`), na.rm = TRUE, position = position_dodge(width=0.3)) +
    facet_grid(type ~ . , scales = "free") + xlab("") + ylab("") +
    guides(x =  guide_axis(angle = 90))
    #theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

print(gall)

#pdf(file = paste0("updRGC_capacity_resratio_", paste(res.ratios, collapse = "_"), 
#                  "_devfac_", developable.factor, ".pdf"), width = 12, height = 10)
#print(gall)
#dev.off()

