library(data.table)

# Settings
#==========
# were are parcels, buildings and various Xwalk tables, exported from the base year DB
data.dir <- "../../data/BY2018"

# where are the constraints file and the flu file located
#constr.dir <- "J:\Staff\Christy\usim-baseyear\dev_constraints"
#flu.dir <- "J:\Staff\Christy\usim-baseyear\flu"
constr.dir <- "~/psrc/urbansim-baseyear-prep/future_land_use"
flu.dir <- constr.dir
    
# when were the flu file and constraints file created
flu.date <- "2023-01-10" 

# factors that will constrain development, 
# i.e. parcel is developable if capacity > developable.factor * current_built
developable.factors <- c(1,2,3)

# which residential ratios should be used for mix-use
res.ratios <- c(30, 40, 50, 60, 70)

# vacancy
vacancy.rate <- 0.05

# Load inputs
#==============
# load constraints file
constr <- fread(file.path(constr.dir, paste0("devconstr_v2_", flu.date, ".csv")))

# load the FLU file by plan_type_id and constraints by LC
flu <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date, ".csv")))

# Load some of the 2018 datasets exported from the DB
pcls <- fread(file.path(data.dir, "parcels.csv"))
job_sqft <- fread(file.path(data.dir, "building_sqft_per_job.csv"))
bts <- fread(file.path(data.dir, "building_types.csv"))

# load 2023 or 2018 buildings
bldgs <- fread(file.path(data.dir, "buildings.csv"))
#bldgs <- fread("~/psrc/urbansim-baseyear-prep/imputation/data2023/buildings_imputed_phase3_lodes_20240226.csv")

# for aggregation into updated growth centers import updated Xwalk and merge with parcels
pcls.upd <- fread(file.path(data.dir, "parcels18_with_updated_growth_center_id.csv"))
pcls <- merge(pcls, pcls.upd, by = "parcel_id")

# read PPH
pph <- fread(file.path(data.dir, "pph.csv"))[, `:=`(county_id = NULL, name = NULL)]

# join with parcels
pcls <- merge(pcls, pph, by = c("control_id", "target_id"))

# Functions
#============

# function for computing capacity for each parcel
compute.parcel.capacity <- function(pcl, constraints, job.sqft, include.coverage = FALSE) {
    pclw <- pcl[, .(parcel_id, plan_type_id, parcel_sqft, county_id, city_id, zone_id)]
    pclw <- merge(pcl, constraints, by = "plan_type_id", allow.cartesian=TRUE)
    pclw[job.sqft, building_sqft_per_job := i.building_sqft_per_job, on = c("generic_land_use_type_id", "zone_id")]
    
    # compute building sqft & residential units
    if(! "coverage" %in% colnames(pclw) || !include.coverage) pclw[, coverage := 1]
    if(include.coverage)
        pclw[, coverage := pmin(0.95, coverage)]
    
    pclw[constraint_type == "far", building_sqft := parcel_sqft * maximum * coverage]
    pclw[constraint_type == "units_per_acre", residential_units := parcel_sqft * maximum / 43560 * coverage]
    
    # select one max for residential and one for non-res type, so that each parcel has 2 records at most
    pclwu <- pclw[pclw[, .I[which.max(maximum)], by = .(parcel_id, constraint_type)]$V1]
    pclwu[, mixed := .N > 1, by = parcel_id]
    
    # add county names
    pclwu[, county := factor(county_id, levels = c(33, 35, 53, 61), labels = c("King", "Kitsap", "Pierce", "Snohomish"))]
    return(pclwu)
}

# function for aggregating capacity to user-specific geography
aggregate.capacity <- function(pcl, by = "county", developable.factor = 1) {
    # extract non-mix-use residential and non-residential parcels as their capacity will  not change with the ratio
    pcl_resid <- pcl[mixed==FALSE, .(parcel_id, units = residential_units, 
                                     remaining_capacity = pmax(0, residential_units - residential_units_built),
                                     developable = residential_units > developable.factor * residential_units_built)][
                                         , type := "residential-units"][!is.na(units)]

    # convert DUs to population
    pcl_resid_pop <- pcl_resid[pcl, .(parcel_id, units = units * (1-vacancy.rate) * i.pph2020_tot, 
                                      remaining_capacity = remaining_capacity * (1-vacancy.rate) * i.pph2020_tot,
                                      developable), on = "parcel_id"
                              ][ , type := "residential-pop"]
    # non-res
    pcl_nonresid <- pcl[mixed==FALSE, .(parcel_id, units = building_sqft, 
                                        remaining_capacity = pmax(0, building_sqft - non_residential_sqft_built), building_sqft_per_job,
                                        developable = building_sqft > developable.factor * non_residential_sqft_built)][
                                            , type := "non-residential-sqft"][!is.na(units)]
    # jobs
    pcl_nonresid_jobs <- pcl_nonresid[, .(parcel_id, remaining_capacity = remaining_capacity/building_sqft_per_job, 
                                          developable)][ , type := "non-residential-jobs"]
    pcl_nonresid[, building_sqft_per_job := NULL]
    
    pcl_non_mix <- merge(unique(pcl[, c("parcel_id", by), with = FALSE]), 
                         rbind(pcl_resid, 
                               pcl_resid_pop, 
                               pcl_nonresid, 
                               pcl_nonresid_jobs, 
                        fill = TRUE), by = "parcel_id")
    
    # aggregate the non-mix-use parcels
    units_non_mix <- pcl_non_mix[ , .(units = sum(units, na.rm = TRUE), 
                                      remaining_capacity = sum(remaining_capacity*developable, na.rm = TRUE)), 
                                  by = c(by, "type")]
    
    # construct mix-use
    res <- NULL
    for(ratio in res.ratios) { # iterate over the various ratios
        # construct residential part
        pcl_mix_res <- pcl[mixed==TRUE & constraint_type == "units_per_acre", 
                           .(parcel_id, units = ratio/100 * residential_units, units_built = residential_units_built)][
                               , `:=`(remaining_capacity = pmax(0, units - units_built), 
                                      developable = units > developable.factor * units_built,
                                      type = "residential-units")][!is.na(units)]
        # population
        pcl_mix_pop <- pcl_mix_res[pcl, .(parcel_id, units = units * (1-vacancy.rate) * i.pph2020_tot, 
                                          remaining_capacity = remaining_capacity * (1-vacancy.rate) * i.pph2020_tot,
                                          developable), on = "parcel_id"
                                    ][ , type := "residential-pop"]
        
        # construct non-residential part
        pcl_mix_nonres <- pcl[mixed==TRUE & constraint_type == "far", .(parcel_id, units = (100 - ratio)/100 * building_sqft, 
                                                                        units_built = non_residential_sqft_built, 
                                                                        building_sqft_per_job)][
                                                                            ,`:=`(remaining_capacity = pmax(0, units - units_built), 
                                                                                  developable = units > developable.factor * units_built,
                                                                                  type = "non-residential-sqft")][!is.na(units)]
        pcl_mix_nonres_jobs <- pcl_mix_nonres[, .(parcel_id, remaining_capacity = remaining_capacity/building_sqft_per_job,
                                                  developable)][, type := "non-residential-jobs"]
        pcl_mix_nonres[, building_sqft_per_job := NULL]
        
        # put res and non-res together
        pcl_mix <- merge(unique(pcl[, c("parcel_id", by), with = FALSE]), 
                         rbind(pcl_mix_res, 
                               pcl_mix_pop,
                               pcl_mix_nonres, 
                               pcl_mix_nonres_jobs, fill = TRUE), by = "parcel_id")
        
        # set developable column to TRUE only if both parts are TRUE
        pcl_mix[pcl_mix[, .(alldev = sum(developable) == .N), by = "parcel_id"], developable := i.alldev, on = "parcel_id"]

        # aggregate to desired geography
        units <- pcl_mix[, .(units = sum(units, na.rm = TRUE), #units_built = sum(units_built, na.rm = TRUE),
                             remaining_capacity = sum(remaining_capacity*developable, na.rm = TRUE)), by = c(by, "type")]
        #units[type != "non-residential-jobs", remaining_capacity := pmax(0, units - units_built)][, units_built := NULL]
        
        # combine with non-mix-use
        units <- rbind(units, units_non_mix)
        units <- units[ , .(units = sum(units, na.rm = TRUE), remaining_capacity = sum(remaining_capacity, na.rm = TRUE)), 
                        by = c(by, "type")]
        
        res <- rbind(res, units[, res_ratio := ratio])
    } # end of loop over ratios
    
    # compute activity units
    res <- rbind(res, res[type %in% c("residential-pop", "non-residential-jobs"), 
                          .(units = sum(units), remaining_capacity = sum(remaining_capacity)), by = c("res_ratio", by)][
                            , type := "activity-units"]
                  )
    # some cleaning and computing shares
    res[, res_ratio := as.factor(res_ratio)]
    res[, type := factor(type, levels = c("residential-units", "non-residential-sqft", 
                                          "activity-units", "residential-pop", "non-residential-jobs"))]
    res[, `:=`(remaining_total_capacity = sum(remaining_capacity)), by = c("type", "res_ratio", by[-1])]
    res[, `:=`(percent_rem_cap = round(remaining_capacity/remaining_total_capacity * 100,1))]
    setnames(res, "units", "total_capacity")
    return(res)
}

# start processing
#=================

# add generic land use type to the FLU dataset
flulc <- rbind(flu[Res_Use == "Y", .(plan_type_id, coverage = LC_Res, generic_land_use_type_id = 1)],
               flu[Res_Use == "Y", .(plan_type_id, coverage = LC_Res, generic_land_use_type_id = 2)],
               flu[Office_Use == "Y", .(plan_type_id, coverage = LC_Office, generic_land_use_type_id = 3)],
               flu[Comm_Use == "Y", .(plan_type_id, coverage = LC_Comm, generic_land_use_type_id = 4)],
               flu[Indust_Use == "Y", .(plan_type_id, coverage = LC_Indust, generic_land_use_type_id = 5)],
               flu[Mixed_Use == "Y", .(plan_type_id, coverage = LC_Mixed, generic_land_use_type_id = 6)]
)
# add the FLU info, including coverage, to the development constraints dataset
constr[flulc, coverage := i.coverage, on = .(plan_type_id, generic_land_use_type_id)]
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
    res <- aggregate.capacity(pclwu, by = "updated_growth_center_id", developable.factor = devfac)
    allres <- rbind(allres, res[, developfac := devfac])
}
setnames(allres, "updated_growth_center_id", "growth_center_id")

# join with location info (i.e. geography names)
gcs <- fread(file.path(data.dir, "growth_centers_updated.csv"))
allres[gcs, name := i.growth_center_name, on = "growth_center_id"]

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

