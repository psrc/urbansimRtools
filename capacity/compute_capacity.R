# Script for computing maximum developable capacity
# Hana Sevcikova
# 07/09/2018

library(data.table)
library(retistruct)
library(raster)

# Prefix of the resulting files
file.prefix <- "capacity"

# Either save to Excel (TRUE) or csv (FALSE)
save.to.excel <- FALSE

# Run names and the corresponding path 
# (the path must contain file parcel__dataset_table__households_jobs__2050.tab)
runs <- list("134stc" = "/Users/hana/d6$/opusgit/urbansim_data/data/psrc_parcel/runs/run_134.run_2018_05_12_13_11/indicators", 
             "151JF" = "/Users/hana/d6$/opusgit/urbansim_data/data/psrc_parcel/runs/run_151.run_2018_06_08_15_24/indicators",
             "128stc_no_boosts" = "/Users/hana/d6$/opusgit/urbansim_data/data/psrc_parcel/runs/run_128.run_2018_05_04_12_56/indicators"
             )
# Which run to process (as an index to the above list)
run <- 1

# Where are csv tables with unlimited proposals and components (e.g. from run 142)
prop.path <- "../data/unlimited"

# Where are csv lookup tables:
# base year buildings2014 & parcels2014,
# households2014 & jobs2014,
# cities, growth_centers, building_sqft_per_job, 
# development_templates & components)
lookup.path <- "../data/"

# initialization
run.name <- names(runs)[run]
data.path <- runs[[run]]

# read and merge datasets
cat("\nLoading data ...")

bld14 <- fread(file.path(lookup.path, "buildings2014.csv"))

props <- fread(file.path(prop.path, "development_project_proposals.csv")) # unlimited proposals
props <- subset(props, status_id != 3) # do not include MPDs

comp <- fread(file.path(prop.path, "development_project_proposal_components.csv"))
comp <- subset(comp, proposal_id %in% props$proposal_id)

pcl <- fread(file.path(lookup.path, "parcels2014.csv"))
setkey(pcl, parcel_id)

cities <- fread(file.path(lookup.path, "cities.csv"))
gcenters <- fread(file.path(lookup.path, "growth_centers.csv"))
bsqft.per.job <- fread(file.path(lookup.path, "building_sqft_per_job.csv"))

templ <- fread(file.path(lookup.path, "development_templates.csv"))
templc <- fread(file.path(lookup.path, "development_template_components.csv"))

demo <- fread(file.path(data.path, "parcel__dataset_table__households_jobs__2050.tab"))

hhs.base <- fread(file.path(lookup.path, "households2014.csv"))
hhs.base <- merge(hhs.base, bld14[, .(building_id, parcel_id)], by = "building_id")
hhs.base <- hhs.base[, .N, by = "parcel_id"]
setkey(hhs.base, parcel_id)

jobs.base <- fread(file.path(lookup.path, "jobs2014.csv"))
jobs.base <- merge(jobs.base, bld14[, .(building_id, parcel_id)], by = "building_id")
jobs.base <- jobs.base[home_based_status == 0, .N, by = "parcel_id"]
setkey(jobs.base, parcel_id)

counties.df <- data.frame(county_id=c(33, 35, 53, 61),
                          county_name=c("King", "Kitsap", "Pierce", "Snohomish"))
cat(" done.\n")
gc(TRUE)
cat("\nMerging ...")

# add density type to proposals
props <- merge(props, templ[, .(template_id, density_type)], by="template_id")

# impute missing sqft_per_unit and compute building_sqft
bld14[residential_units > 0 & building_type_id == 19 & sqft_per_unit == 0, sqft_per_unit := 1000]
bld14[residential_units > 0 & building_type_id != 19 & sqft_per_unit == 0, sqft_per_unit := 500]
bld14[ , building_sqft := residential_units * sqft_per_unit]
bld <- bld14[, .(building_id, parcel_id, residential_units, non_residential_sqft, 
                 building_sqft, job_capacity)]
bld[non_residential_sqft > 0 & building_sqft < non_residential_sqft, building_sqft := non_residential_sqft]

# compute base year units stock
pclstock <- bld[, .(pcl_resunits = sum(residential_units), pcl_nonres_sqft=sum(non_residential_sqft),
                    pcl_bldsqft = sum(building_sqft),
                    pcl_job_capacity = sum(job_capacity)), by=parcel_id]
setkey(pclstock, parcel_id)
rm(bld14, bld)

# add base year HHs and jobs
pclstock <- merge(pclstock, hhs.base, all = TRUE)
setnames(pclstock, "N", "hhs.base")
pclstock <- merge(pclstock, jobs.base, all = TRUE)
setnames(pclstock, "N", "jobs.base")

# merge units stock with proposals
prop <- merge(props, pclstock, by="parcel_id", all.x=TRUE)
prop <- merge(prop, pcl[,.(parcel_id, zone_id)], by="parcel_id")

# Disaggregate proposals into components to get building_type_id and 
# merge with template_components to get building_sqft_per_unit
propc <- merge(prop, comp[,.(building_type_id, component_id, proposal_id)], by="proposal_id")
propc <- merge(propc, templc[, .(template_id, component_id, building_sqft_per_unit)], 
               by = c("template_id", "component_id"))

# Get building_sqft_per_job for each component
propc <- merge(propc, bsqft.per.job, by=c("building_type_id", "zone_id"), all.x = TRUE)

# Compute DU for res and sqft for nonres
propc[, proposed_units_new := ifelse(density_type == "far", 
                                pmax(1, units_proposed_orig/building_sqft_per_unit),
                                units_proposed_orig)]
# compute building_sqft
propc[, building_sqft := ifelse(density_type == "units_per_acre", 
                                units_proposed_orig * building_sqft_per_unit,
                                units_proposed_orig)]

# distinguish res and non-res
propc[, has_non_res := !all(building_type_id %in% c(19,4,12)), by = proposal_id]
propc[, has_res := any(building_type_id %in% c(19,4,12)), by = proposal_id]

# split parcels by type (res, non-res, mix-use)
pcl.type <- propc[,.(is_res = sum(has_non_res) == 0, is_non_res = sum(has_res) == 0,
                     is_mix_use = sum(has_res) > 0 & sum(has_non_res) > 0),
                  by = parcel_id]

# add the res-nonres info to the demo dataset
demo.pcl <- merge(demo, pcl.type, all = TRUE)
rm(demo)

# determine parcel type for parcels with no proposals
has.no.prop <- is.na(demo.pcl$is_mix_use)
demo.pcl[has.no.prop == TRUE, `:=`(is_res = residential_units > 0 & non_residential_sqft == 0,
                                   is_non_res = residential_units == 0 & non_residential_sqft > 0,
                                   is_mix_use = residential_units > 0 & non_residential_sqft > 0)]
no.type <- with(demo.pcl, !is_res & !is_non_res & !is_mix_use)
demo.pcl[no.type == TRUE, `:=`(is_res = households > 0 & non_home_based_employment == 0, 
                               is_non_res = households == 0 & non_home_based_employment > 0,
                               is_mix_use = households > 0 & non_home_based_employment > 0)]
# eliminate no type parcels
demo.pcl <- subset(demo.pcl, is_res | is_non_res | is_mix_use)

# remove smaller proposals and those with status_id 44
prop <- subset(prop, is.na(pcl_bldsqft) | (units_proposed > pcl_resunits & density_type == "units_per_acre") | 
                   (units_proposed > pcl_nonres_sqft & density_type == "far"))
prop <- subset(prop, status_id != 44)

# sum units by proposals separately for res, non-res, mix-use-res and mix-use-non-res
res_units <- propc[parcel_id %in% pcl.type[is_res == TRUE, parcel_id], 
                   .(residential_units = sum(proposed_units_new),
                     building_sqft = sum(building_sqft)), 
                   by = .(parcel_id, proposal_id)]
non_res <- propc[parcel_id %in% pcl.type[is_non_res == TRUE, parcel_id], 
                 .(non_residential_sqft = sum(proposed_units_new),
                   job_capacity = sum(pmax(1, round(proposed_units_new / building_sqft_per_job))),
                   building_sqft = sum(building_sqft)), 
                 by = .(parcel_id, proposal_id)]

# mix-use separated into res and non-res
res_units_mix <- propc[parcel_id %in% pcl.type[is_mix_use == TRUE, parcel_id] & building_type_id %in% c(19,4,12),
                   .(residential_units = sum(proposed_units_new),
                     building_sqft = sum(building_sqft),
                     has_both_comp = sum(has_non_res, has_res) > 1),
                   by = .(parcel_id, proposal_id)]
non_res_mix <- propc[parcel_id %in% pcl.type[is_mix_use == TRUE, parcel_id] & ! building_type_id %in% c(19,4,12),
                 .(non_residential_sqft = sum(proposed_units_new),
                   job_capacity = sum(pmax(1, round(proposed_units_new / building_sqft_per_job))),
                   building_sqft = sum(building_sqft),
                   has_both_comp = sum(has_non_res, has_res) > 1),
                 by = .(parcel_id, proposal_id)]


# remove proposals that yield less units than there is on the ground
non_res <- merge(non_res, pclstock[, .(parcel_id, pcl_job_capacity)], all.x = TRUE, by = "parcel_id")
non_res <- non_res[is.na(pcl_job_capacity) | pcl_job_capacity < job_capacity,]
res_units <- merge(res_units, pclstock[, .(parcel_id, pcl_resunits)], all.x = TRUE, by = "parcel_id")
res_units <- res_units[is.na(pcl_resunits) | pcl_resunits < residential_units,]
non_res_mix <- merge(non_res_mix, pclstock[, .(parcel_id, pcl_job_capacity)], all.x = TRUE, by = "parcel_id")
non_res_mix <- non_res_mix[has_both_comp ==TRUE | is.na(pcl_job_capacity) | pcl_job_capacity < job_capacity,]
res_units_mix <- merge(res_units_mix, pclstock[, .(parcel_id, pcl_resunits)], all.x = TRUE, by = "parcel_id")
res_units_mix <- res_units_mix[has_both_comp ==TRUE | is.na(pcl_resunits) | pcl_resunits < residential_units,]

# select max proposal per parcel
res_units_max <- res_units[, .(residential_units_prop = max(residential_units),
                               building_sqft_prop = max(building_sqft),
                               non_residential_sqft_prop = NA,
                               job_capacity_prop = NA), by = parcel_id]
non_res_max <- non_res[, .(non_residential_sqft_prop = max(non_residential_sqft),
                           job_capacity_prop = max(job_capacity),
                           building_sqft_prop = max(building_sqft),
                           residential_units_prop = NA), by = parcel_id]

# For mix-use, find max proposal for each type and combine together
res_units_mix_max <- res_units_mix[, .SD[which.max(residential_units)], by = parcel_id]
non_res_mix_max <- non_res_mix[, .SD[which.max(job_capacity)], by = parcel_id]
comb_mix_max <- merge(res_units_mix_max, non_res_mix_max, all = TRUE, by = "parcel_id")
comb_mix_max[, building_sqft_prop := ifelse(building_sqft.x > building_sqft.y, building_sqft.x, building_sqft.y)]
comb_mix_max[is.na(pcl_resunits), pcl_resunits := 0]
comb_mix_max[is.na(pcl_job_capacity), pcl_job_capacity := 0]
setnames(comb_mix_max, "residential_units", "residential_units_prop")
setnames(comb_mix_max, "job_capacity", "job_capacity_prop")
setnames(comb_mix_max, "non_residential_sqft", "non_residential_sqft_prop")

# Find parcels where the max proposal is different for res and non-res
comb_mixuse <- copy(comb_mix_max)
comb_mixuse[, is_real_mix := proposal_id.x != proposal_id.y] 
comb_mixuse[is.na(is_real_mix), is_real_mix := TRUE]
pcl.with.real.mix <- comb_mixuse[is_real_mix == TRUE,]$parcel_id
pcl.with.same.mix <- comb_mixuse[is_real_mix == FALSE,]$parcel_id # max prop is the same for res and non-res

comb_max <- rbind(res_units_max, non_res_max, 
                  comb_mixuse[, .(parcel_id, residential_units_prop, 
                                   building_sqft_prop,
                                   non_residential_sqft_prop, job_capacity_prop)])
cat(" done.\n")
gc(TRUE)

cat("\nComputing required columns ...")
# join with existing stock
all.pcls <- merge(demo.pcl, comb_max, by = "parcel_id", all = TRUE)
setkey(all.pcls, parcel_id)
all.pcls <- merge(all.pcls, pclstock, all.x = TRUE)
all.pcls[, `:=`(pcl_resunits = ifelse(is.na(pcl_resunits), 0, pcl_resunits),
                pcl_nonres_sqft = ifelse(is.na(pcl_nonres_sqft), 0, pcl_nonres_sqft),
                pcl_bldsqft = ifelse(is.na(pcl_bldsqft), 0, pcl_bldsqft),
                pcl_job_capacity = ifelse(is.na(pcl_job_capacity), 0, pcl_job_capacity),
                hhs.base = ifelse(is.na(hhs.base), 0, hhs.base),
                jobs.base = ifelse(is.na(jobs.base), 0, jobs.base))]
all.pcls[, residential_units_real := pmax(residential_units, households)] # includes scaling
all.pcls[, new_residential_units := residential_units_real - pcl_resunits]
all.pcls[, new_hhs := households - hhs.base]
all.pcls[, residential_units_capacity := ifelse(is.na(residential_units_prop), residential_units,
                                                residential_units_prop)]
all.pcls[, new_residential_units_capacity := ifelse(is.na(residential_units_prop), 0,
                                                residential_units_prop - pcl_resunits)]
all.pcls[, non_residential_sqft_capacity := ifelse(is.na(non_residential_sqft_prop), non_residential_sqft,
                                                   non_residential_sqft_prop)]
all.pcls[, new_non_residential_sqft := non_residential_sqft - pcl_nonres_sqft]

all.pcls[, new_non_residential_sqft_capacity := ifelse(is.na(non_residential_sqft_prop), 0,
                                                   non_residential_sqft_prop - pcl_nonres_sqft)]
all.pcls[, new_jobs := non_home_based_employment - jobs.base]
all.pcls[, job_capacity_real := pmax(job_capacity, non_home_based_employment)]
all.pcls[, job_cap_capacity := ifelse(is.na(job_capacity_prop), job_capacity,
                                      job_capacity_prop)]
all.pcls[, new_job_capacity := job_capacity_real - pcl_job_capacity]
all.pcls[, new_job_cap_capacity := ifelse(is.na(job_capacity_prop), 0,
                                      job_capacity_prop - pcl_job_capacity)]
all.pcls[, building_sqft_capacity := ifelse(is.na(building_sqft_prop), building_sqft,
                                            building_sqft_prop)]
all.pcls[, building_sqft_res := pmax(building_sqft - non_residential_sqft, 0)]
all.pcls[, new_building_sqft := building_sqft - pcl_bldsqft]
all.pcls[, new_building_sqft_capacity := ifelse(is.na(building_sqft_prop), 0,
                                            building_sqft_prop - pcl_bldsqft)]
all.pcls[, new_building_sqft_res := pmax(new_building_sqft - new_non_residential_sqft, 0)]
all.pcls[, type := factor(ifelse(is_res, "R", ifelse(is_non_res, "NR", "MU")), levels = c("R", "NR", "MU"))]
all.pcls[, has_new_capacity := new_building_sqft_capacity > 0 | new_non_residential_sqft_capacity > 0 | new_job_cap_capacity > 0 | new_residential_units_capacity > 0]

all.pclsG <- merge(all.pcls, pcl[, .(parcel_id, growth_center_id, faz_id, city_id, county_id)])
all.pclsG[, real_mix := parcel_id %in% pcl.with.real.mix]
all.pclsG[, same_mix := parcel_id %in% pcl.with.same.mix]
all.pclsG[, is_new_mix := parcel_id %in% c(pcl.with.real.mix, pcl.with.same.mix)]
cat(" done.\n")
gc()

get.by <- function(by) 
    c("type", by)

summarize.mixuse <- function(dt, is_real_mix, dt.to.join, by = c()) {
    dts <- dt[is_real_mix == TRUE, .(DUcap = sum(residential_units_capacity), 
                                     EMPcap = sum(job_cap_capacity)), by = by]
    # the same max proposal for res and non-res; this amount stays constant when we change res/non-res proportions (min on the axes)
    same.mix <- dt[is_real_mix == FALSE,.(DUcap_min = pmax(0, sum(residential_units_capacity)),
                                          EMPcap_min = pmax(0, sum(job_cap_capacity))), 
                   by = by]
    if(length(by) == 0)
        dts <- cbind(dts, same.mix, dt.to.join)
    else {
        dts <- merge(dts, same.mix, all = TRUE, by = by)
        dts <- merge(dts, dt.to.join, all = TRUE, by = by)
    }
    dts[is.na(DUcap_min), DUcap_min := 0]
    dts[is.na(EMPcap_min), EMPcap_min := 0]
    dts[is.na(EMPcap), EMPcap := 0]
    dts[is.na(DUcap), DUcap := 0]
    dts[, DUcap := round(DUcap + DUcap_min)]
    dts[, EMPcap := round(EMPcap + EMPcap_min)]
    get_share <- function(dtx) {
        #print(dtx)
        # line intersection
        #Origin <- c(dtx$EMPcap_min, dtx$DUcap_min)
        Origin <- c(0,0)
        X <- c(max(dtx$EMPspace, 0), max(dtx$DU, 0)) # current usage
        M1 <- c(dtx$EMPcap_min, dtx$DUcap) # DUcap
        M2 <- c(dtx$EMPcap, dtx$DUcap_min) # EMPcap
        if(all(M1 == M2)) 
            I <- X
        else
            I <- line.line.intersection(Origin, X, M1, M2)
        #v <- pointDistance(I, M2, lonlat = FALSE)
        #dist.denom <- pointDistance(M2, M1, lonlat = FALSE)
        return(list(#ResShare = round(X[2]/sum(X) * 100, 1), #round(v/(dist.denom/100), 1), 
                    EMPspaceutil = round(X[1]/(I[1]/100), 1),
                    EMPutil = round((dtx$HH + dtx$EMP) / ((I[2]+I[1])/100), 1),
                    DUutil = round(X[2]/(I[2]/100), 1),
                    HHutil = round((dtx$HH + dtx$EMP) / ((I[2]+I[1])/100), 1),
                    DUcapS = round(I[2], 0),
                    EMPcapS = round(I[1], 0)
               ))
    }
    res <- dts[, get_share(.SD), by = by]
    res[, type := "MU"]
    if(length(by) == 0)
        res <- cbind(res, dts)
    else 
        res <- merge(res, dts, by = by)
    res[, `:=`(res_share = DUcapS/DUcap, nonresshare = EMPcapS/EMPcap)]
    res[, ResShare := round(res_share/(res_share + nonresshare)*100, 1)]
    res[, `:=`(EMPcap_min = NULL, DUcap_min = NULL, res_share = NULL, nonresshare = NULL)]
    res[, DUutilBase := round(DUbase/DUcapS * 100, 1)]
    res[, HHutilBase := round(HHbase/DUcapS * 100, 1)]
    res[, EMPspaceutilBase := round(EMPspaceBase/EMPcapS * 100, 1)]
    res[, EMPutilBase := round(EMPbase/EMPcapS * 100, 1)]
    res
}

get.sqft.distr <- function(dt, by = c()) {
    dt[, SQFTcapSum := sum(SQFTcap), by = by]
    dt[, SQFTSum := sum(SQFT), by = by]
    dt[, SQFTCapDistr := round(SQFTcap/(SQFTcapSum/100),1)]
    dt[, SQFTDistr := round(SQFT/(SQFTSum/100),1)]
    return(copy(dt))
}

clean.summary <- function(dt) {
    res <- copy(dt)
    #res[type == "MU", `:=`(HHcap = NA, HHutil = NA, EMPcap = NA, EMPutil = NA)]
    res[is.infinite(DUutil) | DU < 0 | DUcap < 0 | type == "NR", DUutil := NA]
    res[is.infinite(DUutilBase) | DUbase < 0 | DUcap < 0 | type == "NR", DUutilBase := NA]
    res[is.infinite(HHutil) | HH < 0 | DUcap < 0 | type == "NR", HHutil := NA]
    res[is.infinite(HHutilBase) | HHbase < 0 | DUcap < 0 | type == "NR", HHutilBase := NA]
    res[is.infinite(EMPspaceutil) | EMPspace < 0 | EMPcap < 0 | type == "R", EMPspaceutil := NA]
    res[is.infinite(EMPspaceutilBase) | EMPspaceBase < 0 | EMPcap < 0 | type == "R", EMPspaceutilBase := NA]
    res[is.infinite(EMPutil) | EMP < 0 | EMPcap < 0 | type == "R", EMPutil := NA]
    res[is.infinite(EMPutilBase) | EMPbase < 0 | EMPcap < 0 | type == "R", EMPutilBase := NA]
    res[, SpaceUtilBase := ifelse(is.na(DUutilBase), EMPspaceutilBase, DUutilBase)]
    res[, SpaceUtil := ifelse(is.na(DUutil), EMPspaceutil, DUutil)]
    res[, AgentUtilBase := ifelse(is.na(HHutilBase), EMPutilBase, HHutilBase)]
    res[, AgentUtil := ifelse(is.na(HHutil), EMPutil, HHutil)]
    #res[is.infinite(SQFTutil) | is.na(SQFTutil), SQFTutil := NA]
    res[, `:=`(EMPspaceutil = NULL, DUutil = NULL, HHutil = NULL, EMPutil = NULL,
               EMPspaceutilBase = NULL, DUutilBase = NULL, HHutilBase = NULL, EMPutilBase = NULL)]
    return(res)
}
summarize.tot <- function(by = c()) {
    cat("\nsummarize.tot by", paste(by, collapse = ", "))
    dtmu <- all.pclsG[type == "MU", ]
    dtmu2 <- dtmu[, .(
                      DUbase = sum(pcl_resunits),
                      DU = sum(residential_units_real),
                      HHbase = sum(hhs.base),
                      HH = sum(households),
                      EMPspace = sum(job_capacity_real),
                      EMPspaceBase = sum(pcl_job_capacity),
                      EMP = sum(non_home_based_employment),
                      EMPbase = sum(jobs.base),
                      SQFTcap = sum(building_sqft_capacity),
                      SQFT = sum(building_sqft),
                      SQFTutil = round(sum(building_sqft)/sum(building_sqft_capacity)* 100,1),
                      ResShareSQFT = round(sum(building_sqft_res)/ sum(building_sqft) * 100, 1),
                      Nparcels = .N
    ), by = by]
    mu <- summarize.mixuse(dtmu, dtmu$real_mix, dt.to.join = dtmu2, by = by)
    res <- all.pclsG[type != "MU", 
                .(DUcap = sum(residential_units_capacity), 
                  DUcapS = NA,
                  DUbase = sum(pcl_resunits),
                  DU = sum(residential_units_real),
                  HHbase = sum(hhs.base),
                  HH = sum(households),
                  DUutilBase = round(sum(pcl_resunits)/sum(residential_units_capacity) * 100,1),
                  DUutil = round(sum(residential_units_real)/sum(residential_units_capacity) * 100,1),
                  HHutilBase = round(sum(hhs.base)/sum(residential_units_capacity) * 100,1),
                  HHutil = round(sum(households)/sum(residential_units_capacity) * 100,1),
                  EMPcap = sum(job_cap_capacity),
                  EMPcapS = NA,
                  EMPspaceBase = sum(pcl_job_capacity),
                  EMPspace = sum(job_capacity_real),
                  EMPbase = sum(jobs.base),
                  EMP = sum(non_home_based_employment),
                  EMPspaceutilBase = round(sum(pcl_job_capacity)/sum(job_cap_capacity) * 100,1),
                  EMPspaceutil = round(sum(job_capacity_real)/sum(job_cap_capacity) * 100,1),
                  EMPutilBase = round(sum(jobs.base)/sum(job_cap_capacity) * 100,1),
                  EMPutil = round(sum(non_home_based_employment)/sum(job_cap_capacity) * 100,1),
                  SQFTcap = sum(building_sqft_capacity),
                  SQFT = sum(building_sqft),
                  SQFTutil = round(sum(building_sqft)/sum(building_sqft_capacity) * 100,1),
                  ResShare = round(sum(building_sqft_res)/ sum(building_sqft) * 100, 1),
                  ResShareSQFT = round(sum(building_sqft_res)/ sum(building_sqft) * 100, 1),
                  Nparcels = .N 
                  ), 
              by = c("type", by)]
    res <- rbind(res, mu)
    return(get.sqft.distr(copy(clean.summary(res)), by = by))
}




# Totals
tot <- list()
reg <- "Region"
tot[[reg]] <- summarize.tot()
setorder(tot[[reg]], type)

reg <- "County"
tot[[reg]] <- summarize.tot("county_id")
tot[[reg]] <- merge(counties.df, tot[[reg]])
setorder(tot[[reg]], type, county_id)

reg <- "City"
tot[[reg]] <- summarize.tot("city_id")
tot[[reg]] <- merge(cities[, .(city_id, city_name, county_id)], tot[[reg]])
setorder(tot[[reg]], type, city_id)

reg <- "FAZ"
tot[[reg]] <- summarize.tot("faz_id")
setorder(tot[[reg]], type, faz_id)

reg <- "RGCs"
tot[[reg]] <- summarize.tot("growth_center_id")
tot[[reg]] <- merge(gcenters[, .(growth_center_id, name, city_id)], tot[[reg]])
setorder(tot[[reg]], type, growth_center_id)


if(save.to.excel) {
    library("openxlsx")
    wb <- createWorkbook()
    sheets <- paste("Total Capacity - ", names(tot))
    for(sheet in sheets) addWorksheet(wb, sheet)
    isheet <- 0
    for(reg in names(tot))
        writeDataTable(wb, sheet = (isheet <- isheet + 1), tot[[reg]])
    saveWorkbook(wb, file = paste0(file.prefix, "_run_", run.name, ".xlsx"), overwrite = TRUE)
} else { # csv
    for(reg in names(tot))
        fwrite(tot[[reg]], file = paste0(file.prefix, reg, "_run_", run.name, ".csv"))
}

cat("\nComputing capacity finished.\n")
