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
    pclw[constraint_type == "units_per_lot", residential_units := ifelse(parcel_sqft >= 3000, maximum, 0)]
    # select one max for residential and one for non-res type, so that each parcel has 2 records at most
    pclwu <- rbind(
        pclw[pclw[constraint_type != "far", .I[which.max(residential_units)], by = .(parcel_id)]$V1][, constraint_type := "units_per_lot"],
        pclw[pclw[constraint_type == "far", .I[which.max(building_sqft)], by = .(parcel_id)]$V1]
    )
    pclwu[, mixed := .N > 1, by = parcel_id]
    
    # add county names
    pclwu[, county := factor(county_id, levels = c(33, 35, 53, 61), labels = c("King", "Kitsap", "Pierce", "Snohomish"))]
    return(pclwu)
}

# function for aggregating capacity to user-specific geography
aggregate.capacity <- function(pcl, by = "county", developable.factor = 1, 
                               res.ratios= c(30, 40, 50, 60, 70)) {
    # extract non-mix-use residential and non-residential parcels as their capacity will  not change with the ratio
    pcl_resid <- pcl[mixed==FALSE, .(parcel_id, units = residential_units, 
                                     remaining_capacity = pmax(0, residential_units - residential_units_built),
                                     developable = residential_units > developable.factor * residential_units_built)][
                                         , type := "residential-units"][!is.na(units)]
    
    pcl_nonresid <- pcl[mixed==FALSE, .(parcel_id, units = building_sqft, 
                                        remaining_capacity = pmax(0, building_sqft - non_residential_sqft_built), building_sqft_per_job,
                                        developable = building_sqft > developable.factor * non_residential_sqft_built)][
                                            , type := "non-residential-sqft"][!is.na(units)]
    
    pcl_nonresid_jobs <- pcl_nonresid[, .(parcel_id, remaining_capacity = remaining_capacity/building_sqft_per_job, developable)][
        , type := "non-residential-jobs"]
    pcl_nonresid[, building_sqft_per_job := NULL]
    
    pcl_non_mix <- merge(unique(pcl[, unique(c("parcel_id", by)), with = FALSE]), 
                         rbind(pcl_resid, 
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
        pcl_mix_res <- pcl[mixed==TRUE & constraint_type == "units_per_lot", 
                           .(parcel_id, units = ratio/100 * residential_units, units_built = residential_units_built)][
                               , `:=`(remaining_capacity = pmax(0, units - units_built), 
                                      developable = units > developable.factor * units_built,
                                      type = "residential-units")][!is.na(units)]
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
        pcl_mix <- merge(unique(pcl[, unique(c("parcel_id", by)), with = FALSE]), 
                         rbind(pcl_mix_res, 
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
    
    # some cleaning and computing shares
    res[, res_ratio := as.factor(res_ratio)]
    res[, type := factor(type, levels = c("residential-units", "non-residential-sqft", "non-residential-jobs"))]
    res[, `:=`(remaining_total_capacity = sum(remaining_capacity)), by = c("type", "res_ratio", by[-1])]
    res[, `:=`(percent_rem_cap = round(remaining_capacity/remaining_total_capacity * 100,1))]
    setnames(res, "units", "total_capacity")
    return(res)
}
