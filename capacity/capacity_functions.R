# Functions
#============

# Settings for the optional land overhead (used if apply.overhead = TRUE in compute.parcel.capacity).
# The overhead covers land not available for buildings: sidewalks and street area included in parcel_sqft
# (all parcels) and new streets, stormwater and open-space tracts when large parcels are subdivided.
# Overhead (in %) by max DU/acre, following the UrbanSim development templates
# (low density templates mostly 30%, medium 15-20%, high density 10%; FAR templates mostly 5-20%).
# small.share is the share of the overhead applied to parcels up to land.min; the full overhead applies
# from land.max on, linear in between (land in sqft), as in Opus urbansim_parcel units_proposed_fraction.
# small.share = 1: overhead does not depend on parcel size; 0: no overhead on small parcels.
default.overhead.settings <- list(dua.breaks = c(22, 60),        # DU/acre breaks
                                  dua.overhead = c(30, 15, 10),  # overhead for <= 22, (22, 60], > 60 DU/acre
                                  far.overhead = 10,             # overhead for FAR constraints
                                  small.share = 1,
                                  land.min = 0.5 * 43560,
                                  land.max = 2 * 43560)

# overhead (in %) for given max DU/acre values
dua.overhead <- function(dua, settings = default.overhead.settings)
    settings$dua.overhead[findInterval(dua, settings$dua.breaks, left.open = TRUE) + 1]

# scale overhead (in %) by parcel size
size.adjusted.overhead <- function(overhead, land.area, settings = default.overhead.settings) {
    ramp <- pmin(1, pmax(0, (land.area - settings$land.min) / (settings$land.max - settings$land.min)))
    overhead * (settings$small.share + (1 - settings$small.share) * ramp)
}

# function for computing capacity for each parcel
# Lot coverage is NOT applied: FAR (floor area / lot area) and DU/acre are both
# defined over the whole lot, so lot coverage is already reflected in them.
# Coverage only matters when converting height to FAR, which happens in the FLU imputation.
# The include.coverage argument is kept for backward compatibility with existing callers but is ignored.
# Set apply.coverage = TRUE only for sensitivity runs reproducing the previous method
# (FAR and DU/acre multiplied by coverage, capped at 0.95).
# Set apply.overhead = TRUE to mimic the land overhead of UrbanSim development templates
# (new streets, stormwater, open-space tracts needed when a large parcel is subdivided);
# see default.overhead.settings and size.adjusted.overhead below. The default gives zoned capacity.
# Set round.units = TRUE to compute units from DU/acre as UrbanSim proposals do
# (psrc_parcel.development_project_proposal.units_proposed_plus_minimum_1DU_per_legal_lot_yield):
# units are rounded per parcel and a legal lot (> 3499 sqft inside the UGB, > 9999 sqft outside)
# gets one unit if the unrounded yield is in (0, 0.5]. Requires is_inside_urban_growth_boundary in pcl.
compute.parcel.capacity <- function(pcl, constraints, job.sqft, include.coverage = FALSE, apply.coverage = FALSE,
                                    apply.overhead = FALSE, overhead.settings = default.overhead.settings,
                                    round.units = FALSE) {
    pclw <- merge(pcl, constraints, by = "plan_type_id", allow.cartesian=TRUE)
    pclw[job.sqft, building_sqft_per_job := i.building_sqft_per_job, on = c("generic_land_use_type_id", "zone_id")]

    # compute building sqft & residential units
    if(apply.coverage && "coverage" %in% colnames(pclw)) {
        pclw[, coverage := pmin(0.95, coverage)]
    } else pclw[, coverage := 1]
    # usable share of the parcel after the land overhead (1 if overhead not applied)
    pclw[, usable_ratio := 1]
    if(apply.overhead) {
        pclw[constraint_type == "units_per_acre",
             usable_ratio := 1 - size.adjusted.overhead(dua.overhead(maximum, overhead.settings), parcel_sqft, overhead.settings)/100]
        pclw[constraint_type == "far",
             usable_ratio := 1 - size.adjusted.overhead(overhead.settings$far.overhead, parcel_sqft, overhead.settings)/100]
    }
    pclw[constraint_type == "far", building_sqft := parcel_sqft * maximum * coverage * usable_ratio]
    pclw[constraint_type == "units_per_acre", residential_units := parcel_sqft * maximum / 43560 * coverage * usable_ratio]
    if(round.units) {
        pclw[constraint_type == "units_per_acre",
             residential_units := round(residential_units) +
                 (residential_units > 0 & residential_units <= 0.5 &
                      ((is_inside_urban_growth_boundary == 1 & parcel_sqft > 3499) |
                       (is_inside_urban_growth_boundary != 1 & parcel_sqft > 9999)))]
    }
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

# function for splitting capacity of mix-use parcels between residential and non-residential use,
# taking current built into account.
# The res.ratio (in %) is the default share of the parcel's development envelope given to residential use.
# If the current built of one use already takes a larger share of its envelope, that use keeps
# what is built and the other use gets only what is left of the envelope (but not less than its own built).
# E.g. with 50% ratio, 100 DU allowed and 70 DU built, residential gets 70 DU (not 50)
# and non-residential gets 30% (not 50%) of its FAR-based sqft.
# Input is the output of compute.parcel.capacity (plus *_built columns), returns one row per mix-use parcel.
split.mixed.capacity <- function(pcl, res.ratio = 50) {
    mix <- merge(pcl[mixed == TRUE & constraint_type != "far",
                     .(parcel_id, res_cap = residential_units, res_built = residential_units_built)],
                 pcl[mixed == TRUE & constraint_type == "far",
                     .(parcel_id, nonres_cap = building_sqft, nonres_built = non_residential_sqft_built, building_sqft_per_job)],
                 by = "parcel_id")
    for(col in c("res_cap", "res_built", "nonres_cap", "nonres_built"))
        mix[is.na(get(col)), (col) := 0]
    # share of each envelope that is already used by the current built
    mix[, res_used := fifelse(res_cap > 0, pmin(1, res_built / res_cap), as.numeric(res_built > 0))]
    mix[, nonres_used := fifelse(nonres_cap > 0, pmin(1, nonres_built / nonres_cap), as.numeric(nonres_built > 0))]
    # residential share: at least what residential built uses, at most what non-res built leaves
    mix[, res_share := pmin(pmax(res.ratio/100, res_used), 1 - nonres_used)]
    # never go below current built (relevant if built of both uses together exceed the envelope)
    mix[, `:=`(residential_units = pmax(res_built, res_share * res_cap),
               building_sqft = pmax(nonres_built, (1 - res_share) * nonres_cap))]
    return(mix[, .(parcel_id, residential_units, residential_units_built = res_built,
                   building_sqft, non_residential_sqft_built = nonres_built, building_sqft_per_job, res_share)])
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
        # split the mix-use capacity, taking current built into account
        mixsplit <- split.mixed.capacity(pcl, res.ratio = ratio)
        # construct residential part
        pcl_mix_res <- mixsplit[, .(parcel_id, units = residential_units, units_built = residential_units_built)][
                               , `:=`(remaining_capacity = pmax(0, units - units_built),
                                      developable = units > developable.factor * units_built,
                                      type = "residential-units")][!is.na(units)]
        # construct non-residential part
        pcl_mix_nonres <- mixsplit[, .(parcel_id, units = building_sqft,
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
