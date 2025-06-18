##################################################
# Script for splitting jurisdictional control totals 
# generated via https://github.com/psrc/control-total-vision2050/blob/master/run_creating_control_totals_from_targets.R
# into HCT and non-HCT parts. 
# For BY 2023, the input file was created manually, using the 2018 CTs and 
# taking into account 2023 observed values.
# 
# Starting from HCT and non-HCT capacity shares for each city,
# the method iteratively increases those shares
# (using weights based on the remaining HCT capacity and RG-specific scales) 
# until the given regional shares are achieved.
# The also script requires a parcel capacity file generated via
# https://github.com/psrc/urbansimRtools/blob/master/capacity/parcels_capacity.R
#
# Hana Sevcikova, PSRC, updated on 2025/06/16
##################################################

library(data.table)
library(openxlsx)
library(RMySQL)
library(ggplot2)

# working directory
setwd("~/psrc/R/urbansimRtools/control_totals")

# Full name of the input file containing the control totals (sheets with not unrolled CTs)
# This is the output of run_creating_control_totals_from_targets.R from the link above
#CT.file <- '~/psrc/R/control-total-vision2050/Control-Totals-LUVit-2022-11-15.xlsx' # BY 2018
CT.file <- 'inputs/Control-Totals-RTP26-2025-06-11.xlsx' # BY 2023

# parcel-level capacity file
# generated via https://github.com/psrc/urbansimRtools/blob/master/capacity/parcels_capacity.R
#capacity.file <- '~/psrc/R/urbansimRtools/capacity/CapacityPcl_res50-2023-01-11.csv' # BY 2018
#capacity.file <- '~/psrc/R/urbansimRtools/capacity/CapacityPclNoSampling_res50-2025-06-03.csv' # BY 2023
capacity.file <- '~/psrc/R/urbansimRtools/capacity/CapacityPclNoSampling_res50-2025-06-17_hb1110.csv' # BY 2023 with HB1110
  
output.dir <- "outputs"
output.suffix <- paste0("-", Sys.Date())  # uniquely identify results from this split  
output.suffix <- paste0("_hb1110-", Sys.Date()) 

save.results <- TRUE      # should results be stored in an Excel file
results.file.name.prefix <- file.path(output.dir, paste0("LUVit_ct_by_tod_generator", output.suffix))  # only used if save.results is TRUE
do.plot <- TRUE           # should plots be created

base.year <- 2023
base.year.in.targets <- base.year # from which base year to start in the targets file (for BY 2018 this should be 2020)
target.year <- 2050       # what is the target year in the targets file    
aggregate.no.growth.areas <- FALSE

#geo.name.split <- "control_hct_id"   # name of the geography column in the parcel file
geo.name.split <- "subreg_id"
geo.name.nosplit <- "control_id"

trgshare <- list(HH = 65, Emp = 75, HHPop = NA) # Regional shares to achieve (HHPop is derived from HH and it is usually a little higher)

# Scenarios are defined as the minimum growth shares in non-HCT areas (for RGs 1, 2, 3)
# E.g. c(10, 10, 10) means the growth into HCT areas cannot be more than 90% 
# (unless the capacity share is higher) for all types of RGs.
scenarios <- list(list(HH = c(10, 10, 10), Emp = c(10, 10, 10), HHPop = NA)#,  
                  #list(HH = c(5, 10, 10), Emp = c(5, 10, 10), HHPop = NA)
                  )

step <- c(1, 0.5, 0.25) # increments for scaling the iterative increase for RGs 1, 2, 3 (i.e. RG=1 grows the fastest)

use.mysql <- FALSE  # if FALSE, base data are taken from base.data.file. 
                   # Set this to TRUE if run for the first time or if there is change in the DB.
base.db <- paste0(base.year, "_parcel_baseyear") # used if use.mysql is TRUE

save.base.data <- TRUE # should the base data pulled from mysql be saved. 
                        # Set this to TRUE if use.mysql is TRUE. It allows to 
                        # skip the mysql step next time around. 
base.data.file <- file.path("inputs", paste0("base_data_shares_", base.year, ".rda")) # where to store or load from the base data

if((save.results || do.plot) && !dir.exists(output.dir))
    dir.create(output.dir, recursive = TRUE)

## Functions
###############
# Connecting to Mysql
mysql.connection <- function(dbname = "2018_parcel_baseyear") {
    # credentials can be stored in a file (as one column: username, password, host)
    if(file.exists("creds.txt")) {
        creds <- read.table("creds.txt", stringsAsFactors = FALSE)
        un <- creds[1,1]
        psswd <- creds[2,1]
        if(nrow(creds) > 2) h <- creds[3,1] 
        else h <- .rs.askForPassword("host:")
    } else {
        un <- .rs.askForPassword("username:")
        psswd <- .rs.askForPassword("password:")
        h <- .rs.askForPassword("host:")
    }
    dbConnect(MySQL(), user = un, password = psswd, dbname = dbname, host = h)
}


# read control totals
ct.hh <- data.table(read.xlsx(CT.file, sheet = "HH"))
setnames(ct.hh, c(geo.name.nosplit, as.character(base.year.in.targets), as.character(target.year)), 
         c("nosplit_geo_id", "base", "target"))

ct.emp <- data.table(read.xlsx(CT.file, sheet = "Emp"))
setnames(ct.emp, c(geo.name.nosplit, as.character(base.year.in.targets), as.character(target.year)), 
         c("nosplit_geo_id", "base", "target"))

ct.pop <- data.table(read.xlsx(CT.file, sheet = "HHPop"))
setnames(ct.pop, c(geo.name.nosplit, as.character(base.year.in.targets), as.character(target.year)), 
         c("nosplit_geo_id", "base", "target"))

targets <- list(HH = ct.hh[, .(nosplit_geo_id, base, target = round(target))], # order of the list items is important (HH must be first)
                Emp = ct.emp[, .(nosplit_geo_id, base, target = round(target))],
                HHPop = ct.pop[, .(nosplit_geo_id, base, target = round(target))])
targets[["HH"]][ , `:=`(trg.pph = targets[["HHPop"]][ , (target-base)]/(target-base), 
                        trg.pop = targets[["HHPop"]][ , target] - targets[["HHPop"]][ , base])]
targets[["HH"]][is.na(trg.pph), trg.pph := 0]

# read capacity file
pcl.cap <- fread(capacity.file)
setnames(pcl.cap, c(geo.name.split, geo.name.nosplit), c("split_geo_id", "nosplit_geo_id"))
#pcl.cap[, is_tod := tod_id > 0]
#pcl.cap[, `:=`(DUnetcap = pmax(0, DUcapacity - DUbase), EMPnetcap = pmax(0, JOBSPcapacity - JOBSPbase))]
pcl.cap[, `:=`(DUtotcap = pmax(DUbase, DUcapacity), EMPtotcap = pmax(JOBSPbase, JOBSPcapacity))]

# aggregate capacity
SDcols <- c('DUtotcap', 'EMPtotcap')
geo.cap <- pcl.cap[, lapply(.SD, sum), by = c("split_geo_id", "nosplit_geo_id"), .SDcols = SDcols]

# read base year HH, HHPop and jobs data from the DB
if(use.mysql) {
    mydb <- mysql.connection(base.db)
    cat("\nLoading households from mysql and merging them with buildings and parcels")
    qr <- dbSendQuery(mydb, paste0("select  t3.", geo.name.nosplit, ", t3.", geo.name.split, ", 
                                   count(t1.household_id) as households, sum(persons) as persons from households as t1",
                               " join buildings as t2 on t1.building_id=t2.building_id join ", base.db, 
                               ".parcels as t3 on t2.parcel_id=t3.parcel_id group by t3.", geo.name.split
                                ))
    hh_base <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
    setnames(hh_base, c(geo.name.split, geo.name.nosplit), c("split_geo_id", "nosplit_geo_id"))

    cat("\nLoading jobs from mysql and merging them with buildings and parcels")
    qr <- dbSendQuery(mydb, paste0("select  t3.", geo.name.nosplit, ", t3.", geo.name.split,
                                   ", count(t1.job_id) as jobs from jobs as t1",
                               " join buildings as t2 on t1.building_id=t2.building_id join ", base.db, 
                               ".parcels as t3 on t2.parcel_id=t3.parcel_id group by t3.", geo.name.split
                    ))
    job_base <- data.table(fetch(qr, n = -1))
    setnames(job_base, c(geo.name.split, geo.name.nosplit), c("split_geo_id", "nosplit_geo_id"))
    dbClearResult(qr)

    base.data <- merge(hh_base, job_base, by = c("split_geo_id", "nosplit_geo_id"), all = TRUE)

    cat("\nLoading Xwalk tables")
    qr <- dbSendQuery(mydb, "select * from controls")
    geos <- unique(data.table(fetch(qr, n = -1)))
    dbClearResult(qr)
    setnames(geos, c(geo.name.nosplit, "control_name"), c("nosplit_geo_id", "name"))

    qr <- dbSendQuery(mydb, "select * from subregs")
    geos2 <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
    setnames(geos2, c("subreg_id", "rgs_id"), c("split_geo_id", "RGid"))
    geos2[, nosplit_geo_id := ifelse(split_geo_id > 1000, split_geo_id - 1000, split_geo_id)]

    geos <- merge(geos, geos2, by = c("nosplit_geo_id", "county_id"))[, subreg_name := NULL]

    base.data <- merge(geos, base.data, by = c("split_geo_id", "nosplit_geo_id"), all = TRUE)

    for(col in c("households", "persons", "jobs"))
        base.data[is.na(base.data[[col]]), (col) := 0]
    if(save.base.data)
        saveRDS(base.data, file = base.data.file)
    cat("\nClosing mysql connection.\n")
    dbDisconnect(mydb)
} else {
    base.data <- readRDS(base.data.file)
}
# base.data is now a table with jurisdiction disaggregated into HCT and non-HCT

# Not used: Approximate 2020 values by taking the same shares between TOD and non-TOD like in 2018
# and using the 2020 total from the input file

# add columns for totals over the no-split areas
base.data[, `:=`(hhtot = sum(households), poptot = sum(persons), jobtot = sum(jobs)), by = "nosplit_geo_id"]
#base.data[, `:=`(is_tod = as.logical(is_tod), hh_share = households/hhtot, pop_share = persons/poptot, job_share = jobs/jobtot)]
#for(col in c("hh_share", "pop_share", "job_share")) base.data[is.na(base.data[[col]]), (col) := 1]
base.data <- merge(base.data, ct.hh[, .(nosplit_geo_id, hhtot_base_nosplit = base)], by = "nosplit_geo_id")
#base.data[, hhbase20 := round(base20hhtot * hh_share)]
base.data <- merge(base.data, ct.pop[, .(nosplit_geo_id, poptot_base_nosplit = base)], by = "nosplit_geo_id")
#base.data[, popbase20 := round(base20poptot * pop_share)]
base.data <- merge(base.data, ct.emp[, .(nosplit_geo_id, emptot_base_nosplit = base)], by = "nosplit_geo_id")
#base.data[, empbase20 := round(base20emptot * job_share)]

# add is_tod column
base.data[, is_tod := split_geo_id != nosplit_geo_id]

# separate HH, HHPop and jobs into three tables 
CTgen.hh <- base.data[, .(split_geo_id, nosplit_geo_id, is_tod, RGid, name, base = households, 
                          pph_base = persons/households, 
                          pph_base_nosplit = poptot_base_nosplit/hhtot_base_nosplit)][
                              , pph.ratio := pph_base/pph_base_nosplit]
# make sure PPH in TOD areas is smaller than the average in the non-split area and vice versa
CTgen.hh[is_tod == TRUE, pph.ratio := pmin(pph.ratio, 0.9999)]   # TOD records
CTgen.hh[is_tod == FALSE, pph.ratio := pmax(pph.ratio, 1-0.9999)] # non-TOD records
CTgen.emp <- base.data[, .(split_geo_id, nosplit_geo_id, is_tod, RGid, name, base = jobs)]
CTgen.pop <- base.data[, .(split_geo_id, nosplit_geo_id, is_tod, RGid, name, base = persons)]


# merge with capacity and compute net capacity in base year as well as HCT capacity shares
merge.with.capacity <- function(df, cap.df, what){
    setnames(cap.df, paste0(what, "totcap"), "totcap")
    df[cap.df, totcap := i.totcap, on = c("split_geo_id", "nosplit_geo_id")]
    df[is.na(totcap), totcap := 0]
    df[, netcap := pmax(0, totcap - base)]
    df[, geonetcap := sum(netcap), by = "nosplit_geo_id"][, capshare := netcap/geonetcap*100]
    df[(is.infinite(capshare) | is.na(capshare)) & is_tod == TRUE, capshare := 100]
    df[(is.infinite(capshare) | is.na(capshare)) & is_tod == FALSE, capshare := 0]
    return(df)
}
CTgen.hh <- merge.with.capacity(copy(CTgen.hh), copy(geo.cap), what = "DU")
CTgen.emp<- merge.with.capacity(copy(CTgen.emp), copy(geo.cap), what = "EMP")

CTgens <- list(HH = CTgen.hh, Emp = CTgen.emp, HHPop = CTgen.pop)

dat.for.bars <- list() 
for(min.share in scenarios) { #  iterate over scenarios of growth limits
    file.suffix <- paste(100-min.share[["HH"]], collapse = "-") # suffix for file names (plots and Excel)

    CTdf <- checks <- todshare <- todshare.sub <- weights <- list()
    aggr.geo <- c()
    # iterate over indicators
    for(ind in names(targets)){
        cat("\n\nProcessing ", ind)
        cat("\n=====================")
        
        # compute the delta target for each jurisdiction
        targets[[ind]][, trggrowth := target - base]

        # merge it with the HCT-split jurisdictions
        CTdf[[ind]] <- merge(CTgens[[ind]], targets[[ind]][, .(nosplit_geo_id, trggrowth, geotottarget.orig = target)], by = "nosplit_geo_id")
        if(ind == "HH") {
            CTdf[[ind]] <- merge(CTdf[[ind]], targets[[ind]][, .(nosplit_geo_id, geotrg.pph = trg.pph, geotrg.pop = trg.pop)], by = "nosplit_geo_id")
            CTdf[[ind]][, trg.pph := pph.ratio * geotrg.pph][, wtrg.pph := trg.pph]
        }
        if(aggregate.no.growth.areas) {
            # Aggregate split jurisdictions where there is no target growth (i.e. less than 500), 
            # no HCT capacity or RGid is not 1,2,3
            if(ind == "HH")  # for jobs use HH geographies (HH must run first), so that we have identical geographies for HH and jobs
                aggr.geo <- CTdf[["HH"]][is_tod == TRUE & (trggrowth <= 500 | capshare == 0 | RGid > 3), nosplit_geo_id]
            else { # check for employment that the filter still applies
                if(ind == "Emp"){
                    no.pass <- CTdf[[ind]][nosplit_geo_id %in% aggr.geo & is_tod == TRUE & (trggrowth > 500 & capshare > 0 & RGid <= 3), nosplit_geo_id]
                    if(length(no.pass) > 0)
                        warning("Locations to aggregate ", paste(no.pass, collapse = ", "), " didn't pass the aggregation filter for employment.")
                }
            }
            if(ind != "HHPop")
                CTdfaggr <- CTdf[[ind]][nosplit_geo_id %in% aggr.geo, .(base = sum(base), is_tod = FALSE, 
                                                                        totcap = sum(totcap), netcap = sum(netcap), capshare = 100,
                                                                        geonetcap = mean(geonetcap), geotottarget.orig = mean(geotottarget.orig), 
                                                                        has_tod = FALSE), 
                                        by = c("nosplit_geo_id", "RGid", "name")]
            else CTdfaggr <- CTdf[[ind]][nosplit_geo_id %in% aggr.geo, .(base = sum(base), is_tod = FALSE, 
                                                                         geotottarget.orig = mean(geotottarget.orig)), 
                                         by = c("nosplit_geo_id", "RGid", "name")]
            if(ind == "HH")
                CTdfaggr <- merge(CTdfaggr, 
                                  CTdf[[ind]][nosplit_geo_id %in% aggr.geo, .(pph_base = mean(pph_base_nosplit), 
                                                                              pph.ratio = 1, geotrg.pph = mean(geotrg.pph),
                                                                              geotrg.pop = mean(geotrg.pop), trg.pph = mean(geotrg.pph),
                                                                              wtrg.pph = mean(geotrg.pph), is_tod = FALSE), 
                                              by = c("nosplit_geo_id", "RGid", "name")], 
                                  by = c("nosplit_geo_id", "is_tod", "RGid", "name"))
            CTdfaggr[, trggrowth := geotottarget.orig - base]
            
            # Keep just one entry for each aggregated geography
            CTdf[[ind]] <- rbind(CTdf[[ind]][!nosplit_geo_id %in% CTdfaggr[, nosplit_geo_id]], 
                                 CTdfaggr, fill = TRUE)
        }    
        if(ind == "HHPop") {
            # For HHPop use the same target shares as for HH (thus, HH must run first)
            CTdf[[ind]] <- merge(CTdf[[ind]], CTdf[["HH"]][, .(nosplit_geo_id, is_tod, has_tod, target.share, pop = wtrg * wtrg.pph)], by = c("nosplit_geo_id", "is_tod"))
            #CTdf[[ind]][is_tod == TRUE, wtrg := pmax(trggrowth, 0) * target.share/100]
            CTdf[[ind]][is_tod == TRUE, wtrg := pop]
            CTdf[[ind]][CTdf[[ind]][is_tod == TRUE], wtrg := ifelse(is_tod == TRUE, i.wtrg, trggrowth - i.wtrg), on = "nosplit_geo_id"][is.na(wtrg), wtrg := trggrowth]
        } else {
            # mark rows that do not have TOD siblings
            CTdf[[ind]][, has_tod := TRUE]
            CTdf[[ind]][is_tod == FALSE & ! nosplit_geo_id %in% CTdf[[ind]][is_tod == TRUE, nosplit_geo_id], has_tod := FALSE]
            CTdf[[ind]][has_tod == FALSE, capshare := 100]
            
            # compute initial targets based on HCT capacity shares, while not exceeding capacity
            CTdf[[ind]][is_tod == TRUE, trg0 := pmin(netcap, trggrowth * capshare/100)]
            CTdf[[ind]][CTdf[[ind]][is_tod == TRUE], trg0 := ifelse(is_tod == TRUE, i.trg0, trggrowth - i.trg0), on = "nosplit_geo_id"][is.na(trg0), trg0 := trggrowth] 
            
            # for non-HCT, if running over capacity, put the remainder into HCT 
            CTdf[[ind]][, overflow := 0]
            CTdf[[ind]][is_tod == FALSE & has_tod == TRUE & trg0 > netcap, `:=`(overflow = netcap - trg0, trg0 = netcap)]
            CTdf[[ind]][CTdf[[ind]][is_tod == FALSE & has_tod == TRUE], trg0 := ifelse(is_tod == TRUE, trg0 - i.overflow, trg0), on = "nosplit_geo_id"]
            
            # compute initial target shares
            CTdf[[ind]][, target.share := round(trg0/trggrowth * 100, 1)]
            
            # set step increments for the different RGs
            for(id in 1:3)
                CTdf[[ind]][RGid == id, incr := step[id]]
            
            # some initialization
            CTdf[[ind]][, `:=`(wtrg = trg0, scale = 0)]
            
            # set minimum growth shares based on this scenario
            for(id in 1:3)
                CTdf[[ind]][RGid == id, minshare := min.share[[ind]][id]]
            
            # if the total growth target is larger than the total capacity in the geography, 
            # growth will go only into HCT, achieved by removing the restriction on max HCT growth
            CTdf[[ind]][geonetcap < trggrowth, minshare := 0]
            
            # if HCT is already above the max share, keep it at that level
            CTdf[[ind]][is_tod == TRUE & (100 - capshare) < minshare,  minshare := round(pmin(minshare, 100 - capshare), 1)]
        } 
        # compute regional HCT shares
        if(ind == "HH") {
            # adjust pph
            #CTdf[[ind]][, max.wtrg.pph := wtrg.pph + 0.5]
            CTdf[[ind]][, max.wtrg.pph := geotrg.pph + 0.5]
            while(TRUE) {
                CTdf[[ind]][, wtrg.pop := wtrg * pmax(1.2, wtrg.pph)]#[is.na(wtrg.pop), wtrg.pop := 0]
                CTdf[[ind]][has_tod == FALSE, wtrg.pop := geotrg.pop]
                CTdf[[ind]][CTdf[[ind]][is_tod == TRUE], wtrg.pop := ifelse(is_tod == TRUE, wtrg.pop, geotrg.pop - i.wtrg.pop), on = "nosplit_geo_id"][is.na(wtrg.pop), wtrg.pop := geotrg.pop]
                CTdf[[ind]][, wtrg.pph := wtrg.pop / wtrg]
                if(all(CTdf[[ind]][is_tod == FALSE & has_tod == TRUE, wtrg.pph <= max.wtrg.pph] )) break
                CTdf[[ind]][is_tod == TRUE & nosplit_geo_id %in% CTdf[[ind]][is_tod == FALSE & has_tod == TRUE & wtrg.pph > max.wtrg.pph, nosplit_geo_id], wtrg.pph := wtrg.pph * 1.01]
            }
            todshare[[ind]] <- CTdf[[ind]][is_tod == TRUE, sum(wtrg * wtrg.pph)] / CTdf[[ind]][, sum(wtrg * wtrg.pph, na.rm = TRUE)] * 100
            
        } else 
            todshare[[ind]] <- CTdf[[ind]][is_tod == TRUE, sum(wtrg)]/CTdf[[ind]][, sum(wtrg, na.rm = TRUE)] * 100
        cat("\n\nStep 0:", todshare[[ind]])
        
        # prepare for iterating
        counter <- 1
        df <- copy(CTdf[[ind]])
        
        # The weights object will keep the various values from each iteration. Used for plotting and debugging purposes only.
        weights[[ind]] <- NULL
        weights[[ind]] <- rbind(weights[[ind]], df[is_tod == TRUE, .(nosplit_geo_id, RGid, name, todcap.share = NA, incr = NA, 
                                                                     scale = scale, target.share, remcap = NA, wtrg, iter = 0)])
        weights[[ind]] <- rbind(weights[[ind]], data.table(nosplit_geo_id = 0, RGid = -1, name = "Total", todcap.share = NA, incr = NA, 
                                                           scale = NA, target.share = todshare[[ind]], remcap = NA, wtrg = NA, iter = 0))
        
        # iterate to achieve the desired regional TOD share (for HHPop use the HH results)
        while(ind != "HHPop" && todshare[[ind]] < trgshare[[ind]]) {
            # compute remaining capacity
            df[, remcap := pmax(0, netcap - wtrg)][, true_remcap := remcap]
            df[is_tod == TRUE & abs(100 - target.share - minshare) <= 0.0001, remcap := 0]
            
            # compute shares of remaining regional HCT capacity
            df[is_tod == TRUE, todcap.share := remcap/sum(remcap)]
            
            # compute a scaling factor for the HCT shares
            df[is_tod == TRUE, scale := scale + incr * todcap.share]
            
            # for HCT compute new targets without going over capacity
            df[is_tod == TRUE, wtrg := pmin(netcap, trggrowth * pmin(1-minshare/100, (1+scale) * capshare/100))]
            
            # compute targets for non-HCT via a subtraction
            df[df[is_tod == TRUE], wtrg := ifelse(is_tod == TRUE, i.wtrg, trggrowth - i.wtrg), on = "nosplit_geo_id"][is.na(wtrg), wtrg := trggrowth]
            
            # if non-HCT capacity is exceeded, add the remainder to HCT
            df[, overflow := 0]
            df[is_tod == FALSE & has_tod == TRUE & wtrg > netcap, `:=`(overflow = netcap - wtrg, wtrg = netcap)]
            df[df[is_tod == FALSE & has_tod == TRUE], wtrg := ifelse(is_tod == TRUE, wtrg - i.overflow, wtrg), on = "nosplit_geo_id"]
            
            # update regional HCT shares
            todshare.old <- todshare[[ind]]
            if(ind == "HH") {
                # adjust pph
                while(TRUE) {
                    df[, wtrg.pop := wtrg * pmax(1.2, wtrg.pph)]#[is.na(wtrg.pop), wtrg.pop := 0]
                    df[has_tod == FALSE, wtrg.pop := geotrg.pop]
                    df[df[is_tod == TRUE], wtrg.pop := ifelse(is_tod == FALSE, geotrg.pop - i.wtrg.pop, wtrg.pop), on = "nosplit_geo_id"][is.na(wtrg.pop), wtrg.pop := geotrg.pop]
                    #df[df[is_tod == TRUE & pph.ratio <= 1], wtrg.pop := ifelse(is_tod == FALSE & pph.ratio > 1, geotrg.pop - i.wtrg.pop, wtrg.pop), on = "geo_id"]
                    #df[df[is_tod == TRUE & pph.ratio > 1], wtrg.pop := ifelse(is_tod == FALSE & pph.ratio <= 1, geotrg.pop - i.wtrg.pop, wtrg.pop), on = "geo_id"]
                    df[, wtrg.pph := wtrg.pop / wtrg]
                    if(all(df[is_tod == FALSE & has_tod == TRUE, wtrg.pph <= max.wtrg.pph])) break
                    df[is_tod == TRUE & nosplit_geo_id %in% df[is_tod == FALSE & has_tod == TRUE & wtrg.pph > max.wtrg.pph, nosplit_geo_id], wtrg.pph := wtrg.pph * 1.01]
                }
                todshare[[ind]] <- df[is_tod == TRUE, sum(wtrg * wtrg.pph)] / df[, sum(wtrg * wtrg.pph, na.rm = TRUE)] * 100
                todshare[["HH2"]] <- df[is_tod == TRUE, sum(wtrg)] / df[, sum(wtrg, na.rm = TRUE)] * 100
                #CTdf[[ind]][, wtrg.pph := df$wtrg.pph][, wtrg.pop := df$wtrg.pop]
            } else 
                todshare[[ind]] <- df[is_tod == TRUE, sum(wtrg)]/df[, sum(wtrg, na.rm = TRUE)] * 100
            
            # update HCT growth share for each geography
            df[, target.share := round(wtrg/trggrowth * 100, 1)]
            
            # add data to the weights object
            weights[[ind]] <- rbind(weights[[ind]], df[is_tod == TRUE, .(nosplit_geo_id, RGid, name, todcap.share = todcap.share * 100, incr, 
                                                                         scale = scale * 100, target.share, remcap = true_remcap, wtrg, iter = counter)])
            weights[[ind]] <- rbind(weights[[ind]], data.table(nosplit_geo_id = 0, RGid = -1, name = "Total", todcap.share = NA, incr = NA, 
                                                               scale = NA, target.share = todshare[[ind]], remcap = NA, wtrg= NA, iter = counter))

            cat("\nStep ", counter, ":", todshare[[ind]])
            counter <- counter + 1
            df[, true_remcap := pmax(0, netcap - wtrg)]
            
            # if the desired regional HCT share achieved get out of the loop
            if(abs(todshare.old - todshare[[ind]]) <= 0.0001) break
        }
        
        # compute regional HCT shares while excluding non-HCT jurisdictions 
        todshare.sub[[ind]] <- df[is_tod == TRUE, sum(wtrg)]/df[has_tod == TRUE | (nosplit_geo_id %in% aggr.geo & RGid <= 3), 
                                                                sum(wtrg, na.rm = TRUE)] * 100
        # some post-processing
        df[, tottrg.final := base + wtrg] # total 2050 value
        df[has_tod == FALSE, target.share := 100]
        
        # for checking purposes whether jurisdictional targets match
        df[, geotottarget.final := sum(tottrg.final), by = "nosplit_geo_id"][, trgdif := geotottarget.final - geotottarget.orig]
        
        # sort rows
        df <- df[order(nosplit_geo_id, -is_tod)]
        
        # assign new ids to the HCT geographies
        #df[, control_id := geo_id]
        #df[is_tod == TRUE, control_id := control_id + 1000]
        
        # compute total HCT shares for each TOD 
        todshare.bytod <- df[is_tod == TRUE, sum(wtrg), by = "RGid"]
        todshare.bytod[df[, sum(wtrg, na.rm = TRUE), by = "RGid"][RGid %in% 1:3], share := V1/i.V1 * 100, on = "RGid"]
        
        # keep results
        checks[[ind]] <- copy(todshare.bytod)
        CTdf[[ind]] <- copy(df)
    }
    # TODO: If aggregation is on, it might generate records with subreg_id being NA. Need to debug it.
    # Originally it was written for the case that the control_hcts get aggregate and re-enumerated,
    # but now with using fixed control_hct_id it doesn't work.
    
    # assemble results
    hhres <- CTdf[["HH"]][, .(subreg_id = split_geo_id, nosplit_geo_id, RGid, name, is_tod = as.integer(is_tod), DUtotcapacity = round(totcap),
                              DUnetcapacity = round(netcap), DUcapshare = round(capshare, 1), target_share = round(target.share, 1),
                              target_growth_ini = round(trg0), target_growth_final = round(wtrg), 
                              HHbase = base, HHtarget = round(tottrg.final), 
                              PPHbase = pph_base, PPHtarget = CTdf[["HHPop"]]$tottrg.final / tottrg.final)]
    
    popres <- CTdf[["HHPop"]][, .(subreg_id = split_geo_id, nosplit_geo_id, RGid, name, is_tod = as.integer(is_tod), 
                                  target_share = round(target.share, 1), target_growth_final = round(wtrg), HHPopbase = base, 
                                  HHPoptarget = round(tottrg.final))]
    empres <- CTdf[["Emp"]][, .(subreg_id = split_geo_id, nosplit_geo_id, RGid, name, is_tod = as.integer(is_tod), EMPtotcapacity = round(totcap),
                                EMPnetcapacity = round(netcap), EMPcapshare = round(capshare, 1), target_share = round(target.share, 1),
                                target_growth_ini = round(trg0), target_growth_final = round(wtrg), Empbase = base, Emptarget = round(tottrg.final))]
    
    checkdf <- merge(hhres[, .(HH = sum(HHtarget)), by = "nosplit_geo_id"], popres[, .(Pop = sum(HHPoptarget)), by = "nosplit_geo_id"], by = "nosplit_geo_id")
    checkdf <- merge(checkdf, targets[["HH"]][, .(nosplit_geo_id, HHtrg = target)], by = "nosplit_geo_id")
    checkdf <- merge(checkdf, targets[["HHPop"]][, .(nosplit_geo_id, Poptrg = target)], by = "nosplit_geo_id")
    cat("\nLargest diffs: HH = ", max(checkdf[, abs(HH - HHtrg)]), ", HHPop = ", max(checkdf[, abs(Pop - Poptrg)]))

    # table of checks
    check <- merge(merge(checks[["HH"]][, .(RGid, tod_share_hh = round(share, 1))], 
                         checks[["HHPop"]][, .(RGid, tod_share_pop = round(share, 1))], by = "RGid"),
                   checks[["Emp"]][, .(RGid, tod_share_emp = round(share, 1))], by = "RGid")[RGid %in% 1:3]
    
    # attach subtotal check
    check <- rbind(check, data.table(RGid = -2, tod_share_hh = round(todshare.sub[["HH"]], 2), 
                                     tod_share_pop = round(todshare.sub[["HHPop"]], 2),
                                     tod_share_emp = round(todshare.sub[["Emp"]], 2)))
    
    # attach total check
    check <- rbind(check, data.table(RGid = -1, tod_share_hh = round(todshare[["HH2"]], 2), 
                                     tod_share_pop = round(todshare[["HHPop"]], 2),
                                     tod_share_emp = round(todshare[["Emp"]], 2)))
    check[, RGid := as.character(RGid)][RGid == "-2", RGid := "Tot within RG"]
    check[, RGid := as.character(RGid)][RGid == "-1", RGid := "Total"]
    
    # interpolate results and create unrolled CTs
    source("interpolate.R")
    
    setnames(popres, c("HHPopbase", "HHPoptarget"), 
             paste0(c("HHPop", "HHPop"), c(base.year.in.targets, target.year)))
    setnames(hhres, c("HHbase", "HHtarget", "PPHbase", "PPHtarget"), 
             paste0(c("HH", "HH", "PPH", "PPH"), 
                    c(base.year.in.targets, target.year, base.year.in.targets, target.year)))
    setnames(empres, c("Empbase", "Emptarget"), 
             paste0(c("Emp", "Emp"), c(base.year.in.targets, target.year)))
    
    
    to.interpolate <- list(HHPop = popres, HH = hhres, Emp = empres)
    CTs <- list(HHwork = hhres, HHPopwork = popres, EMPwork = empres, check = check)
    unrolled <- NULL
    years.to.fit <- unique(c(base.year, base.year.in.targets, seq(2020, 2040, by = 5), 2044, target.year)) # desired years
    years.to.fit <- sort(years.to.fit[years.to.fit >= base.year])
    for (indicator in names(to.interpolate)) {
        if(is.null(to.interpolate[[indicator]])) next
        CTs[[indicator]] <- interpolate.controls.with.ankers(to.interpolate[[indicator]], indicator, 
                                                             anker.years = c(base.year.in.targets, target.year),
                                                             years.to.fit = years.to.fit, id.col = geo.name.split)
        this.unrolled <- unroll(CTs[[indicator]], indicator, new.id.col = geo.name.split)
        unrolled <- if(is.null(unrolled)) this.unrolled else merge(unrolled, this.unrolled, all = TRUE)
    }
    CTs[["unrolled"]] <- unrolled
    
    # aggregate to regional and interpolate into all years
    years.to.fit <- base.year:target.year
    unrolled.reg <- NULL
    years <- unique(c(base.year, base.year.in.targets, target.year))
    for (indicator in names(to.interpolate)) {
        if(is.null(to.interpolate[[indicator]])) next
        tmpdf <- copy(to.interpolate[[indicator]])
        setnames(tmpdf, paste0(indicator, years), paste0("V", years))
        regCT <- tmpdf[, lapply(.SD, sum), .SDcols = paste0("V", years)][, subreg_id := -1]
        setnames(regCT, paste0("V", years), paste0(indicator, years))
        regCTintp <- interpolate.controls.with.ankers(regCT, indicator, anker.years = years,
                                                             years.to.fit = years.to.fit, id.col = geo.name.split)
        this.unrolled <- unroll(regCTintp, indicator, new.id.col = geo.name.split)
        unrolled.reg <- if(is.null(unrolled.reg)) this.unrolled else merge(unrolled.reg, this.unrolled, all = TRUE)
    }
    CTs[["unrolled regional"]] <- unrolled.reg
    
    # save results into an Excel file
    if(save.results)
        write.xlsx(CTs, file = paste0(results.file.name.prefix, "_", file.suffix, ".xlsx"))
    
    # generate plots
    if(do.plot){
        # evolution of HCT shares over all iterations
        RGdf <- data.table(RGid = c(1:3, -1), RG = c("Metro", "Core Cities", "HCT Comm", "Region"))
        for(ind in c("HH", "Emp")){
            if(! ind %in% names(dat.for.bars))
                dat.for.bars[[ind]] <- list()
            dat <- copy(weights[[ind]][RGid <= 3])
            
            dat[target.share == 0, target.share := NA]
            dat[todcap.share == 0 | is.na(target.share), todcap.share := NA]
            dat[RGdf, RG := i.RG, on = "RGid"]
            dat[, RG := factor(RG, levels = RGdf$RG)]
            g <- ggplot(dat[!is.na(target.share)], aes(x = iter)) + geom_line(aes(y = target.share, col = factor(nosplit_geo_id))) + 
                xlab("iteration") + ylab("target TOD share") + facet_wrap(. ~ RG, ncol = 2) 
            g <- g + geom_text(data = dat[!is.na(target.share) & iter == 0], aes(x = iter, y = target.share, label = name, 
                                                                                 col = factor(nosplit_geo_id), hjust = 0, vjust = 1.5), size = 2.3) + theme(legend.position="none")
            
            g2 <- ggplot(dat[!is.na(todcap.share)], aes(x = iter)) + geom_line(aes(y = todcap.share, col = factor(nosplit_geo_id))) + 
                xlab("iteration") + ylab("remaining TOD capacity share") + facet_wrap(. ~ RG) 
            g2 <- g2 + geom_text(data = dat[!is.na(todcap.share) & iter == 1], aes(x = iter, y = todcap.share, label = name, 
                                                                                   col = factor(nosplit_geo_id), hjust = 0, vjust = 1.5), size = 3) + theme(legend.position="none")
            
            pdf(file.path(output.dir, paste0("target_shares_evol_", ind, output.suffix, "_", file.suffix, ".pdf")), width = 8, height = 9)
            print(g)
            print(g2)
            dev.off()
            dat.for.bars[[ind]][[as.character(min.share[[ind]][1])]] <- dat
        }
    }
} # end loop for scenarios

## plotting iteration curves from different scenarios beside one another
# #dat90 <- copy(dat)
# # dat95 <- copy(dat)
# datt <- rbind(dat90[, max_share := "max in Metro: 90"], dat95[, max_share := "max in Metro: 95"])
# gt <- ggplot(datt[!is.na(target.share) & RGid != -1], aes(x = iter)) + geom_line(aes(y = target.share, col = factor(geo_id))) +
#     xlab("iteration") + ylab("target TOD share") + facet_wrap(RG ~ max_share, ncol = 2, scales = "free_x")
# gt <- gt + geom_text(data = datt[!is.na(target.share) & iter == 0 & RGid != -1], aes(x = iter, y = target.share, label = name,
#                                                                      col = factor(geo_id), hjust = 0, vjust = 1.5), size = 2.3) + theme(legend.position="none")
# print(gt)
# 
# pdf(paste0("target_shares_evol_Emp_comp90-95_", Sys.Date(), ".pdf"), width = 8, height = 12)
# print(gt)
# dev.off()

if(do.plot){
    # bar chart of starting and final shares
    main.bar <- 90
    point.bar <- setdiff(c(95, 90), main.bar)
    pdf(file.path(output.dir, paste0("hct_final_shares-main-", main.bar, output.suffix, ".pdf")), width = 16, height = 8)
    for(ind in c("HH", "Emp")){
        datbar <- NULL
        for(share in as.numeric(names(dat.for.bars[[ind]]))){
            datbar <- rbind(datbar, dat.for.bars[[ind]][[as.character(share)]][RGid > -1 & (iter == 0 | iter == max(iter))][, `:=`(max_share = 100-share, max_iter = max(iter))])
        }
        datbar[name == "Bothell" & nosplit_geo_id == 126, name := "Bothell (Sno)"]
        datbar[startsWith(name, "Mid-County"), name := "Uninc. Pierce HCT"]
        sortdat <- datbar[max_share == main.bar & iter == 0]
        sortdat$name <- reorder(sortdat$name, sortdat$target.share, decreasing = TRUE)
        datbar <- datbar[max_share == point.bar & iter == 0, target.share := 0]
        datbar[iter == 0, `:=`(what = "capacity")]
        datbar[iter == max_iter, `:=`(what = paste("max:", max_share))]
        datbar[datbar[iter == 0], share := ifelse(iter == 0, target.share, target.share - i.target.share), on = c("nosplit_geo_id", "max_share")]
        datbar <- datbar[order(share, decreasing = TRUE)]
        datbar[, what := factor(what, levels = c("max: 95", "max: 90", "capacity"))]
        datbar[, name := factor(name, levels = levels(sortdat$name))]
        
        point.legend <- paste("max:", point.bar)
        point.legend.values <- c(1)
        names(point.legend.values) <- point.legend
        gbar <- ggplot(data = datbar[what != point.legend]) + geom_bar(position="stack", stat="identity", aes(x = name, y = share, fill = what)) + 
            scale_x_discrete(guide = guide_axis(angle = 90)) + ylim(0, 100) +
            facet_grid(. ~ RG, scale = "free_x", space = "free") +
            scale_fill_discrete(name = "HCT shares") + xlab("") + ylab("HCT shares") +
            guides(fill = guide_legend(override.aes = list(shape = c(NA, NA) ), order = 1 )) +
            ggtitle(list(HH = "Households", Emp = "Employment")[[ind]])
        # uncomment for marking the 95-90-90 scenario
        gbar2 <- gbar #+ geom_point(data = datbar[what == point.legend], aes(x = name, y = share), shape = "circle", size = 2, show.legend = TRUE) +
            #scale_shape_manual(name = NULL, values = point.legend.values, guide = guide_legend(order = 2) ) 
        print(gbar2)
    }
    dev.off()
}

# # write out values for selected jurisdictions
# select.jur <- c("University Place", "Puyallup")
# out <- weights[["HH"]][name %in% select.jur]
# out <- out[CTdf[["HH"]][name %in% select.jur & is_tod == TRUE, ], .(name, juris_target_growth = i.trggrowth, iteration = iter, tod_share = target.share,
#                                                target_total = wtrg, remainingTODcapacity = remcap, TODcapacity_share = todcap.share,
#                                                increment = incr, weight = 1 + scale/100), on = .(name)][order(name, iteration)]
#fwrite(out, file = "selected_cities_split_ct.csv")
