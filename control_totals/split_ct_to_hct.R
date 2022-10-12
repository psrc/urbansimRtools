# Script for splitting jurisdictional control totals into 
# HCT and non-HCT parts. 
# Starting from HCT and non-HCT capacity shares for each city,
# the method iteratively increases those shares until the desired 
# regional shares are achieved.
#
# 2022/10/12
# Hana Sevcikova, PSRC

library(data.table)
library(openxlsx)
library(RMySQL)

# working directory
setwd("~/psrc/R/urbansimRtools/control_totals")

# Full name of the input file containing the control totals (not unrolled)
CT.file <- '~/psrc/R/control-total-vision2050/Control-Totals-LUV3RebasedTrg-2022-08-01.xlsx'

# temp file for reading in various inputs (not needed anymore)
# CTg.file <- '~/psrc/control_totals/LUV3_control_generator_20220928_non_decreasing.xlsx'

# parcel-level capacity file
capacity.file <- '~/psrc/R/urbansimRtools/capacity/CapacityPcl_res50-2022-09-26.csv'
    
save.results <- FALSE      # should results be stored in an Excel file
do.plot <- FALSE           # should plots be created
file.suffix <- "95-90-90" # suffix for file names (plots and Excel)

geo.name <- "subreg_id"   # name of the geography column

trgshare <- list(HH = 64.5, Emp = 75, HHPop = NA) # Regional shares to achieve
min.share <- list(HH = c(10, 10, 10), Emp = c(10, 10, 10), HHPop = NA)  # minimum growth shares in non-HCT areas (for RGs 1, 2, 3)
#min.share <- list(HH = c(5, 10, 10), Emp = c(5, 10, 10), HHPop = NA)

step <- c(1, 0.5, 0.25) # steps of scaling increase for RGs 1, 2, 3

use.mysql <- FALSE # if FALSE, base data are taken from base.data.file. 
                   # Set this to TRUE if run for the first time or if there is change in the DB.
base.db <- "2018_parcel_baseyear" # used if use.mysql is TRUE

save.base.data <- FALSE # should the base data pulled from mysql be saved. 
                        # Set this to TRUE if use.mysql is TRUE. It allows to 
                        # skip the mysql step next time around. 
base.data.file <- "base_data_shares.rda" # where to store or load from the base data

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
setnames(ct.hh, "control_id", "geo_id")

ct.emp <- data.table(read.xlsx(CT.file, sheet = "Emp"))
setnames(ct.emp, "control_id", "geo_id")

ct.pop <- data.table(read.xlsx(CT.file, sheet = "HHPop"))
setnames(ct.pop, "control_id", "geo_id")

targets <- list(HH = ct.hh[, .(geo_id, base20 = `2020`, trg50 = round(`2050`))], # order of the list items is important (HH must be first)
                Emp = ct.emp[, .(geo_id, base20 = `2020`, trg50 = round(`2050`))],
                HHPop = ct.pop[, .(geo_id, base20 = `2020`, trg50 = `2050`)])


# read capacity file
pcl.cap <- fread(capacity.file)
setnames(pcl.cap, geo.name, "geo_id")
pcl.cap[, is_tod := tod_id > 0]
#pcl.cap[, `:=`(DUnetcap = pmax(0, DUcapacity - DUbase), EMPnetcap = pmax(0, JOBSPcapacity - JOBSPbase))]
pcl.cap[, `:=`(DUtotcap = pmax(DUbase, DUcapacity), EMPtotcap = pmax(JOBSPbase, JOBSPcapacity))]

# aggregate capacity
SDcols <- c('DUtotcap', 'EMPtotcap')
geo.cap <- pcl.cap[, lapply(.SD, sum), by = c("geo_id", "is_tod"), .SDcols = SDcols]

# read 2020 HH and HHPop data from the DB
if(use.mysql) {
    mydb <- mysql.connection(base.db)
    qr <- dbSendQuery(mydb, paste0("select  t3.subreg_id, t3.tod_id > 0 as is_tod, count(t1.household_id) as households, sum(persons) as persons from households as t1",
                               " join buildings as t2 on t1.building_id=t2.building_id join ", base.db, 
                               ".parcels as t3 on t2.parcel_id=t3.parcel_id group by t3.", geo.name, ", is_tod"
                                ))
    hh_base <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
    setnames(hh_base, geo.name, "geo_id")

    qr <- dbSendQuery(mydb, paste0("select  t3.subreg_id, t3.tod_id > 0 as is_tod, count(t1.job_id) as jobs from jobs as t1",
                               " join buildings as t2 on t1.building_id=t2.building_id join ", base.db, 
                               ".parcels as t3 on t2.parcel_id=t3.parcel_id group by t3.", geo.name, ", is_tod"
                    ))
    job_base <- data.table(fetch(qr, n = -1))
    setnames(job_base, geo.name, "geo_id")
    dbClearResult(qr)

    base.data <- merge(hh_base, job_base, by = c("geo_id", "is_tod"), all = TRUE)

    qr <- dbSendQuery(mydb, "select * from subregs")
    geos <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
    setnames(geos, c("subreg_id", "subreg_name", "rgs_id"), c("geo_id", "name", "RGid"))

    base.data <- merge(geos, base.data, by = "geo_id", all = TRUE)
    base.data[is.na(is_tod), is_tod := FALSE]
    for(col in c("households", "persons", "jobs"))
        base.data[is.na(base.data[[col]]), (col) := 0]
    if(save.base.data)
        saveRDS(base.data, file = base.data.file)
} else {
    base.data <- readRDS(base.data.file)
}

base.data[, `:=`(hhtot = sum(households), poptot = sum(persons), jobtot = sum(jobs)), by = "geo_id"]
base.data[, `:=`(is_tod = as.logical(is_tod), hh_share = households/hhtot, pop_share = persons/poptot, job_share = jobs/jobtot)]
for(col in c("hh_share", "pop_share", "job_share")) base.data[is.na(base.data[[col]]), (col) := 1]
base.data <- merge(base.data, ct.hh[, .(geo_id, base20hhtot = `2020`)], by = "geo_id")
base.data[, hhbase20 := round(base20hhtot * hh_share)]
base.data <- merge(base.data, ct.pop[, .(geo_id, base20poptot = `2020`)], by = "geo_id")
base.data[, popbase20 := round(base20poptot * pop_share)]
base.data <- merge(base.data, ct.emp[, .(geo_id, base20emptot = `2020`)], by = "geo_id")
base.data[, empbase20 := round(base20emptot * job_share)]

CTgen.hh <- base.data[, .(geo_id, is_tod, RGid, name, base18 = households, base20 = hhbase20)]
CTgen.emp <- base.data[, .(geo_id, is_tod, RGid, name, base18 = jobs, base20 = empbase20)]
CTgen.pop <- base.data[, .(geo_id, is_tod, RGid, name, base18 = persons, base20 = popbase20)]

# CTgen.hh <- rbind(data.table(read.xlsx(CTg.file, sheet = "LUV3 HHs", rows = 14:75, cols = c(2:4,6), colNames = FALSE))[, is_tod := TRUE],
#                   data.table(read.xlsx(CTg.file, sheet = "LUV3 HHs", rows = 79:233, cols = c(2:4,6), colNames = FALSE))[, is_tod := FALSE])
# colnames(CTgen.hh)[1:4] <- c("geo_id", "RGid", "name", "base20")
# CTgen.hh[CTgen.hh[is_tod == TRUE], base20 := ifelse(is_tod, base20, base20 - i.base20), on = "geo_id"]

# # read 2020 Emp data
# CTgen.emp <- rbind(data.table(read.xlsx(CTg.file, sheet = "LUV3 Emp", rows = 14:75, cols = c(2:4,6), colNames = FALSE))[, is_tod := TRUE],
#                    data.table(read.xlsx(CTg.file, sheet = "LUV3 Emp", rows = 79:233, cols = c(2:4,6), colNames = FALSE))[, is_tod := FALSE])
# colnames(CTgen.emp)[1:4] <- c("geo_id", "RGid", "name", "base20")
# CTgen.emp[CTgen.emp[is_tod == TRUE], base20 := ifelse(is_tod, base20, base20 - i.base20), on = "geo_id"]
# 
# # read 2020 Pop data
# CTgen.pop <- rbind(data.table(read.xlsx(CTg.file, sheet = "LUV3 HHPop", rows = 14:75, cols = c(2:4,6), colNames = FALSE))[, is_tod := TRUE],
#                    data.table(read.xlsx(CTg.file, sheet = "LUV3 HHPop", rows = 79:233, cols = c(2:4,6), colNames = FALSE))[, is_tod := FALSE])
# colnames(CTgen.pop)[1:4] <- c("geo_id", "RGid", "name", "base20")
# CTgen.pop[CTgen.pop[is_tod == TRUE], base20 := ifelse(is_tod, base20, base20 - i.base20), on = "geo_id"]

# merge with capacity and compute net capacity
merge.with.capacity <- function(df, cap.df, what){
    setnames(cap.df, paste0(what, "totcap"), "totcap")
    df[cap.df, totcap := i.totcap, on = c("geo_id", "is_tod")]
    df[is.na(totcap), totcap := 0]
    df[, netcap := pmax(0, totcap - base20)]
    df[, geonetcap := sum(netcap), by = "geo_id"][, capshare := netcap/geonetcap*100]
    df[(is.infinite(capshare) | is.na(capshare)) & is_tod == TRUE, capshare := 100]
    df[(is.infinite(capshare) | is.na(capshare)) & is_tod == FALSE, capshare := 0]
    return(df)
}
CTgen.hh <- merge.with.capacity(copy(CTgen.hh), copy(geo.cap), what = "DU")
CTgen.emp<- merge.with.capacity(copy(CTgen.emp), copy(geo.cap), what = "EMP")

CTgens <- list(HH = CTgen.hh, Emp = CTgen.emp, HHPop = CTgen.pop)


CTdf <- checks <- todshare <- todshare.sub <- weights <- list()

# iterate over indicators
for(ind in names(targets)){
    cat("\n\nProcessing ", ind)
    cat("\n=====================")
    targets[[ind]][, trggrowth := trg50 - base20]
    CTdf[[ind]] <- merge(CTgens[[ind]], targets[[ind]][, .(geo_id, trggrowth, geotottarget.orig = trg50)], by = "geo_id")

    if(ind == "HHPop") {
        CTdf[[ind]] <- merge(CTdf[[ind]], CTdf[["HH"]][, .(geo_id, is_tod, has_tod, target.share)], by = c("geo_id", "is_tod"))
        CTdf[[ind]][is_tod == TRUE, wtrg := pmax(trggrowth, 0) * target.share/100]
        CTdf[[ind]][CTdf[[ind]][is_tod == TRUE], wtrg := ifelse(is_tod == TRUE, i.wtrg, trggrowth - i.wtrg), on = "geo_id"][is.na(wtrg), wtrg := trggrowth]
    } else {
        # aggregate TODs where there is no target growth or no HCT capacity
        if(ind == "HH")  # for jobs use HH geographies (HH must run first)
            aggr.geo <- CTdf[["HH"]][is_tod == TRUE & (trggrowth <= 500 | capshare == 0 | RGid > 3), geo_id]
        else { # check for employment that the filter still applies
            no.pass <- CTdf[[ind]][geo_id %in% aggr.geo & is_tod == TRUE & (trggrowth > 500 & capshare > 0 & RGid <= 3), geo_id]
            if(length(no.pass) > 0)
                warning("Locations to aggregate ", paste(no.pass, collapse = ", "), " didn't pass the aggregation filter for employment.")
        }
        CTdfaggr <- CTdf[[ind]][geo_id %in% aggr.geo, .(base18 = sum(base18), base20 = sum(base20), is_tod = FALSE, totcap = sum(totcap), netcap = sum(netcap), capshare = 100,
                               geonetcap = mean(geonetcap), geotottarget.orig = mean(geotottarget.orig), has_tod = FALSE), 
                           by = c("geo_id", "RGid", "name")]
        CTdfaggr[, trggrowth := geotottarget.orig - base20]
        CTdf[[ind]] <- rbind(CTdf[[ind]][!geo_id %in% CTdfaggr[, geo_id]], CTdfaggr, fill = TRUE)

        # mark rows that do not have TOD siblings
        CTdf[[ind]][, has_tod := TRUE]
        CTdf[[ind]][is_tod == FALSE & ! geo_id %in% CTdf[[ind]][is_tod == TRUE, geo_id], has_tod := FALSE]
        CTdf[[ind]][has_tod == FALSE, capshare := 100]
        
        # compute initial targets
        CTdf[[ind]][is_tod == TRUE, trg0 := pmin(netcap, trggrowth * capshare/100)]
        CTdf[[ind]][CTdf[[ind]][is_tod == TRUE], trg0 := ifelse(is_tod == TRUE, i.trg0, trggrowth - i.trg0), on = "geo_id"][is.na(trg0), trg0 := trggrowth] 
        
        # for non-HCT, if running over capacity, put the remainder into HCT 
        CTdf[[ind]][, overflow := 0]
        CTdf[[ind]][is_tod == FALSE & has_tod == TRUE & trg0 > netcap, `:=`(overflow = netcap - trg0, trg0 = netcap)]
        CTdf[[ind]][CTdf[[ind]][is_tod == FALSE & has_tod == TRUE], trg0 := ifelse(is_tod == TRUE, trg0 - i.overflow, trg0), on = "geo_id"]
        
        # compute initial target shares
        CTdf[[ind]][, target.share := round(trg0/trggrowth * 100, 1)]
        
        # set step increments
        for(id in 1:3)
            CTdf[[ind]][RGid == id, incr := step[id]]

        # some initialization
        CTdf[[ind]][, `:=`(wtrg = trg0, scale = 0)]
        
        # set minimum shares
        for(id in 1:3)
            CTdf[[ind]][RGid == id, minshare := min.share[[ind]][id]]

        # total target is larger than total capacity, growth only in HCT
        CTdf[[ind]][geonetcap < trggrowth, minshare := 0] 
        # if HCT is already above the max share, keep it at that level
        CTdf[[ind]][is_tod == TRUE & (100 - capshare) < minshare,  minshare := round(pmin(minshare, 100 - capshare), 1)]
    } 
    # compute regional HCT shares
    todshare[[ind]] <- CTdf[[ind]][is_tod == TRUE, sum(wtrg)]/CTdf[[ind]][, sum(wtrg, na.rm = TRUE)] * 100
    cat("\n\nStep 0:", todshare[[ind]])

    # prepare for iterating
    counter <- 1
    df <- copy(CTdf[[ind]])
    
    # The weights object will keep the various values from each iteration. Used for plotting purposes only.
    weights[[ind]] <- NULL
    weights[[ind]] <- rbind(weights[[ind]], df[is_tod == TRUE, .(geo_id, RGid, name, todcap.share = NA, incr = NA, 
                                                                 scale = NA, target.share, iter = 0)])
    weights[[ind]] <- rbind(weights[[ind]], data.table(geo_id = 0, RGid = -1, name = "Total", todcap.share = NA, incr = NA, 
                                                       scale = NA, target.share = todshare[[ind]], iter = 0))
    
    # iterate to achieve the desired regional TOD share (for HHPop use the HH results)
    while(ind != "HHPop" && todshare[[ind]] < trgshare[[ind]]) {
        # compute remaining capacity
        df[, remcap := pmax(0, netcap - wtrg)]
        df[is_tod == TRUE & abs(100 - target.share - minshare) <= 0.0001, remcap := 0]
        # compute shares of remaining HCT capacity
        df[is_tod == TRUE, todcap.share := remcap/sum(remcap)]
        # compute a scaling factor for the HCT shares
        df[is_tod == TRUE, scale := scale + incr * todcap.share]
        # for HCT compute new targets without going over capacity
        df[is_tod == TRUE, wtrg := pmin(netcap, trggrowth * pmin(1-minshare/100, (1+scale) * capshare/100))]
        # compute targets for non-HCT via a subtraction
        df[df[is_tod == TRUE], wtrg := ifelse(is_tod == TRUE, i.wtrg, trggrowth - i.wtrg), on = "geo_id"][is.na(wtrg), wtrg := trggrowth]
        # if non-HCT capacity is exceeded, add the remainder to HCT
        df[, overflow := 0]
        df[is_tod == FALSE & has_tod == TRUE & wtrg > netcap, `:=`(overflow = netcap - wtrg, wtrg = netcap)]
        df[df[is_tod == FALSE & has_tod == TRUE], wtrg := ifelse(is_tod == TRUE, wtrg - i.overflow, wtrg), on = "geo_id"]
        # update regional HCT shares
        todshare.old <- todshare[[ind]]
        todshare[[ind]] <- df[is_tod == TRUE, sum(wtrg)]/df[, sum(wtrg, na.rm = TRUE)] * 100
        # update HCT shares for each geography
        df[, target.share := round(wtrg/trggrowth * 100, 1)]
        # add data to the weights object
        weights[[ind]] <- rbind(weights[[ind]], df[is_tod == TRUE, .(geo_id, RGid, name, todcap.share = todcap.share * 100, incr, 
                                                                     scale = scale * 100, target.share, iter = counter)])
        weights[[ind]] <- rbind(weights[[ind]], data.table(geo_id = 0, RGid = -1, name = "Total", todcap.share = NA, incr = NA, 
                                                           scale = NA, target.share = todshare[[ind]], iter = counter))
        cat("\nStep ", counter, ":", todshare[[ind]])
        counter <- counter + 1
        # if desired regional HCT share achieved get out of the loop
        if(abs(todshare.old - todshare[[ind]]) <= 0.0001) break
    }
    # compute regional HCT shares while excluding non-HCT jurisdictions 
    todshare.sub[[ind]] <- df[is_tod == TRUE, sum(wtrg)]/df[has_tod == TRUE | (geo_id %in% aggr.geo & RGid <= 3), 
                                                                               sum(wtrg, na.rm = TRUE)] * 100
    # some post-processing
    df[, tottrg.final := base20 + wtrg] # total 2050 value
    df[has_tod == FALSE, target.share := 100]
    # for checking purposes if jurisdictional targets are the same
    df[, geotottarget.final := sum(tottrg.final), by = "geo_id"][, trgdif := geotottarget.final - geotottarget.orig]
    # sort rows
    df <- df[order(geo_id, -is_tod)]
    # assign new ids
    df[, control_id := geo_id]
    df[is_tod == TRUE, control_id := control_id + 1000]
    # compute HCT shares for each TOD 
    todshare.bytod <- df[is_tod == TRUE, sum(wtrg), by = "RGid"]
    todshare.bytod[df[, sum(wtrg, na.rm = TRUE), by = "RGid"][RGid %in% 1:3], share := V1/i.V1 * 100, on = "RGid"]
    # keep results
    checks[[ind]] <- copy(todshare.bytod)
    CTdf[[ind]] <- copy(df)
}

# assemble results
hhres <- CTdf[["HH"]][, .(subreg_id = control_id, geo_id, RGid, name, is_tod = as.integer(is_tod), DUtotcapacity = round(totcap),
                  DUnetcapacity = round(netcap), DUcapshare = round(capshare, 1), target_share = round(target.share, 1),
              target_growth_ini = round(trg0), target_growth_final = round(wtrg), HH2018 = base18, HH2020 = base20, HH2050 = round(tottrg.final))]

popres <- CTdf[["HHPop"]][, .(subreg_id = control_id, geo_id, RGid, name, is_tod = as.integer(is_tod), 
                    target_share = round(target.share, 1), target_growth_final = round(wtrg), HHPop2018 = base18, HHPop2020 = base20, 
                    HHPop2050 = round(tottrg.final))]
empres <- CTdf[["Emp"]][, .(subreg_id = control_id, geo_id, RGid, name, is_tod = as.integer(is_tod), EMPtotcapacity = round(totcap),
                          EMPnetcapacity = round(netcap), EMPcapshare = round(capshare, 1), target_share = round(target.share, 1),
                          target_growth_ini = round(trg0), target_growth_final = round(wtrg), Emp2018 = base18, Emp2020 = base20, Emp2050 = round(tottrg.final))]

# table of checks
check <- merge(merge(checks[["HH"]][, .(RGid, tod_share_hh = round(share, 1))], 
               checks[["HHPop"]][, .(RGid, tod_share_pop = round(share, 1))], by = "RGid"),
               checks[["Emp"]][, .(RGid, tod_share_emp = round(share, 1))], by = "RGid")[RGid %in% 1:3]
# attach subtotal check
check <- rbind(check, data.table(RGid = -2, tod_share_hh = round(todshare.sub[["HH"]], 2), 
                                 tod_share_pop = round(todshare.sub[["HHPop"]], 2),
                                 tod_share_emp = round(todshare.sub[["Emp"]], 2)))
# attach total check
check <- rbind(check, data.table(RGid = -1, tod_share_hh = round(todshare[["HH"]], 2), 
                                 tod_share_pop = round(todshare[["HHPop"]], 2),
                                 tod_share_emp = round(todshare[["Emp"]], 2)))

check[, RGid := as.character(RGid)][RGid == "-2", RGid := "Tot within RG"]
check[, RGid := as.character(RGid)][RGid == "-1", RGid := "Total"]

# interpolate results and create unrolled CTs
source("interpolate.R")

to.interpolate <- list(HHPop = popres, HH = hhres, Emp = empres)
CTs <- list(HHwork = hhres, HHPopwork = popres, EMPwork = empres, check = check)
unrolled <- NULL
years.to.fit <- c(seq(2020, 2040, by = 5), 2044, 2050)
for (indicator in names(to.interpolate)) {
    if(is.null(to.interpolate[[indicator]])) next
    CTs[[indicator]] <- interpolate.controls.with.ankers(to.interpolate[[indicator]], indicator, 
                                                         years.to.fit = years.to.fit, id.col = geo.name)
    this.unrolled <- unroll(CTs[[indicator]], indicator, new.id.col = geo.name)
    unrolled <- if(is.null(unrolled)) this.unrolled else merge(unrolled, this.unrolled, all = TRUE)
}
CTs[["unrolled"]] <- unrolled

# save results into an Excel file
if(save.results)
    write.xlsx(CTs, file = paste0("LUVit_ct_by_tod_generator_", file.suffix, "_", Sys.Date(), ".xlsx"))

# generate plots
if(do.plot){
    RGdf <- data.table(RGid = c(1:3, -1), RG = c("Metro", "Core Cities", "HCT Comm", "Region"))
    for(ind in c("HH", "Emp")){
        dat <- copy(weights[[ind]][RGid <= 3])
        
        dat[target.share == 0, target.share := NA]
        dat[todcap.share == 0 | is.na(target.share), todcap.share := NA]
        dat[RGdf, RG := i.RG, on = "RGid"]
        dat[, RG := factor(RG, levels = RGdf$RG)]
        g <- ggplot(dat[!is.na(target.share)], aes(x = iter)) + geom_line(aes(y = target.share, col = factor(geo_id))) + 
            xlab("iteration") + ylab("target TOD share") + facet_wrap(. ~ RG, ncol = 2) 
        g <- g + geom_text(data = dat[!is.na(target.share) & iter == 0], aes(x = iter, y = target.share, label = name, 
                                                                             col = factor(geo_id), hjust = 0, vjust = 1.5), size = 2.3) + theme(legend.position="none")
        
        g2 <- ggplot(dat[!is.na(todcap.share)], aes(x = iter)) + geom_line(aes(y = todcap.share, col = factor(geo_id))) + 
            xlab("iteration") + ylab("remaining TOD capacity share") + facet_wrap(. ~ RG) 
        g2 <- g2 + geom_text(data = dat[!is.na(todcap.share) & iter == 1], aes(x = iter, y = todcap.share, label = name, 
                                                                               col = factor(geo_id), hjust = 0, vjust = 1.5), size = 3) + theme(legend.position="none")
        
        pdf(paste0("target_shares_evol_", ind, "_", file.suffix, "_", Sys.Date(), ".pdf"), width = 8, height = 9)
        print(g)
        print(g2)
        dev.off()
    }
}