# Script for merging city-level control totals with regional CTs 
# while distributing city-level totals between household sizes,
# using Larry Blain's method.
# Hana Sevcikova
# Updated 04/11/2023

options(error=quote(dump.frames("last.dump", TRUE)))

library(data.table)
library(RMySQL)
library(openxlsx)

# Users settings
#########
# working directory
setwd("~/psrc/R/urbansimRtools/control_totals")

# Full name of the input file containing the unrolled control totals
#CT.file <- '~/psrc/R/control-total-vision2050/Control-Totals-LUV3RebasedTrg-2022-08-01.xlsx'
CT.file <- 'LUVit_ct_by_tod_generator_90-90-90_2023-01-11.xlsx'

# baseyear DB
base.db <- "2018_parcel_baseyear"
parcels.db <- base.db

# working database where outputs are stored
work.db <- "sandbox_hana"

# set base year and last year
base.year <- 2018
last.year <- 2050

# file with borrowing distribution of household size for small geographies 
# (donors and recipients)
borrow.distr.file <- "borrow_distribution_2023Apr.txt"

# should outputs be stored in the DB and should existing tables be overwritten
store.outputs <- TRUE
overwrite.existing <- FALSE

# name of the geo column in parcels
geo.id <- "control_id"
# name of the geo column in CTs
ct.geo.id <- "subreg_id"
# name of the county_id column
county.id <- "county_id_orig"

# names of existing regional control tables (if NULL the CTs are read from a csv file)
reg.hh.ct.table <- "2018_parcel_baseyear.annual_household_control_totals_region" # we need regional 2045
reg.hh.ct.table <- NULL
reg.hh.ct.file <- "~/psrc/R/urbansimRtools/control_totals/regional_annual_household_control_totals-2023-04-11.csv"
reg.emp.ct.table <- "2018_parcel_baseyear.annual_employment_control_totals_region"
#reg.emp.ct.table <- NULL
reg.emp.ct.file <- "~/psrc/R/urbansimRtools/control_totals/regional_annual_employment_control_totals-2022-11-28.csv"

###### End of users settings


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

# Larry Blaine's formula
hhpop.control <- function(op.year, ref.year) {
  tmp <- CTpop[year == ref.year, .(hh = sum(household_count), pop = sum(household_count * pph)), by = .(geo_id, year, hhsize)]
  wtmp <- dcast(tmp, geo_id + year ~ hhsize, value.var = c("hh", "pop"))
  
  wtmp[CTs[year == op.year], `:=`(total_hhpop = i.total_hhpop, total_hh = i.total_hh), on = "geo_id"]
  wtmp[ , r1 := (total_hhpop - total_hh * pop_large / hh_large)/(pop_small - hh_small * pop_large / hh_large)]
  wtmp[ , r2 := (total_hh - (total_hhpop - total_hh * pop_large / hh_large)/(pop_small - hh_small * pop_large / hh_large) * hh_small) / hh_large]
  
  this.ctp <- CTpop[year == op.year]
  this.ctp[wtmp, `:=`(r1 = i.r1, r2 = i.r2), on = c("geo_id")]
  this.ctp[CTpop[year == ref.year], prev_household_count := i.household_count, on = c("geo_id", "pph")]
  this.ctp[hhsize == "small", household_count := prev_household_count * r1]
  this.ctp[hhsize == "large", household_count := prev_household_count * r2]
  this.ctp[, household_count := round(pmax(1, household_count))]
  return(this.ctp[, .(geo_id, year, pph, household_count)])
}

# Rebalance to match aggregated control totals
# by sampling proportionally to HH count (excluding the group 7+)
rebalance.hhs <- function() {
    orig.CTpop <- copy(CTpop)
    while(TRUE) {
        aggr <- CTpop[, .(tot = sum(household_count)), by = .(geo_id, year)]
        aggr[CTs, should_be := i.total_hh, on = c("geo_id", "year")]
        aggr <- aggr[!is.na(should_be)]
        aggr[, dif := should_be - tot]
        difs <- aggr[abs(dif) > 0]
        if(nrow(difs) == 0) break
        for (i in 1:nrow(difs)) {
            d <- difs[i, dif]
            sct <- CTpop[geo_id == difs[i, geo_id] & year == difs[i, year] & pph < 7]
            sampl <- sample(sct$pph, min(abs(d), sum(sct$household_count > 0)), prob = sct$household_count)
            idx <- which(CTpop$geo_id == difs[i, geo_id] & CTpop$year == difs[i, year] & CTpop$pph %in% sampl)
            CTpop[idx, household_count := household_count + sign(d)] # subtracts if d is negative, otherwise adds
        }
    }
    orig.CTpop[CTpop, new_household_count := i.household_count, on = c("geo_id", "year", "pph")]
    cat("\nRebalancing HHs: ", orig.CTpop[new_household_count > household_count, sum(new_household_count - household_count)], 
        " households added; ", orig.CTpop[new_household_count < household_count, sum(household_count - new_household_count)], 
        " households subtracted")
    return(orig.CTpop[new_household_count > household_count, sum(new_household_count - household_count)])
}

# Rebalance to match aggregated control totals
# of population by subtracting the difference from the highest (7+) PPH category 
rebalance.pop <- function() {
    orig.CTpop <- copy(CTpop)
    aggr <- CTpop[, .(tot = sum(hhpop)), by = .(geo_id, year)]
    aggr[CTpop[pph == 7, .(mean_pph = mean(mean_pph)), by = .(geo_id, year)], mean_pph := i.mean_pph, on = c("geo_id", "year")]
    aggr[CTs, should_be := i.total_hhpop, on = c("geo_id", "year")]
    aggr <- aggr[!is.na(should_be)]
    aggr[, dif := should_be - tot]
    difs <- aggr[dif < 0] # process only records where we are overestimating population 
    for (i in 1:nrow(difs)) {
      dhhs7 <- abs(difs[i, dif]/difs[i, mean_pph])
      idx <- which(CTpop$geo_id == difs[i, geo_id] & CTpop$year == difs[i, year] & CTpop$pph == 7)
      CTpop[idx, household_count := max(0, household_count - floor(dhhs7))]
      #stop("")
    }
    orig.CTpop[CTpop, `:=`(new_household_count = i.household_count, 
                           new_hhpop = i.household_count * mean_pph), on = c("geo_id", "year", "pph")]
    if(orig.CTpop[new_household_count < household_count, sum(hhpop - new_hhpop)] > 1000000) stop("")
    cat("\nRebalancing HHPop: ", orig.CTpop[new_household_count < household_count, sum(household_count - new_household_count)], 
        " households and ", orig.CTpop[new_household_count < household_count, sum(hhpop - new_hhpop)], 
        " persons subtracted")
}

# use borrowed distribution for small geographies
set.count.by.borrowed.distr <- function(bdist, max.small = 800){
  # filter borrowed distribution 
  aggr.base <- CTpop[year == base.year, .(tot = sum(household_count)), by = geo_id]
  bdist[aggr.base, base := i.tot, on = c(recipient_geo_id = "geo_id")]
  #bdist <- bdist[base < max.small] # consider as small geo those with HH < max.small (800)
  
  # Merge CTpop with bdist and compute donor distribution
  CTpop[bdist, donor_geo_id := i.donor_geo_id, on = c(geo_id = "recipient_geo_id")]
  CTpop[year == base.year, hhdistr := household_count / sum(household_count), by = geo_id]
  CTpop[CTpop[year == base.year], donor_distr := i.hhdistr, on = c(donor_geo_id = "geo_id", year = "year", pph = "pph")]
  # Use 2050 control to multiply donor distribution
  CTpop[CTs[year == last.year], CThh50 := i.total_hh, on = "geo_id"]
  CTpop[year == base.year & !is.na(donor_geo_id), household_count := pmax(1, round(donor_distr * CThh50))]
  
  # cleanup
  CTpop[, `:=`(donor_geo_id = NULL, CThh50 = NULL, hhdistr = NULL, donor_distr = NULL)]
}
####### end of functions

# Start of processing
#####################
# fix random seed
set.seed(1234)

# read inputs
#CTs <- fread(CT.file) # if csv file
CTs <- data.table(read.xlsx(CT.file, sheet = "unrolled")) # if Excel sheet
CTs[, year := as.integer(year)]
CTs <- CTs[year >= base.year]
setnames(CTs, ct.geo.id, "geo_id") # temporarily rename; will be renamed back

# create all combinations of cities, year and persons
cities <- unique(CTs$geo_id)
CTpop <- data.table(expand.grid(geo_id = cities, year = sort(unique(c(base.year, CTs$year))), pph = 1:7))

# Read HHs joined on buildings and parcels from mysql (need parcels to get control_id) 
mydb <- mysql.connection(base.db)

qr <- dbSendQuery(mydb, paste0("select  t3.", ct.geo.id, ", 
(case when t1.persons > 7 then 7 else t1.persons end) as pph, 
count(t1.household_id) as household_count from households as t1 
join buildings as t2 on t1.building_id=t2.building_id 
join ", parcels.db, ".parcels as t3 on t2.parcel_id=t3.parcel_id 
group by t3.", ct.geo.id, ", (case when t1.persons > 7 then 7 else t1.persons end)"))

hhs <- data.table(fetch(qr, n = -1))
dbClearResult(qr)

hhs[, year := base.year]
setnames(hhs, ct.geo.id, "geo_id") # temporary

qr <- dbSendQuery(mydb, paste0("select  t3.", ct.geo.id, 
", avg(t1.persons) as mean_pph from households as t1
join buildings as t2 on t1.building_id=t2.building_id 
join ", parcels.db, ".parcels as t3 on t2.parcel_id=t3.parcel_id 
where t1.persons >= 7 group by t3.", ct.geo.id))

hhs.avgpph <- data.table(fetch(qr, n = -1))
dbClearResult(qr)

qr <- dbSendQuery(mydb, paste0("select  t3.", county.id,
", avg(t1.persons) as mean_pph from households as t1
join buildings as t2 on t1.building_id=t2.building_id 
join ", parcels.db, ".parcels as t3 on t2.parcel_id=t3.parcel_id 
where t1.persons >= 7 group by t3.", county.id))

hhs.avgpph.county <- data.table(fetch(qr, n = -1))
dbClearResult(qr)

# get geo-county cross-walk
qr <- dbSendQuery(mydb, paste0("select ", ct.geo.id, ", county_id from subregs"))
geo.cnty <- data.table(fetch(qr, n = -1))
dbClearResult(qr)

setnames(hhs.avgpph, ct.geo.id, "geo_id")
setnames(hhs.avgpph.county, county.id, "county_id")
setnames(geo.cnty, ct.geo.id, "geo_id")

# merge base year CTs with CTpop 
CTpop[hhs, household_count := i.household_count, on = c("geo_id", "year", "pph")]
CTpop[is.na(household_count), household_count := 1]

# add county column
CTpop <- merge(CTpop, geo.cnty, by = "geo_id")

# add mean of pph 
CTpop[hhs.avgpph, `:=`(mean_pph = ifelse(pph == 7, i.mean_pph, pph)), 
      on = c("geo_id")]
CTpop[pph < 7 & is.na(mean_pph), mean_pph := pph]
CTpop[hhs.avgpph.county, mean_pph := ifelse(pph == 7 & (household_count < 5 | is.na(mean_pph)), i.mean_pph, pph), on = "county_id"]

CTpop[, hhsize := ifelse(pph < 3, "small", "large")]

# Use borrowed distribution for small geographies
bdist <- fread(borrow.distr.file)
set.count.by.borrowed.distr(bdist)

# Iterate over years to run Larry Blains method
years <- unique(CTpop$year)
for(i in 2:length(years)) {
  upd <- hhpop.control(years[i], years[i-1])
  CTpop[upd, household_count := i.household_count, on = c("geo_id", "year", "pph")]
}
CTpop[, hhsize := NULL]


CTpop[, hhpop := mean_pph * household_count]

# Rebalance to handle any deviation from aggregate control
for(i in 1:20){
  cat("\n\nIteration: ", i, "\n==================")
  rebalance.pop()
  CTpop[, hhpop := mean_pph * household_count]
  hhadded <- rebalance.hhs()
  CTpop[, hhpop := mean_pph * household_count]
  if(hhadded < 10) break
}

# create final input tables
# HHs
CTpop <- CTpop[year > base.year]
CTpop[, `:=`(total_number_of_households = household_count, 
             income_min = 0, income_max = -1, 
             persons_min = pph, persons_max = ifelse(pph < 7, pph, -1),
             workers_min = 0, workers_max = -1)]
setnames(CTpop, "geo_id", ct.geo.id)

if(is.null(reg.hh.ct.table)) { # get the regional totals from file
  cttbl <- fread(reg.hh.ct.file)
} else { # load from mysql
  qr <- dbSendQuery(mydb, paste0("select * from ", reg.hh.ct.table)) # load existing regional CTs
  cttbl <- data.table(fetch(qr, n = -1))
  dbClearResult(qr)
  #if("city_id" %in% colnames(cttbl))
  #  setnames(cttbl, "city_id", ct.geo.id)
}
cttbl[, (ct.geo.id) := -1]
cttbl <- cttbl[year >= base.year]
resCThh <- cttbl[!year %in% unique(CTpop$year)]
resCThh <- rbind(resCThh, CTpop[, colnames(cttbl), with = FALSE])

# jobs
if(is.null(reg.emp.ct.table)) { # get the regional totals from file
  cttbl <- fread(reg.emp.ct.file)
} else { # load from mysql
  qr <- dbSendQuery(mydb, paste0("select * from ", reg.emp.ct.table))
  cttbl <- data.table(fetch(qr, n = -1))
  dbClearResult(qr)
  #if("city_id" %in% colnames(cttbl))
  #  setnames(cttbl, "city_id", ct.geo.id)
}
cttbl[, (ct.geo.id) := -1]
cttbl <- cttbl[year >= base.year]
resCTjobs <- cttbl[!year %in% unique(CTs$year)]
newCTs <- CTs[, .(geo_id, year, total_number_of_jobs = total_emp, home_based_status = -1, sector_id = -1)]
setnames(newCTs, "geo_id", ct.geo.id)
resCTjobs <- rbind(resCTjobs, newCTs)

# disconnect DB
dbDisconnect(mydb)

# write outputs into working DB
if(store.outputs) {
  mydb <- mysql.connection(work.db)
  dbWriteTable(mydb, "annual_household_control_totals", resCThh, overwrite = overwrite.existing, row.names = FALSE)
  dbWriteTable(mydb, "annual_employment_control_totals", resCTjobs, overwrite = overwrite.existing, row.names = FALSE)
  dbDisconnect(mydb)
}

# Check results
checkDT <- CTpop[year > base.year, .(total_hh = sum(household_count), total_hhpop = sum(mean_pph * household_count)),
                by = c(ct.geo.id, "year")]
setnames(CTs, "geo_id", ct.geo.id)
checkDT[CTs, `:=`(diff_hh = total_hh - i.total_hh, diff_hhpop = total_hhpop - i.total_hhpop),
        on = c(ct.geo.id, "year")]

cat("\nMax differences in HH:", range(checkDT$diff_hh))
cat("\nMax differences in HHpop:", range(checkDT$diff_hhpop))
cat("\n")
