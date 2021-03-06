library(data.table)
library(RMySQL)

# Users settings
#########
# working directory
setwd("~/R/urbansimRtools/control_totals")

# Full name of the input file containing the unrolled control totals
CT.file <- '/Volumes/DataTeam/Projects/UrbanSim/NEW_DIRECTORY/Inputs/annual_control_totals/subregional_control_totals/controls_by_hhsize/unrolled_controls.csv'

# baseyear DB
base.db <- "2014_parcel_baseyear_core"

# working database where outputs are stored
work.db <- "sandbox_hana"

# should outputs be stored in the DB and should existing tables be overwritten
store.outputs <- TRUE
overwrite.existing <- FALSE

###### End of users settings


## Functions
###############
# Connecting to Mysql
mysql.connection <- function(dbname = "2014_parcel_baseyear_core") {
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
  tmp <- CTpop[year == ref.year, .(hh = sum(household_count), pop = sum(household_count * pph)), by = .(city_id, year, hhsize)]
  wtmp <- dcast(tmp, city_id + year ~ hhsize, value.var = c("hh", "pop"))
  
  wtmp[CTs[year == op.year], `:=`(total_hhpop = i.total_hhpop, total_hh = i.total_hh), on = "city_id"]
  wtmp[ , r1 := (total_hhpop - total_hh * pop_large / hh_large)/(pop_small - hh_small * pop_large / hh_large)]
  wtmp[ , r2 := (total_hh - (total_hhpop - total_hh * pop_large / hh_large)/(pop_small - hh_small * pop_large / hh_large) * hh_small) / hh_large]
  
  this.ctp <- CTpop[year == op.year]
  this.ctp[wtmp, `:=`(r1 = i.r1, r2 = i.r2), on = c("city_id")]
  this.ctp[CTpop[year == ref.year], prev_household_count := i.household_count, on = c("city_id", "pph")]
  this.ctp[hhsize == "small", household_count := prev_household_count * r1]
  this.ctp[hhsize == "large", household_count := prev_household_count * r2]
  this.ctp[, household_count := round(pmax(1, household_count))]
  return(this.ctp[, .(city_id, year, pph, household_count)])
}

# Rebalance to match aggregated control totals
# by sampling proportionally to HH count
rebalance <- function() {
  aggr <- CTpop[, .(tot = sum(household_count)), by = .(city_id, year)]
  aggr[CTs, should_be := i.total_hh, on = c("city_id", "year")]
  aggr <- aggr[!is.na(should_be)]
  aggr[, dif := should_be - tot]
  difs <- aggr[abs(dif) > 0]
  for (i in 1:nrow(difs)) {
    d <- difs[i, dif]
    sct <- CTpop[city_id == difs[i, city_id] & year == difs[i, year]]
    sampl <- sample(sct$pph, abs(d), prob = sct$household_count)
    idx <- which(CTpop$city_id == difs[i, city_id] & CTpop$year == difs[i, year] & CTpop$pph %in% sampl)
    CTpop[idx, household_count := household_count + sign(d)] # subtracts if d is negative, otherwise adds
  }
}

# use borrowed distribution for small geographies
set.count.by.borrowed.distr <- function(bdist, max.small = 800){
  # filter borrowed distribution 
  aggr.base <- CTpop[year == 2014, .(tot = sum(household_count)), by = city_id]
  bdist[aggr.base, base := i.tot, on = c(recipient_city_id = "city_id")]
  bdist <- bdist[base < max.small] # consider as small geo those with HH < max.small (800)
  
  # Merge CTpop with bdist and compute donor distribution
  CTpop[bdist, donor_city_id := i.donor_city_id, on = c(city_id = "recipient_city_id")]
  CTpop[year == 2014, hhdistr := household_count / sum(household_count), by = city_id]
  CTpop[CTpop[year == 2014], donor_distr := i.hhdistr, on = c(donor_city_id = "city_id", year = "year", pph = "pph")]
  
  # Use 2050 control to multiply donor distribution
  CTpop[CTs[year == 2050], CThh50 := i.total_hh, on = "city_id"]
  CTpop[year == 2014 & !is.na(donor_city_id), household_count := pmax(1, round(donor_distr * CThh50))]
  
  # cleanup
  CTpop[, `:=`(donor_city_id = NULL, CThh50 = NULL, hhdistr = NULL, donor_distr = NULL)]
}
####### end of functions

# Start of processing
#####################
# read inputs
CTs <- fread(CT.file)

# create all combinations of cities, year and persons
cities <- unique(CTs$city_id)
CTpop <- data.table(expand.grid(city_id = cities, year = sort(unique(c(2014, CTs$year))), pph = 1:7))

# Read HHs joined on buildings and parcels from mysql (need parcels to get city_id) 
mydb <- mysql.connection(base.db)

qr <- dbSendQuery(mydb, "select  t3.city_id, 
(case when t1.persons > 7 then 7 else t1.persons end) as pph, 
count(t1.household_id) as household_count from households as t1 
join buildings as t2 on t1.building_id=t2.building_id 
join parcels as t3 on t2.parcel_id=t3.parcel_id 
group by t3.city_id, (case when t1.persons > 7 then 7 else t1.persons end)")
hhs <- data.table(fetch(qr, n = -1))
dbClearResult(qr)

hhs[, year := 2014]

# merge 2014 data with CTpop 
CTpop[hhs, household_count := i.household_count, on = .(city_id, year, pph)]
CTpop[is.na(household_count), household_count := 1]

CTpop[, hhsize := ifelse(pph < 3, "small", "large")]

# Use borowed distribution for small geographies
bdist <- fread("borrow_distribution.txt")
set.count.by.borrowed.distr(bdist)

# Iterate over years to run Larry Blains method
years <- unique(CTpop$year)
for(i in 2:length(years)) {
  upd <- hhpop.control(years[i], years[i-1])
  CTpop[upd, household_count := i.household_count, on = c("city_id", "year", "pph")]
}
CTpop[, hhsize := NULL]

# Rebalance to handle any deviation from aggregate control
rebalance()

# create final input tables
# HHs
CTpop <- CTpop[year > 2014]
CTpop[, `:=`(total_number_of_households = household_count, 
             income_min = 0, income_max = -1, 
             persons_min = pph, persons_max = ifelse(pph < 7, pph, -1),
             workers_min = 0, workers_max = -1)]
qr <- dbSendQuery(mydb, "select * from annual_household_control_totals")
cttbl <- data.table(fetch(qr, n = -1))
resCThh <- cttbl[!year %in% unique(CTpop$year)]
resCThh <- rbind(resCThh, CTpop[, colnames(cttbl), with = FALSE])
dbClearResult(qr)

# jobs
qr <- dbSendQuery(mydb, "select * from annual_employment_control_totals")
cttbl <- data.table(fetch(qr, n = -1))
resCTjobs <- cttbl[!year %in% unique(CTs$year)]
resCTjobs <- rbind(resCTjobs, 
                   CTs[, .(city_id, year, total_number_of_jobs = total_emp, home_based_status = -1, sector_id = -1)]
                   )
dbClearResult(qr)

# disconnect DB
dbDisconnect(mydb)

# write outputs into working DB
if(store.outputs) {
  mydb <- mysql.connection(work.db)
  dbWriteTable(mydb, "annual_household_control_totals", resCThh, overwrite = overwrite.existing)
  dbWriteTable(mydb, "annual_employment_control_totals", resCTjobs, overwrite = overwrite.existing)
  dbDisconnect(mydb)
}

# Check results
checkDT <- CTpop[year > 2014, .(total_hh = sum(household_count), total_hhpop = sum(pph * household_count)),
                by = .(city_id, year)]
checkDT[CTs, `:=`(diff_hh = total_hh - i.total_hh, diff_hhpop = total_hhpop - i.total_hhpop),
        on = c("city_id", "year")]

cat("\nMax differences in HH:", range(checkDT$diff_hh))
cat("\nMax differences in HHpop:", range(checkDT$diff_hhpop))
cat("\n")
