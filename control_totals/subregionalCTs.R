library(data.table)
library(RMySQL)

setwd("~/R/urbansimRtools/control_totals")

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
####### end of functions

# read inputs
CT.file <- '/Volumes/DataTeam/Projects/UrbanSim/NEW_DIRECTORY/Inputs/annual_control_totals/subregional_control_totals/controls_by_hhsize/unrolled_controls.csv'
CTs <- fread(CT.file)

# baseyear DB
base.db <- "2014_parcel_baseyear_core"
# working database for outputs
work.db <- "sandbox_hana"

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

# iterate over years to run Larry Blains method
years <- unique(CTpop$year)
for(i in 2:length(years)) {
  upd <- hhpop.control(years[i], years[i-1])
  CTpop[upd, household_count := i.household_count, on = c("city_id", "year", "pph")]
}
CTpop[, hhsize := NULL]

# Rebalance to handle any deviation from aggregate control
# by sampling proportionately to HH count
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

# create final input table
CTpop <- CTpop[year > 2014]
CTpop[, `:=`(total_number_of_households = household_count, 
             income_min = 0, income_max = -1, 
             persons_min = pph, persons_max = ifelse(pph < 7, pph, -1),
             workers_min = 0, workers_max = -1)]
qr <- dbSendQuery(mydb, "select * from annual_household_control_totals")
cttbl <- data.table(fetch(qr, n = -1))
cttbl.res <- cttbl[!year %in% unique(CTpop$year)]
cttbl.res <- rbind(cttbl.res, CTpop[, colnames(cttbl), with = FALSE])

dbClearResult(qr)
dbDisconnect(mydb)

# write outputs into working DB
mydb <- mysql.connection(work.db)
dbWriteTable(mydb, "annual_household_control_totals", cttbl.res, overwrite=TRUE)
dbDisconnect(mydb)
