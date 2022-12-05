## Script for generating regional control totals.
## For households, it disaggregates given HH and HHPop totals 
##     into groups by persons, workers and income.
## For jobs, it either copies a given table or it scales it to 
##     given forecast. TODO: implement generating job totals from REF.
## Results are written into csv files and optionally to a mysql DB.
## 
## Mike Jensen & Hana Sevcikova, PSRC 
## 2022-12-05

library(dplyr)
library(srvyr)
library(magrittr)
library(data.table)
library(DBI)
library(odbc)
library(RMySQL)
library(readxl)
library(tools)

# settings related to base year data
base_year <- 2018
get.data.from.rda <- TRUE # if TRUE, data is retrieved from a previously stored rda file
get.data.from.mysql <- FALSE # if FALSE, and get.data.from.rda is FALSE, data is retrieved from PUMs
base.db <- paste0(base_year,"_parcel_baseyear") # mysql DB (used only if get.data.from.mysql is TRUE)
store.base.data <- TRUE # save retrieved base year data into an rda file (not used if get.data.from.rda is TRUE) 
rda.file.name <- paste0("base_year_data_", base_year, "_for_CT.rda") # file name where to store the data

# where is the forecast data
get.forecast.from.elmer <- FALSE # if FALSE, the forecast is retrieved from a file
# the file is used only if get.forecast.from.elmer is FALSE
#forecast.file.name <- "J:/Projects/UrbanSim/NEW_DIRECTORY/Inputs/annual_control_totals/controls_from_2017_macro_forecast/Working_Update_annual_hh_control_totals_2018_forecasts_2018_BY.xlsx"
#forecast.file.name <- "~/psrc/control_totals/Working_Update_annual_hh_control_totals_2018_forecasts_2018_BY.xlsx"
forecast.file.name <- "~/psrc/R/urbansimRtools/control_totals/LUVit_ct_by_tod_generator_90-90-90_2022-11-22.xlsx"
# if the forecast the regional macroeconomic forecast  
is.forecast.ref <- FALSE # if FALSE it is assumed it comes from a version of unrolled control totals

# what should be done for jobs CTs (currently only scaling implemented)
create.emp.totals <- TRUE # if FALSE only HHs CTs are created
scale.emp.controls <- TRUE # if FALSE the totals from the table below are simply copied; otherwise they are scaled to forecast
emp.ct.table <- "psrc_2014_parcel_baseyear_just_friends.annual_employment_control_totals_lum_sector"

# where to write results
output.file.name.hh <- paste0("regional_annual_household_control_totals-", Sys.Date(), ".csv")
output.file.name.emp <- paste0("regional_annual_employment_control_totals-", Sys.Date(), ".csv")
output.ct.db <- "sandbox_hana" # if not NULL, output is also written into mysql DB
#output.ct.db <- base.db
overwrite.existing.in.db <- FALSE # should mysql tables be overwritten (only used if output.ct.db is not NULL)  
  
# Functions ---------------------------------------------------------

# Larry Blaine's formula
hhpop.control <- function(op.year, ref.year){
  wtmp <- CTpph[year==ref.year, .(hh = sum(household_count), pop = sum(household_count * pph)), by=.(year, hhsize)] %>%
    dcast(year ~ hhsize, value.var = c("hh", "pop")) %>% .[, year:=year + 1]
  wtmp[forecast[year==op.year], `:=`(total_hhpop = i.hh_pop, total_hh = i.household_count), on=.(year)]
  wtmp[, r1:=(total_hhpop - total_hh * pop_large / hh_large)/(pop_small - hh_small * pop_large / hh_large)]
  wtmp[, r2:=(total_hh - (total_hhpop - total_hh * pop_large / hh_large)/
                (pop_small - hh_small * pop_large / hh_large) * hh_small) / hh_large]
  
  this.ctp <- CTpph[year==op.year]
  this.ctp[wtmp, `:=`(r1=i.r1, r2=i.r2), on="year"]
  this.ctp[CTpph[year==ref.year], prev_household_count:=i.household_count, on = "pph"]
  this.ctp[hhsize=="small", household_count:=prev_household_count * r1]
  this.ctp[hhsize=="large", household_count:=prev_household_count * r2]
  this.ctp[, household_count:=round(pmax(1, household_count))]
  return(this.ctp[, .(year, pph, household_count)])
}

# Rebalance to match aggregated control totals
# by sampling proportionally to HH count
rebalance <- function() {
  while(TRUE) {
    aggr <- CTpph[, .(tot=sum(household_count)), by=.(year)]
    aggr[forecast, should_be:=i.household_count, on=.(year)]
    aggr <- aggr[!is.na(should_be)]
    aggr[, dif := should_be - tot]
    difs <- aggr[abs(dif) > 0]
    if(nrow(difs) == 0) break
    for (i in 1:nrow(difs)) {
      d <- difs[i, dif]
      sct <- CTpph[year == difs[i, year]]
      sampl <- sample(sct$pph, min(abs(d), sum(sct$household_count > 0)), prob = sct$household_count)
      idx <- which(CTpph$year == difs[i, year] & CTpph$pph %in% sampl)
      CTpph[idx, household_count := household_count + sign(d)] # subtracts if d is negative, otherwise adds
    }
  }
}

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

# income levels 
income.bins <- c(0, 50000, 75000, 100000, 0)
income.labels <- c("Under $50,000", "$50,000-$74,999", "$75,000-$99,999", "$100,000 or more")

# Data retrieval ----------------------------------------------------

if(get.data.from.rda){
  hhs_full <- readRDS(rda.file.name)
} else {

  if(!get.data.from.mysql){
    # Get household size combinations from relevant PUMS
    library(psrccensus)
    ref_data <- get_psrc_pums(1, base_year, "h", c("NP","HINCP","NWRK","BINCOME"))                     # Reference data from regional PUMS
    ref_data %<>% mutate(pph=case_when(as.integer(NP)>7 ~7L, !is.na(NP) ~as.integer(NP)),
                         workers=case_when(as.integer(NWRK)>3 ~4L, !is.na(NWRK) ~as.integer(NWRK)),
                         income=case_when(grepl("\\b25,000\\b", as.character(BINCOME)) ~"Under $50,000",
                                          !is.na(BINCOME) ~as.character(BINCOME))) %>%
      mutate(across(c(pph, workers), factor))
    hhs <- psrc_pums_count(ref_data, group_vars="pph", incl_na=FALSE) %>%                              # Counts by household size
      rename(year=DATA_YEAR) %>% setDT() %>% .[pph!="Total", .(year, pph, count, share)]
    hhs[, c("year", "pph"):=lapply(.SD, as.integer), .SDcols=c("year", "pph")]
    ref_data1 <- ungroup(ref_data) %>% filter(if_any(c(workers, income), ~ !is.na(.))) %>%
      group_by(pph) %>% group_by(interact(workers, income), .add=TRUE)
    hhs_full <- psrc_pums_count(ref_data1, group_vars="keep_existing", incl_na=FALSE) %>%              # Counts by full breakdown of CT categories
      .[income!="Total" & workers!="Total", .(year, pph, workers, income, count, share)] %>% .[, year:=base_year]
    levels(hhs_full$income) <- income.labels
    hhs_full[, c("year", "pph", "workers"):=lapply(.SD, as.integer), .SDcols=c("year", "pph", "workers")]
  } else { # get data from mysql
    ## Get household size combinations from UrbanSim baseyear itself
    prsn_sql <- "CASE WHEN h.persons < 7 THEN h.persons ELSE 7 END"
    wrkr_sql <- "CASE WHEN workers < 4 THEN h.workers ELSE 4 END"
    inc_sql <- paste("CASE WHEN h.income >= 100000 THEN '", income.labels[4], "'",
                     "WHEN h.income >= 75000 THEN '", income.labels[3], "'",
                     "WHEN h.income >= 50000 THEN '", income.labels[2], "'",
                     "ELSE '", income.labels[1], "' END")
    
    hhs_sql <- paste("SELECT", base_year, "AS year,", prsn_sql, "AS pph,", 
                     wrkr_sql, "AS workers,", inc_sql, "as income, count(*) AS count", 
                     "FROM", base.db,".households", "AS h",
                     "GROUP BY", prsn_sql, ",",  wrkr_sql, ",", inc_sql)
    
    
    mysql_connection <- mysql.connection(base.db)
    
    hhs_full <- DBI::dbGetQuery(mysql_connection, DBI::SQL(hhs_sql)) %>% setDT()
    sums <- hhs_full[,.(hh_tot=sum(count)), by=.(pph)]
    setkey(hhs_full, pph)
    setkey(sums, pph)
    hhs_full[sums, share:=count/hh_tot]
    hhs_full[, income := trimws(income)]
    DBI::dbDisconnect(mysql_connection)
  } 
  if(store.base.data) saveRDS(hhs_full, file = rda.file.name)
}

if(get.forecast.from.elmer) {
  # Retrieve macro forecast from Elmer
  hh_4cast_sql <- paste("SELECT p.data_year AS year, p.population AS hh_pop, h.households AS household_count",
                        "FROM Elmer.Macroeconomic.v_pop AS p JOIN Elmer.Macroeconomic.v_households AS h ON p.data_year=h.data_year",
                        "WHERE p.group_name='InHouseholds';")
  
  elmer_connection <- DBI::dbConnect(odbc::odbc(),
                                     driver = "ODBC Driver 17 for SQL Server",
                                     server = "AWS-PROD-SQL\\Sockeye",
                                     database = "Elmer",
                                     trusted_connection = "yes",
                                     port = 1433)
  
  forecast <- DBI::dbGetQuery(elmer_connection, DBI::SQL(hh_4cast_sql))  %>% setDT()
  DBI::dbDisconnect(elmer_connection)
} else { 
  # Load household population and households from a file
  # It can be either a csv or an Excel file
  if(file_ext(forecast.file.name) == "csv") forecast <- fread(forecast.file.name) # it should have columns year, hh_pop, household_count, (optionally job_count)
  else {
    if(is.forecast.ref) { # it is Macroeconomic forecast
      forecast <- data.table(read_excel(forecast.file.name, sheet="2018 REF Forecast Data",                             
                                  range="G28:AM30", col_names=as.character(2018:2050))) %>% 
              .[c(1,3),] %>% data.table::transpose(keep.names="year") %>% setnames(c("V1", "V2"), c("hh_pop", "household_count"))
      forecast[, year := as.integer(year)]
    } else { # the forecast comes from a sheet in the same format as unrolled control totals
      forecast <- data.table(read_excel(forecast.file.name, sheet="unrolled regional"))
      forecast <- forecast[, .(year = as.integer(year), hh_pop = total_hhpop, household_count = total_hh, job_count = total_emp)]
    }
  }
}

# 0. Fix random seed
set.seed(1234)

# 1. Households by size ---------------------------------------------

hhs_full[, income := trimws(income)]

CTpph <- data.table(expand.grid(year = sort(unique(forecast$year)), pph = 1:7))                    # create all combinations of year and pph

CTpph[hhs_full[, .(count = sum(count)), by = .(year, pph)], household_count := i.count, on = .(year, pph)]                                     # merge base year hh counts with CTpph 
CTpph[is.na(household_count), household_count := 1]
CTpph[, hhsize := ifelse(pph < 3, "small", "large")]

years <- unique(CTpph$year)
for(i in 2:length(years)) {                                                                        # Iterate over years to run Larry Blaine's method
  upd <- hhpop.control(years[i], years[i-1])
  CTpph[upd, household_count := i.household_count, on = c("year", "pph")]
}
CTpph[, hhsize := NULL]

rebalance()                                                                                        # Rebalance to handle any deviation from aggregate control


# 2. Workers and Income brackets ------------------------------------

CTpph %<>% setkeyv(c("year", "pph"))                                                               # Keys for easier joining
hhs_full %<>% setkeyv(c("pph", "workers", "income"))

CTpop <- data.table(expand.grid(year = sort(unique(forecast$year)),                                # Create all combinations of year, pph, worker & income
                                pph = 1:7,
                                workers = 0:4,
                                income = unique(hhs_full$income))) %>%
  .[pph>=workers] %>%                                                                              # Remove nonsense combinations
  setkeyv(c("pph", "workers", "income"))

CTpop %<>% .[hhs_full, share:=i.share, on=key(.)] %>% setkeyv(c("year", "pph")) %>%                
  .[CTpph, household_count:=share * i.household_count, on=key(.)]                                  # Apply baseyear worker & income shares by pph

CTpop[, income := factor(income, levels = income.labels)]
CTpop[, income_min := income.bins[income]][, income_max := income.bins[as.integer(income)+1]-1]

# QC <- CTpop[, lapply(.SD, sum), by=.(year, pph), .SDcols=c("share","household_count")]           # Verify

# 3. Format for baseyear and export ---------------------------------

# # HHs
CTpop <- CTpop[year > base_year]
CTpop[, `:=`(persons_min = pph, persons_max = ifelse(pph < 7, pph, -1),
              workers_min = workers, workers_max = ifelse(workers < 4, workers, -1),
             total_number_of_households = round(household_count))]
CTpop[, `:=`(income = NULL, workers = NULL, pph = NULL, household_count = NULL, share = NULL)]

# save as csv file
fwrite(CTpop, file = output.file.name.hh)

if(create.emp.totals){
  mysql_connection <- mysql.connection(base.db)
  CTemp <- DBI::dbGetQuery(mysql_connection, DBI::SQL(paste0("select * from ", emp.ct.table))) %>% setDT()
  if("city_id" %in% colnames(CTemp)) CTemp[, city_id := NULL]
  DBI::dbDisconnect(mysql_connection)
  CTemp <- CTemp[year > base_year]
  if(scale.emp.controls) { # assumes forecast was read from a file and contains a column job_count
    setnames(CTemp, "total_number_of_jobs", "number_of_jobs_prev")
    CTemp[, tot_jobs := sum(number_of_jobs_prev), by = .(year)]
    CTemp[, share := number_of_jobs_prev/tot_jobs]
    CTemp[forecast, forecast_total := i.job_count, on = .(year)]
    CTemp[, total_number_of_jobs := round(forecast_total * share)][, `:=`(number_of_jobs_prev = NULL, tot_jobs = NULL, share = NULL, forecast_total = NULL)]
  }
  fwrite(CTemp, file = output.file.name.emp)
}

if(!is.null(output.ct.db)) {
  mysql_connection <- mysql.connection(output.ct.db)
  dbWriteTable(mysql_connection, "annual_household_control_totals_region", CTpop, overwrite = overwrite.existing.in.db, row.names = FALSE)
  if(create.emp.totals)
    dbWriteTable(mysql_connection, "annual_employment_control_totals_region", CTemp, overwrite = overwrite.existing.in.db, row.names = FALSE)
  DBI::dbDisconnect(mysql_connection)
}

