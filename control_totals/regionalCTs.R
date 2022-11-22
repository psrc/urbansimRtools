#library(psrccensus)
library(dplyr)
library(srvyr)
library(magrittr)
library(data.table)
library(DBI)
library(odbc)
library(RMySQL)
library(readxl)

base_year <- 2018

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

# Data retrieval ----------------------------------------------------

## Get household size combinations from relevant PUMS
# ref_data <- get_psrc_pums(1, base_year, "h", c("NP","HINCP","NWRK","BINCOME"))                     # Reference data from regional PUMS
# ref_data %<>% mutate(pph=case_when(as.integer(NP)>7 ~7L, !is.na(NP) ~as.integer(NP)),
#                      workers=case_when(as.integer(NWRK)>3 ~4L, !is.na(NWRK) ~as.integer(NWRK)),
#                      income=case_when(grepl("\\b25,000\\b", as.character(BINCOME)) ~"Under $50,000",
#                                           !is.na(BINCOME) ~as.character(BINCOME))) %>%
#               mutate(across(c(pph, workers), factor))
# hhs <- psrc_pums_count(ref_data, group_vars="pph", incl_na=FALSE) %>%                              # Counts by household size
#   rename(year=DATA_YEAR) %>% setDT() %>% .[pph!="Total", .(year, pph, count, share)]
# hhs[, c("year", "pph"):=lapply(.SD, as.integer), .SDcols=c("year", "pph")]
# ref_data1 <- ungroup(ref_data) %>% filter(if_any(c(workers, income), ~ !is.na(.))) %>%
#   group_by(pph) %>% group_by(interact(workers, income), .add=TRUE)
# hhs_full2 <- psrc_pums_count(ref_data1, group_vars="keep_existing", incl_na=FALSE) %>%              # Counts by full breakdown of CT categories
#   .[income!="Total" & workers!="Total", .(year, pph, workers, income, count, share)] %>% .[, year:=base_year]
# levels(hhs_full2$income) <- c("Under $50,000", "$50,000-$74,999", "$75,000-$99,999", "$100,000 or more")
# hhs_full[, c("year", "pph", "workers"):=lapply(.SD, as.integer), .SDcols=c("year", "pph", "workers")]

## Get household size combinations from UrbanSim baseyear itself
prsn_sql <- "CASE WHEN h.persons < 7 THEN h.persons ELSE 7 END"
wrkr_sql <- "CASE WHEN workers < 4 THEN h.workers ELSE 4 END"
inc_sql <- paste("CASE WHEN h.income >= 100000 THEN '$100,000 or more'",
                 "WHEN h.income >= 75000 THEN '$75,000-$99,999'",
                 "WHEN h.income >= 50000 THEN '$50,000-$74,999'",
                 "ELSE 'under $50,000' END")

hhs_sql <- paste("SELECT", base_year, "AS year,", prsn_sql, "AS pph,", 
                 wrkr_sql, "AS workers,", inc_sql, "as income, count(*) AS count", 
                 "FROM", paste0(base_year,"_parcel_baseyear.households"), "AS h",
                 "GROUP BY", prsn_sql, ",",  wrkr_sql, ",", inc_sql)

mysql_connection <- RMySQL::dbConnect(RMySQL::MySQL(),
                                      dbname=paste0(base_year,"_parcel_baseyear"),
                                      username="psrcurbansim",
                                      password="psrc_urbansim",
                                      host="10.10.10.203",
                                      port=3306)

hhs_full <- DBI::dbGetQuery(mysql_connection, DBI::SQL(hhs_sql)) %>% setDT()
sums <- hhs_full[,.(hh_tot=sum(count)), by=.(pph)]
setkey(hhs_full, pph)
setkey(sums, pph)
hhs_full[sums, share:=count/hh_tot]
DBI::dbDisconnect(mysql_connection)

# Retrieve macro forecast from Elmer
# hh_4cast_sql <- paste("SELECT p.data_year AS year, p.population AS hh_pop, h.households AS household_count",
#                       "FROM Elmer.Macroeconomic.v_pop AS p JOIN Elmer.Macroeconomic.v_households AS h ON p.data_year=h.data_year",
#                       "WHERE p.group_name='InHouseholds';")
# 
# elmer_connection <- DBI::dbConnect(odbc::odbc(), 
#                                    driver = "ODBC Driver 17 for SQL Server",
#                                    server = "AWS-PROD-SQL\\Sockeye",
#                                    database = "Elmer",
#                                    trusted_connection = "yes",
#                                    port = 1433)
# 
# forecast <- DBI::dbGetQuery(elmer_connection, DBI::SQL(hh_4cast_sql))  %>% setDT()
# DBI::dbDisconnect(elmer_connection)

# Retrieve macro forecast from file - for those who can't connect to Elmer
xlf <- paste0(#"J:/Projects/UrbanSim/NEW_DIRECTORY/Inputs/",
  #"annual_control_totals/controls_from_2017_macro_forecast/",
  "~/psrc/control_totals/",
  "Working_Update_annual_hh_control_totals_2018_forecasts_2018_BY.xlsx")
forecast <- data.table(read_excel(xlf, sheet="2018 REF Forecast Data",                             # Load household population and households from Macro forecast  
                                  range="G28:AM30", col_names=as.character(2018:2050))) %>% 
  .[c(1,3),] %>% data.table::transpose(keep.names="year") %>% setnames(c("V1", "V2"), c("hh_pop", "household_count"))
forecast[, year := as.integer(year)]

# 1. Households by size ---------------------------------------------

CTpph <- data.table(expand.grid(year = sort(unique(forecast$year)), pph = 1:7))                    # create all combinations of year and pph

CTpph[hhs_full, household_count := i.count, on = .(year, pph)]                                     # merge base year hh counts with CTpph 
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

# QC <- CTpop[, lapply(.SD, sum), by=.(year, pph), .SDcols=c("share","household_count")]           # Verify

# 3. Format for baseyear and export ---------------------------------

# # HHs
# CTpop <- CTpop[year > base_year]
# CTpop[, `:=`(total_number_of_households = household_count, 
#              income_min = 0, income_max = -1, 
#              persons_min = pph, persons_max = ifelse(pph < 7, pph, -1),
#              workers_min = 0, workers_max = -1)]
# qr <- dbSendQuery(mydb, "select * from annual_household_control_totals")
# cttbl <- data.table(fetch(qr, n = -1))
# cttbl <- cttbl[year >= base_year]
# resCThh <- cttbl[!year %in% unique(CTpop$year)]
# resCThh <- rbind(resCThh, CTpop[, colnames(cttbl), with = FALSE])
# dbClearResult(qr)
# 
# # jobs                                                                                           # Haven't built employment in yet--uses different Macro file
# qr <- dbSendQuery(mydb, "select * from annual_employment_control_totals")
# cttbl <- data.table(fetch(qr, n = -1))
# cttbl <- cttbl[year >= base_year]
# resCTjobs <- cttbl[!year %in% unique(CTs$year)]
# resCTjobs <- rbind(resCTjobs, 
#                    CTs[, .(city_id, year, total_number_of_jobs = total_emp, home_based_status = -1, sector_id = -1)]
# )
# dbClearResult(qr)
# 
# # disconnect DB
# dbDisconnect(mydb)

# write outputs into working DB
# if(store.outputs) {
#   mydb <- mysql.connection(work.db)
#   dbWriteTable(mydb, "annual_household_control_totals", resCThh, overwrite = overwrite.existing, row.names = FALSE)
#   dbWriteTable(mydb, "annual_employment_control_totals", resCTjobs, overwrite = overwrite.existing, row.names = FALSE)
#   dbDisconnect(mydb)
# }
# 
# # Check results
# checkDT <- CTpop[year > base_year, .(total_hh = sum(household_count), total_hhpop = sum(pph * household_count)),
#                  by = .(city_id, year)]
# checkDT[CTs, `:=`(diff_hh = total_hh - i.total_hh, diff_hhpop = total_hhpop - i.total_hhpop),
#         on = c("city_id", "year")]
# 
# cat("\nMax differences in HH:", range(checkDT$diff_hh))
# cat("\nMax differences in HHpop:", range(checkDT$diff_hhpop))
# cat("\n")
