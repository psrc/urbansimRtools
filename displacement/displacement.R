library(data.table)
setwd("~/psrc/R/urbansimRtools/displacement")

# load data
run <- "run62" # just to get up to date base year data
#run <- "BY14run89"
BY <- 2018
pcl <- fread(file.path(run, BY, "parcels.csv"))
setkey(pcl, parcel_id)
bld <- fread(file.path(run, BY, "buildings.csv"))
setkey(bld, building_id)
hh <- fread(file.path(run, BY, "households.csv"))[building_id > 0]
setkey(hh, building_id)
hh[bld, parcel_id := i.parcel_id]

# correct wrong year_built
bld[year_built > BY + 2, year_built := 0]

# load development proposals
mpd <- fread(file.path(run, BY, "development_project_proposals.csv"))
dprop <- fread(file.path(run, BY + 1, "development_project_proposals.csv"))
setkey(dprop, proposal_id)
cdprop <- fread(file.path(run, BY + 1, "development_project_proposal_components.csv"))
setkey(cdprop, proposal_id, parcel_id)

# remove projects on parcels where there are MPDs
devmpd <- mpd[start_year <= BY + 3]
dprop <- dprop[!parcel_id %in% devmpd[,parcel_id]]
if(BY == 2018){
# temporary fix related to an issue of creating new development projects in run 62 
dprop[bld[, .(is_redevelopment = 0, .N), by = parcel_id], has_bld := i.N > 0, on = c("parcel_id", "is_redevelopment")][is.na(has_bld), has_bld := FALSE]
dprop <- dprop[is_redevelopment == 1 | !has_bld]
}
mpd <- mpd[start_year > BY + 3]

# join components with proposals
cdprop <- cdprop[proposal_id %in% dprop[, proposal_id]]
cdprop[dprop, parcel_id := i.parcel_id]

# select occupied residential buildings that are older than 10 years
age.limit <- 8
rbld <- bld[residential_units > 0 & building_id %in% hh[, building_id] & year_built < (BY + 2 - age.limit)]

# select proposals that are on parcels with selected residential buildings
redprop <- dprop[parcel_id %in% rbld[, parcel_id]]
setkey(redprop, proposal_id, parcel_id)

# select buildings that are on parcels that have at least one proposal
redbld <- rbld[parcel_id %in% redprop[, parcel_id]]
setkey(redbld, building_id, parcel_id)

# mark residential and non-res proposals
proptype <- cdprop[proposal_id %in% redprop[, proposal_id]][, 
                            .(is_res = any(building_type_id %in% c(19, 4, 12)), 
                                is_nonres = any(!building_type_id %in% c(19, 4, 12))), by = proposal_id]
setkey(proptype, proposal_id)
redprop <- merge(redprop, proptype, by = "proposal_id")

# for non-res proposals, estimate the number of units as if the project would be MF residential
redprop[, units_est := units_proposed]
redprop[is_nonres == TRUE, units_est := round(units_proposed/1000)]

# select only the largest proposals
redprop[redprop[, .I[which.max(units_est)], by = parcel_id]$V1, is_max := TRUE]
redpropm <- redprop[!is.na(is_max) & is_max == TRUE]

# sum existing DUs and improvement values for each of the affected parcels
redpcl <- redbld[, .(DU = sum(residential_units), IV = sum(improvement_value)), by = parcel_id]
setkey(redpcl, parcel_id)

# join with existing units
redpropm[redpcl, existing_units := i.DU, on = "parcel_id"]

# select proposals where the number of proposed units is 2 times of the existing units, 
# or if MPD, proposed should be simply larger than existing 
comp.factor <- 1
redpropm.build <- redpropm[units_est > comp.factor*existing_units | (units_est > existing_units & parcel_id %in% mpd[, parcel_id])]

# select households that live on the parcels with selected proposals
selhh <- hh[parcel_id %in% redpropm.build[, parcel_id]]

# output
res <- selhh[, .(HHatRisk = .N), by = parcel_id]
setkey(res, parcel_id)
res[pcl, `:=`(census_tract_id = i.census_tract_id)]
if("census_2010_block_id" %in% colnames(pcl))
    res[pcl, `:=`(census_2010_block_id = i.census_2010_block_id)]

hhtot <- hh[, .N, by = "parcel_id"]
hhtot[res, hh_at_risk := i.HHatRisk, on = "parcel_id"][is.na(hh_at_risk), hh_at_risk := 0]
hhtot[pcl, county_id := i.county_id, on = "parcel_id"]

if(BY == 2018){
    resall <- hhtot[pcl, .(parcel_id, county_id, census_2010_block_id = i.census_2010_block_id, 
                           census_tract_id = i.census_tract_id, hh_at_risk, hh_total = N), on = "parcel_id"][!is.na(hh_total)]
    #fwrite(resall, file = paste0("hh_at_displacement_risk-", Sys.Date(), ".csv"))
}

# summary
resout <- hhtot[, .(hh_at_risk = sum(hh_at_risk), hh_total = sum(N), 
                    percent = round(sum(hh_at_risk)/sum(N)*100, 1)), by = county_id]
merge(data.table(county_id = c(33, 35, 53, 61), county_name = c("King", "Kitsap", "Pierce", "Snohomish")),
                resout)
# total
hhtot[, .(hh_at_risk = sum(hh_at_risk), hh_total = sum(N), percent = round(sum(hh_at_risk)/sum(N)*100, 1))]
