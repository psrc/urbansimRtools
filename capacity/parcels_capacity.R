##################################################
# Script for computing capacity for each parcel. 
# It uses a set of proposals from an unlimited run.
# Hana Sevcikova, PSRC, 2018-10-01
##################################################

library(data.table)
library(raster)

### Users settings
##################
# Where is this script
setwd('~/R/urbansimRtools/capacity') 

# save in csv file
save <- TRUE

# sampling share of residential projects on mix-use parcels
res.ratio <- 75

# prefix of the output file name
file.prefix <- paste0("CapacityPcl_res", res.ratio, "-", Sys.Date())

# Where are csv tables with the full set of proposals and components 
# (ideally from an unlimited urbansim run)
#prop.path <- "J:/Projects/Parcel\ Data/Capacity/unlimited-run-142"
prop.path <- "/Volumes/DataTeam/Projects/Parcel\ Data/Capacity/unlimited-run-142"

# Where are csv lookup tables 
# (base year buildings & parcels, building_sqft_per_job,
# development_constraints, development_templates & components)
#lookup.path <- "J:/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"
lookup.path <- "/Volumes/DataTeam/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"

rng.seed <- 1 # make it reproducible
####### End users settings

set.seed(rng.seed) 

# read and merge datasets
bld14 <- fread(file.path(lookup.path, "buildings.csv"))
props <- fread(file.path(prop.path, "development_project_proposals.csv"))
props <- subset(props, status_id != 3) # do not include MPDs
comp <- fread(file.path(prop.path, "development_project_proposal_components.csv"))
comp <- subset(comp, proposal_id %in% props$proposal_id)
pcl <- fread(file.path(lookup.path, "parcels.csv"))
setkey(pcl, parcel_id)
constraints <- fread(file.path(lookup.path, "development_constraints.csv"))
constr <- constraints[, .(max_dens = max(maximum)), by = .(plan_type_id, constraint_type)]
templ <- fread(file.path(lookup.path, "development_templates.csv"))
props <- merge(props, templ[, .(template_id, density_type)], by="template_id")
templc <- fread(file.path(lookup.path, "development_template_components.csv"))
bsqft.per.job <- fread(file.path(lookup.path, "building_sqft_per_job.csv"))

# impute missing sqft_per_unit and compute building_sqft in the base year buildings
bld14[residential_units > 0 & building_type_id == 19 & sqft_per_unit == 0, sqft_per_unit := 1000]
bld14[residential_units > 0 & building_type_id != 19 & sqft_per_unit == 0, sqft_per_unit := 500]
bld14[ , building_sqft := residential_units * sqft_per_unit]

# extract only columns that are needed
bld <- bld14[, .(building_id, parcel_id, residential_units, non_residential_sqft, 
                 building_sqft, job_capacity)]

# adjust building_sqft if smaller than non_residential_sqft
bld[non_residential_sqft > 0 & building_sqft < non_residential_sqft, building_sqft := non_residential_sqft]

# compute base year units stock for each parcel
pclstock <- bld[, .(pcl_resunits = sum(residential_units), pcl_nonres_sqft=sum(non_residential_sqft),
                    pcl_bldsqft = sum(building_sqft),
                    pcl_job_capacity = sum(job_capacity)), by=parcel_id]
setkey(pclstock, parcel_id)

# merge proposals with units stock and add zone_id
prop <- merge(props, pclstock, by = "parcel_id", all.x = TRUE)
prop <- merge(prop, pcl[,.(parcel_id, zone_id)], by = "parcel_id")

# disaggregate proposals into components & add info from the template table 
propc <- merge(prop, comp[,.(building_type_id, component_id, 
                             expected_sales_price_per_sqft, proposal_id)], by = "proposal_id")
propc <- merge(propc, templc[, .(template_id, component_id, building_sqft_per_unit, percent_building_sqft)], 
               by = c("template_id", "component_id"))
# merge with building_sqft_per_job
propc <- merge(propc, bsqft.per.job, by=c("building_type_id", "zone_id"), all.x = TRUE)

# compute proposed DU & sqft for res and nonres, respectively
propc[, proposed_units_new := ifelse(density_type == "far", 
                                     pmax(1, units_proposed_orig/building_sqft_per_unit),
                                     units_proposed_orig)*percent_building_sqft/100.]
# compute building_sqft
propc[, building_sqft := ifelse(density_type == "units_per_acre", 
                                units_proposed_orig * building_sqft_per_unit,
                                units_proposed_orig)*percent_building_sqft/100.]

# distinguish res and non-res proposals
propc[, has_non_res := !all(building_type_id %in% c(19,4,12)), by = proposal_id]
propc[, has_res := any(building_type_id %in% c(19,4,12)), by = proposal_id]

# remove smaller proposals and those with status_id 44
propc <- subset(propc, is.na(pcl_bldsqft) | 
                  (units_proposed_orig > pcl_resunits & density_type == "units_per_acre") | 
                  (units_proposed_orig > pcl_nonres_sqft & density_type == "far"))
propc <- subset(propc, status_id != 44)

# split parcels by type (res, non-res, mix-use)
pcl.type <- propc[,.(is_res = sum(has_non_res) == 0, is_non_res = sum(has_res) == 0,
                     is_mix_use = sum(has_res) > 0 & sum(has_non_res) > 0),
                  by = parcel_id]

# sum units by proposals separately for res, non-res, mix-use-res and mix-use-nonres
res_units <- propc[parcel_id %in% pcl.type[is_res == TRUE, parcel_id], 
                   .(residential_units = sum(proposed_units_new),
                     building_sqft = sum(building_sqft)), 
                   by = .(parcel_id, proposal_id)]
non_res <- propc[parcel_id %in% pcl.type[is_non_res == TRUE, parcel_id], 
                 .(non_residential_sqft = sum(proposed_units_new),
                   job_capacity = sum(pmax(1, round(proposed_units_new / building_sqft_per_job))),
                   building_sqft = sum(building_sqft)), 
                 by = .(parcel_id, proposal_id)]

res_units_mix <- propc[parcel_id %in% pcl.type[is_mix_use == TRUE, parcel_id] & building_type_id %in% c(19,4,12),
                       .(residential_units = sum(proposed_units_new),
                         building_sqft = sum(building_sqft), 
                         has_both_comp = sum(has_non_res, has_res) > 1),
                       by = .(parcel_id, proposal_id)]

non_res_mix <- propc[parcel_id %in% pcl.type[is_mix_use == TRUE, parcel_id] & ! building_type_id %in% c(19,4,12),
                     .(non_residential_sqft = sum(proposed_units_new),
                       building_sqft = sum(building_sqft),
                       job_capacity = sum(pmax(1, round(proposed_units_new / building_sqft_per_job))),
                       has_both_comp = sum(has_non_res, has_res) > 1),
                     by = .(parcel_id, proposal_id)]

# remove proposals that yield less units than there is on the ground (but not real mixed proposals)
# merge with parcels
non_res <- merge(non_res, pclstock[, .(parcel_id, pcl_job_capacity, pcl_nonres_sqft)], all.x = TRUE, by = "parcel_id")
non_res <- non_res[is.na(pcl_job_capacity) | pcl_job_capacity < job_capacity,]
non_res <- non_res[is.na(pcl_nonres_sqft) | pcl_nonres_sqft < non_residential_sqft,]
res_units <- merge(res_units, pclstock[, .(parcel_id, pcl_resunits)], all.x = TRUE, by = "parcel_id")
res_units <- res_units[is.na(pcl_resunits) | pcl_resunits < residential_units,]
res_units_mix <- merge(res_units_mix, pclstock[, .(parcel_id, pcl_resunits)], all.x = TRUE, by = "parcel_id")
res_units_mix <- res_units_mix[has_both_comp == TRUE | is.na(pcl_resunits) | pcl_resunits < residential_units,]
non_res_mix <- merge(non_res_mix, pclstock[, .(parcel_id, pcl_nonres_sqft, pcl_job_capacity)], all.x = TRUE, by = "parcel_id")
non_res_mix <- non_res_mix[has_both_comp == TRUE | is.na(pcl_nonres_sqft) | pcl_nonres_sqft < non_residential_sqft,]
non_res_mix <- non_res_mix[has_both_comp == TRUE | is.na(pcl_job_capacity) | pcl_job_capacity < job_capacity,]

# select max proposal per parcel
res_units_maxt <- res_units[, .SD[which.max(residential_units)], by = parcel_id]
res_units_max <- res_units_maxt[ , .(parcel_id, residential_units_prop = residential_units,
                                      building_sqft_prop = building_sqft, 
                                      non_residential_sqft_prop = 0, job_capacity_prop = 0)]

non_res_maxt <- non_res[, .SD[which.max(non_residential_sqft)], by = parcel_id]
non_res_max <- non_res_maxt[, .(parcel_id, non_residential_sqft_prop = non_residential_sqft,
                                 job_capacity_prop = job_capacity, building_sqft_prop = building_sqft,
                                 residential_units_prop = 0)]

res_units_mix_max <- res_units_mix[, .SD[which.max(residential_units)], by = parcel_id]
non_res_mix_max <- non_res_mix[, .SD[which.max(non_residential_sqft)], by = parcel_id]

# combine MU max proposals into one dataset
comb_mix_max <- merge(res_units_mix_max[, .(parcel_id, proposal_id)], 
                      non_res_mix_max[, .(parcel_id, proposal_id)], 
                      all = TRUE, by = "parcel_id")
# determine which proposal to take for each parcel; sample if not unique
comb_mix_max[proposal_id.x == proposal_id.y | is.na(proposal_id.y), proposal_id := proposal_id.x]
comb_mix_max[is.na(proposal_id.x), proposal_id := proposal_id.y]  
sampled.props <- apply(comb_mix_max[is.na(proposal_id), .(proposal_id.x, proposal_id.y)], 1, 
                       function(x) sample(x, 1, prob = c(res.ratio, 100 - res.ratio)))
comb_mix_max[is.na(proposal_id), proposal_id := sampled.props]
comb_mix_max[, `:=`(proposal_id.x = NULL, proposal_id.y = NULL)]

# fill sampled proposals with data
comb_mix_max[res_units_mix, `:=`(residential_units_prop = i.residential_units, building_sqft_prop = i.building_sqft), 
			    				  on = c("parcel_id", "proposal_id")]
comb_mix_max[non_res_mix, `:=`(non_residential_sqft_prop = i.non_residential_sqft, 
			  	                      building_sqft.y = i.building_sqft, 
                                job_capacity_prop = i.job_capacity), 
             on = c("parcel_id", "proposal_id")]

# parcels with no res component
comb_mix_max[is.na(building_sqft_prop), building_sqft_prop := 0] 
comb_mix_max[is.na(residential_units_prop), residential_units_prop := 0]
# parcels with no non-res component
comb_mix_max[is.na(building_sqft.y), building_sqft.y := 0] 
comb_mix_max[is.na(non_residential_sqft_prop), non_residential_sqft_prop := 0]
comb_mix_max[is.na(job_capacity_prop), job_capacity_prop := 0]
# add components and cleanup
comb_mix_max[, building_sqft_prop := building_sqft_prop + building_sqft.y]
comb_mix_max[, building_sqft.y := NULL]
comb_mix_max[, proposal_id := NULL]

# combine all three parts together (res, non-res & MU)
comb_max <- rbind(res_units_max, non_res_max, comb_mix_max)
setkey(comb_max, parcel_id)

# combine proposals and existing stock into one dataset
all.pcls <- merge(comb_max, pclstock, all = TRUE)

# compute quantities of interest
all.pcls[, `:=`(DUbase = ifelse(is.na(pcl_resunits), 0, pcl_resunits),
                NRSQFbase = ifelse(is.na(pcl_nonres_sqft), 0, pcl_nonres_sqft),
                JOBSPbase = ifelse(is.na(pcl_job_capacity), 0, pcl_job_capacity),
                BLSQFbase = ifelse(is.na(pcl_bldsqft), 0, pcl_bldsqft))]
all.pcls[, `:=`(DUcapacity = ifelse(is.na(residential_units_prop), DUbase,
                                     residential_units_prop),
                NRSQFcapacity = ifelse(is.na(non_residential_sqft_prop), NRSQFbase,
                                                        non_residential_sqft_prop),
                JOBSPcapacity = ifelse(is.na(job_capacity_prop), JOBSPbase,
                                           job_capacity_prop),
                BLSQFcapacity = ifelse(is.na(building_sqft_prop), BLSQFbase,
                                       building_sqft_prop)
                )]
respcl <- all.pcls[, .(parcel_id, DUbase, DUcapacity, NRSQFbase, NRSQFcapacity, 
                       JOBSPbase, JOBSPcapacity, BLSQFbase, BLSQFcapacity)]

# output results
if(save)
  fwrite(respcl, file = paste0(file.prefix, ".csv"))



