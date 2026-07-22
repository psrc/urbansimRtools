library(data.table)
library(ggplot2)
#options(error=quote(dump.frames("last.dump", TRUE)))
#load("last.dump.rda"); debugger()

# Settings
#==========
# were are parcels, buildings and various Xwalk tables, exported from the base year DB
data.dir <- "../data/BY2023"
rds.data.dir <- "~/psrc/R/shinyserver/baseyear2023explorer/data" # directory with big binary datasets

# where are the constraints file and the flu file located
#constr.dir <- "J:\Staff\Christy\usim-baseyear\dev_constraints"
#flu.dir <- "J:\Staff\Christy\usim-baseyear\flu"
constr.dir <- "~/psrc/urbansim-baseyear-prep/future_land_use/dev_constraints"
flu.dir <- file.path(constr.dir, "../flu")
    
# when were the flu file and constraints file created
flu.date <- c("2023-01-10", "2026-07-22")
#flu.date <- c("2026-06-01", "2026-07-22")

flu.names <- c("old", "new")
lc.factor <- c(1, 1) # if LC is defined as percentage (then use 1/100) or proportion (then use 1)

# factors that will constrain development, 
# i.e. parcel is developable if capacity > developable.factor * current_built
developable.factors <- c(1,2,3)

# which residential ratios should be used for mix-use
res.ratios <- c(30, 40, 50, 60, 70)

# should the parcel file be updated with new plan_type_id
update.plantype <- c(FALSE, TRUE)
#update.plantype <- c(TRUE, TRUE)

# should parcel data be stored
store.pcl.data <- FALSE
include.ct <- TRUE

source("capacity_functions.R")

# load cities and change names, so that names are unique (either adding RG name or county name)
counties <- data.table(name = c("King", "Kitsap", "Pierce", "Snohomish"),
           county_id = c(33, 35, 53, 61))
cities <- fread(file.path(data.dir, "cities.csv"))
cities[, N := .N, by = "city_name"]
cities[, Ncnty := .N, by = c("city_name", "county_id")]
# add RG name
cities[N > 1 & Ncnty > 1  & rg_proposed != "CitiesTowns", city_name := paste(city_name, rg_proposed)]
cities[, N2 := .N, by = "city_name"]
cities[, Ncnty2 := .N, by = c("city_name", "county_id")]
# add county name
cities[counties, county_name := i.name, on = "county_id"]
cities[N2 > 1 & Ncnty2 == 1, city_name := paste0(city_name, " (", county_name, ")")]

controls <- fread(file.path(data.dir, "controls.csv"))
controls[, N := .N, by = "control_name"]
# add county name
controls[counties, county_name := i.name, on = "county_id"]
controls[N > 1, control_name := paste0(control_name, " (", county_name, ")")]

geographies <- list(growth_centers = list(xwalk = fread(file.path(data.dir, "growth_centers.csv")),
                                          id_name = "growth_center_id", name_col = "name"),
                    counties = list(xwalk = counties, id_name = "county_id", name_col = "name"),
                    large_areas = list(xwalk = fread(file.path(data.dir, "large_areas.csv")),
                                       id_name = "large_area_id", name_col = "large_area_name"),
                    cities = list(xwalk = cities, id_name = "city_id", name_col = "city_name"),
                    controls = list(xwalk = controls, id_name = "control_id", name_col = "control_name")
                    )
# rename all name columns to "name"
for(geo in names(geographies))
  setnames(geographies[[geo]]$xwalk, geographies[[geo]]$name_col, "name")


# Load inputs common to both FLUs
#==============
# Load some of the 2023 datasets exported from the DB
pcls <- readRDS(file.path(rds.data.dir, "parcels.rds"))
#pcls <- fread(file.path(data.dir, "parcels.csv"))
setkey(pcls, "parcel_id")
# correct one city
pcls[city_id == 107, city_id := 109]


job_sqft <- fread(file.path(data.dir, "building_sqft_per_job.csv"))
bts <- fread(file.path(data.dir, "building_types.csv"))

# load 2023 buildings
bldgs <- readRDS(file.path(rds.data.dir, "buildings.rds"))
#bldgs <- fread("~/psrc/urbansim-baseyear-prep/imputation/data2023/buildings_imputed_phase3_lodes_20240226.csv")

# assemble sqft per job
job_sqft[bts, generic_land_use_type_id := i.generic_building_type_id, on = "building_type_id"]
# take the mean over sectors by each zone and GLU type
job_sqft_mean <- job_sqft[, .(building_sqft_per_job = mean(building_sqft_per_job)), by = .(zone_id, generic_land_use_type_id)]
# impute sqft per job for mix-use by taking the average over sectors and three GLU types by each zone
jsf_nmu <- job_sqft[generic_land_use_type_id %in% c(3,4,5), .(building_sqft_per_job = mean(building_sqft_per_job)), by = .(zone_id)]
job_sqft_mean <- rbind(job_sqft_mean, jsf_nmu[, generic_land_use_type_id := 6])

# get current built
pclbld <- bldgs[, .(residential_units = sum(residential_units), non_residential_sqft = sum(non_residential_sqft),
                    building_sqft = sum(residential_units * sqft_per_unit + non_residential_sqft)), by = .(parcel_id)]

# preserve the original plan type
pcls[, plan_type_id_orig := plan_type_id]

# iterate over multiple FLUs
allres <- allpcls <- NULL
for(fluidx in c(1, 2)){

  # load constraints file
  constr <- fread(file.path(constr.dir, paste0("devconstr_final_", flu.date[fluidx], ".csv")))

  # load the FLU file by plan_type_id and constraints by LC
  flu <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date[fluidx], ".csv")))

  # # quick temporary fixes
  # if(fluidx == 2){
  #   # Everett_HI
  #   constr[plan_type_id == 417 & constraint_type == "far", `:=`(maximum = 2.424242, maxht = 100)]
  #   flu[plan_type_id == 417, `:=`(MaxFAR_Indust = 2.424242, MaxHt_Indust = 100)]
  # }
  if(update.plantype[fluidx]){
    # load parcels with updated plan_type_id
    pcls_upd <- fread(file.path(constr.dir, paste0("prcls_ptid_final_", flu.date[fluidx], ".csv")))
    pcls[pcls_upd, plan_type_id := i.plan_type_id, on = "parcel_id"]
  }

  # add generic land use type to the FLU dataset
  flulc <- rbind(flu[Res_Use == 1 | Res_Use == "Y", .(plan_type_id, coverage = LC_Res, generic_land_use_type_id = 1)],
               flu[Res_Use == 1 | Res_Use == "Y", .(plan_type_id, coverage = LC_Res, generic_land_use_type_id = 2)],
               flu[Office_Use == 1 | Office_Use == "Y", .(plan_type_id, coverage = LC_Office, generic_land_use_type_id = 3)],
               flu[Comm_Use == 1 | Comm_Use == "Y", .(plan_type_id, coverage = LC_Comm, generic_land_use_type_id = 4)],
               flu[Indust_Use == 1 | Indust_Use == "Y", .(plan_type_id, coverage = LC_Indust, generic_land_use_type_id = 5)],
               flu[Mixed_Use == 1 | Mixed_Use == "Y", .(plan_type_id, coverage = LC_Mixed, generic_land_use_type_id = 6)]
  )
  # add the FLU info, including coverage, to the development constraints dataset
  constr[flulc, coverage := i.coverage * lc.factor[fluidx], on = .(plan_type_id, generic_land_use_type_id)]
  constr[is.na(coverage), coverage := 1] 

  # compute capacity for each parcel
  pclwu <- compute.parcel.capacity(pcls, constr, job_sqft_mean, include.coverage = TRUE)

  # join current built with parcels' capacity
  pclwu[pclbld, `:=`(residential_units_built = i.residential_units, 
                   non_residential_sqft_built = i.non_residential_sqft,
                   building_sqft_built = i.building_sqft), 
          on = "parcel_id"]
  pclwu[is.na(residential_units_built), residential_units_built := 0]
  pclwu[is.na(non_residential_sqft_built), non_residential_sqft_built := 0]
  pclwu[is.na(building_sqft_built), building_sqft_built := 0]

  allpcls[[fluidx]] <- aggregate.capacity(pclwu, by = "parcel_id", 
                                  developable.factor = 1, res.ratios = 50)[
                                    type %in% c("residential-units", "non-residential-sqft")]
  allpcls[[fluidx]] <- dcast(allpcls[[fluidx]][, .(parcel_id, type, total_capacity)], 
                             parcel_id ~ type, value.var = "total_capacity")
  setkey(allpcls[[fluidx]], "parcel_id")
  allpcls[[fluidx]][pcls, plan_type_id := i.plan_type_id]
  
  
  # aggregate capacity for desired geography, using different residential ratios
  # and developable factors
  for(geo in names(geographies)){
    for(devfac in developable.factors){
      res <- aggregate.capacity(pclwu, by = geographies[[geo]]$id_name, 
                                developable.factor = devfac,
                                res.ratios = res.ratios)
      res[geographies[[geo]]$xwalk, name := i.name, on = geographies[[geo]]$id_name]
      setnames(res, geographies[[geo]]$id_name, "id")
      allres <- rbind(allres, res[, `:=`(developfac = devfac, geography = geo, 
                                         flu = flu.names[fluidx])])
    }
  }
  if(update.plantype[fluidx]){
    # reset back the original plan type
    pcls[, plan_type_id := plan_type_id_orig]
  }
}

if(store.pcl.data){
  pcl.to.store <- merge(allpcls[[1]], allpcls[[2]])
  colnames(pcl.to.store) <- gsub(".x", paste0("_", flu.names[1]), colnames(pcl.to.store), fixed = TRUE)
  colnames(pcl.to.store) <- gsub(".y", paste0("_", flu.names[2]), colnames(pcl.to.store), fixed = TRUE)
  pcl.to.store <- merge(pcls[, .(parcel_id, county_id, large_area_id, city_id, faz_id, zone_id, 
                                 growth_center_id, control_id, parcel_sqft)],
                        pcl.to.store)
  fwrite(pcl.to.store, file = paste0("parcel_capacity_data_flu-", flu.date[2], ".csv"))
}


# choose one developable factor and subset results to it for plotting purposes
developable.factor <- developable.factors[1]
res <- allres[developfac == developable.factor]

if(include.ct){
  ct <- fread(file.path(data.dir, "annual_household_control_totals_07202026.csv"))
  ct <- ct[year == 2050, .(hh = sum(total_number_of_households)), by = "subreg_id"]
  # correct JBLM control
  ct[subreg_id == 405, subreg_id := 403]
  # aggregate to control_id
  ct[, control_id := subreg_id][subreg_id > 1000, control_id := control_id - 1000]
  ct <- ct[, .(hh = sum(hh)), by = "control_id"]
  ct[controls, `:=`(name = i.name, county_id = i.county_id, county_name = i.county_name),
     on = "control_id"]
  ctcnty <- ct[, .(hh = sum(hh)), by = c("county_id", "county_name")]
  res <- rbind(res, ct[, .(id = control_id, type = "residential-units", 
                           total_capacity = hh, res_ratio = 50, name, 
                           geography = "controls", flu = "target")], fill = TRUE)
  res <- rbind(res, ctcnty[, .(id = county_id, type = "residential-units", 
                               total_capacity = hh,
                               res_ratio = 50, name = county_name,
                               geography = "counties", flu = "target")], fill = TRUE)
}


# plot results

res2 <- melt(res, id.vars = c("flu", "id", "name", "geography", "type", "res_ratio"), 
             variable.name = "indicator")


reseb <- res2[id > 0 &  indicator %in% c("remaining_capacity", "total_capacity")][type == "non-residential-jobs" & indicator == "total_capacity", value := NA]
reseb <- dcast(reseb, flu + geography + id + name + type + indicator ~ res_ratio, value.var = "value")

allg <- NULL
for(geo in names(geographies)){
  g <- ggplot(reseb[geography == geo & indicator == "total_capacity" & type != "non-residential-jobs" & flu != "target"], 
               aes(x = name, group = flu, color = flu)) + 
    geom_errorbar(aes(ymin = `40`, ymax = `60`), position = position_dodge(width=0.3), na.rm = TRUE)  + 
    geom_point(aes(y = `50`), na.rm = TRUE, position = position_dodge(width=0.3)) +
    facet_grid(type ~ . , scales = "free") + xlab("") + ylab("") +
    guides(x =  guide_axis(angle = 90))
  if(include.ct && nrow(reseb[geography == geo & flu == "target"]) > 0)
    g <- g + geom_point(data = reseb[geography == geo & flu == "target"], 
                       aes(y = `50`, fill = "HH target"), shape = 4, color = "black") +
      scale_fill_manual(name = "", values = c("HH target" = "yellow"))
  if(include.ct && geo == "controls"){
    # create a plot with only id where target is larger than capacity
    dat <- reseb[geography == geo & indicator == "total_capacity" & type == "residential-units"]
    dat[dat[flu == "target"], target := `i.50`, on = c("id")]
    show.ids <- unique(dat[flu == "new" & `50` - target < 0, id])
    dat <- dat[id %in% show.ids]
    gt <- ggplot(dat[flu != "target"], 
                aes(x = name, group = flu, color = flu)) + 
      geom_errorbar(aes(ymin = `40`, ymax = `60`), position = position_dodge(width=0.3), na.rm = TRUE)  + 
      geom_point(aes(y = `50`), na.rm = TRUE, position = position_dodge(width=0.3)) +
      facet_grid(type ~ . , scales = "free") + xlab("") + ylab("") +
      guides(x =  guide_axis(angle = 90)) +
      geom_point(data = dat[flu == "target"], aes(y = `50`, fill = "HH target"), shape = 4, color = "black") +
      scale_fill_manual(name = "", values = c("HH target" = "yellow"))
  }
  allg[[geo]] <- g
}

print(allg[["counties"]])
print(allg[["cities"]])
print(allg[["large_areas"]])
print(allg[["growth_centers"]])
print(allg[["controls"]])


pdf(file = paste0("capacity_comparisons_various_gegraphies_flu-", flu.date[1], "_", flu.date[2], ".pdf"), width = 14, height = 8)
#pdf(file = paste0("capacity_no_lc_comparisons_various_gegraphies_flu-", flu.date[1], "_", flu.date[2], ".pdf"), width = 14, height = 8)

print(allg[["counties"]] + ggtitle("Counties"))
print(allg[["large_areas"]]+ ggtitle("Large Areas"))
print(allg[["growth_centers"]] + ggtitle("Growth Centers"))
print(allg[["cities"]]  + ggtitle("Cities"))
print(allg[["controls"]]  + ggtitle("Controls"))
if(include.ct){
  print(gt + ggtitle("Controls lacking capacity"))
}
dev.off()

stop("End of processing")

# below is exploration code
############################
if(!exists("pcl.to.store")) {
  pcl.to.store <- merge(allpcls[[1]], allpcls[[2]])
  colnames(pcl.to.store) <- gsub(".x", paste0("_", flu.names[1]), colnames(pcl.to.store), fixed = TRUE)
  colnames(pcl.to.store) <- gsub(".y", paste0("_", flu.names[2]), colnames(pcl.to.store), fixed = TRUE)
  pcl.to.store <- merge(pcls[, .(parcel_id, county_id, large_area_id, city_id, faz_id, zone_id, 
                                 growth_center_id, control_id, parcel_sqft)],
                        pcl.to.store)
}

spcls <- pcl.to.store[city_id == 38]
spcls <- pcl.to.store[growth_center_id == 514]
spcls <- pcl.to.store[control_id == 64]
spcls[, .(DUnew = sum(`residential-units_new`, na.rm = TRUE), 
          NRSFnew = sum(`non-residential-sqft_new`, na.rm = TRUE)
          ), by = "plan_type_id_new"][order(-NRSFnew)]#[order(-DUnew)]
spcls[, .(DUold = sum(`residential-units_old`, na.rm = TRUE), 
          NRSFold = sum(`non-residential-sqft_new`, na.rm = TRUE)
          ), by = "plan_type_id_old"][order(-NRSFold)]#[order(-DUold)]
spcls[, .(DUnew = sum(`residential-units_new`, na.rm = TRUE), DUold = sum(`residential-units_old`, na.rm = TRUE)
), by = "plan_type_id_new"][order((DUnew - DUold))]

spcls[, .N, by = "plan_type_id_new"][order(-N)]
spcls[, .N, by = "plan_type_id_old"][order(-N)]
spcls[plan_type_id_new == 2122, .N, by = "plan_type_id_old"][order(-N)]
  
constr.old <- fread(file.path(constr.dir, paste0("devconstr_final_", flu.date[1], ".csv")))
constr.new <- fread(file.path(constr.dir, paste0("devconstr_final_", flu.date[2], ".csv")))
flu.old <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date[1], ".csv")))
flu.new <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date[2], ".csv")))
flu.new[plan_type_id == 859]
flu.old[plan_type_id %in% c(1311)]

# check where there is only Mixed_Use and nothing else
paste(sort(flu.new[Mixed_Use == 1 & Res_Use == 0 & Comm_Use == 0 & Office_Use == 0 & Indust_Use == 0, juris_zn]), collapse = ", ")