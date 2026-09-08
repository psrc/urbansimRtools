library(data.table)
library(ggplot2)
#options(error=quote(dump.frames("last.dump", TRUE)))
#load("last.dump.rda"); debugger()

# Settings
#==========
# were are parcels, buildings and various Xwalk tables, exported from the base year DB
#data.dir <- "../data/BY2023"
data.dir <- "../data/BY2023updgeo" # data with updated geographies

rds.data.dir <- "~/psrc/R/shinyserver/baseyear2023explorer/data" # directory with big binary datasets

# where are the constraints file and the flu file located
#constr.dir <- "J:\Staff\Christy\usim-baseyear\dev_constraints"
#flu.dir <- "J:\Staff\Christy\usim-baseyear\flu"
constr.dir <- "~/psrc/urbansim-baseyear-prep/future_land_use/dev_constraints"
flu.dir <- file.path(constr.dir, "../flu")
    
# when were the flu file and constraints file created
flu.date <- c("2023-01-10", "2026-07-28")
#flu.date <- c("2026-06-01", "2026-07-22")
flu.date <- c("2023-01-10", "2026-08-19")
flu.date <- c("2023-01-10", "2026-09-03")
#flu.date <- c("2026-08-19", "2026-09-03")

flu.names <- c("old", "new")
#flu.names <- c("07-28", "08-07")
#flu.names <- c("08-19", "09-03")

lc.factor <- c(1, 1) # if LC is defined as percentage (then use 1/100) or proportion (then use 1)

# factors that will constrain development, 
# i.e. parcel is developable if capacity > developable.factor * current_built
developable.factors <- 1
#developable.factors <- c(1,2,3)

# which residential ratios should be used for mix-use
res.ratios <- c(40, 50, 60)
#res.ratios <- c(30, 40, 50, 60, 70)

# should the parcel file be updated with new plan_type_id
# - depends on which plan_type_id is attached to the parcel file
#   (set it to FALSE, if the FLU's plan_type_id is the one attached to parcels)
#update.plantype <- c(FALSE, TRUE)
update.plantype <- c(TRUE, FALSE) # using parcel table that has been updated with new plan_type_id
update.plantype <- c(TRUE, TRUE)
#update.plantype <- c(TRUE, TRUE)
update.plantype.from.rds <- c(TRUE, FALSE) # should the update be made using data in baseyear explorer (should be TRUE for old FLU)

# should current built be considered
consider.current.built <- TRUE

# should parcel data be stored
store.pcl.data <- TRUE

store.pcl.data.for.housing.analysis <- TRUE

# include targets in the plots
include.ct <- TRUE

source("capacity_functions.R")

# counties names
counties <- data.table(name = c("King", "Kitsap", "Pierce", "Snohomish"),
           county_id = c(33, 35, 53, 61))

# load cities and change names, so that names are unique (either adding RG name or county name)
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

# load controls and make the names unique by adding county name if needed
controls <- fread(file.path(data.dir, "controls.csv"))
controls[, N := .N, by = "control_name"]
controls[counties, county_name := i.name, on = "county_id"]
controls[N > 1, control_name := paste0(control_name, " (", county_name, ")")]

# load regional geographies
rgs <- fread(file.path(data.dir, "control_rgs.csv"))

# set geographies for the various plots
geographies <- list(growth_centers = list(xwalk = fread(file.path(data.dir, "growth_centers.csv")),
                                          id_name = "growth_center_id", name_col = "name"),
                    counties = list(xwalk = counties, id_name = "county_id", name_col = "name"),
                    large_areas = list(xwalk = fread(file.path(data.dir, "large_areas.csv")),
                                       id_name = "large_area_id", name_col = "large_area_name"),
                    cities = list(xwalk = cities, id_name = "city_id", name_col = "city_name"),
                    controls = list(xwalk = controls, id_name = "control_id", name_col = "control_name"),
                    rgs = list(xwalk = rgs, id_name = "control_rgs_id", name_col = "control_rgs_name")
                    )
# rename all name columns to "name"
for(geo in names(geographies))
  setnames(geographies[[geo]]$xwalk, geographies[[geo]]$name_col, "name")


# Load inputs common to both FLUs
#==============
# Load some of the 2023 datasets exported from the DB
#pcls <- readRDS(file.path(rds.data.dir, "parcels.rds")) # 
pcls <- fread(file.path(data.dir, "parcels.csv"))
setkey(pcls, "parcel_id")
# correct one city
pcls[city_id == 107, city_id := 109]

# add rgs info to parcels
pcls[controls, control_rgs_id := i.control_rgs_id, on = "control_id"]

# load sqft/job and building types
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

if(any(update.plantype.from.rds))
  pclplantypes <- list(readRDS(file.path(rds.data.dir, "parcels.rds"))[, .(parcel_id, plan_type_id)],
                       NULL)


# preserve the original plan type associated with the parcel table
pcls[, plan_type_id_orig := plan_type_id]

# iterate over multiple FLUs
allres <- allpcls <- NULL
for(fluidx in c(1, 2)){

  # load constraints file
  constr <- fread(file.path(constr.dir, paste0("devconstr_final_", flu.date[fluidx], ".csv")))

  # load the FLU file by plan_type_id and constraints by LC
  flu <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date[fluidx], ".csv")))

  if(update.plantype[fluidx]){
    # load parcels with updated plan_type_id
    if(update.plantype.from.rds[fluidx]) {
      pcls_upd <- pclplantypes[[fluidx]]
    } else 
      pcls_upd <- fread(file.path(constr.dir, paste0("prcls_ptid_final_", flu.date[fluidx], ".csv")))
      if(! "parcel_id" %in% colnames(pcls_upd) && "PIN" %in% colnames(pcls_upd))
        setnames(pcls_upd, "PIN", "parcel_id")
    # assign the plan type to parcels
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
  if(consider.current.built){
    # update capacity so that it is not less than the current built
    pclwu[constraint_type != "far", 
          residential_units := pmax(residential_units_built, residential_units)]
    pclwu[constraint_type == "far", 
          building_sqft := pmax(non_residential_sqft_built, building_sqft)]
  }
  # the allpcls object below is created only for the purpose of storing 
  # capacity on parcel level
  allpcls[[fluidx]] <- aggregate.capacity(pclwu, by = "parcel_id", 
                                  developable.factor = 1, res.ratios = 50)[
                                    type %in% c("residential-units", "non-residential-sqft")]
  # convert into wide format
  allpcls[[fluidx]] <- dcast(allpcls[[fluidx]][, .(parcel_id, type, total_capacity)], 
                             parcel_id ~ type, value.var = "total_capacity")
  setkey(allpcls[[fluidx]], "parcel_id")
  
  # assign plan_type_id
  allpcls[[fluidx]][pcls, plan_type_id := i.plan_type_id]
  
  
  # aggregate capacity to desired geographies, using different residential ratios
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

if(store.pcl.data || store.pcl.data.for.housing.analysis){
  # store parcel's capacity
  pcl.to.store <- merge(allpcls[[1]], allpcls[[2]])
  colnames(pcl.to.store) <- gsub(".x", paste0("_", flu.names[1]), colnames(pcl.to.store), fixed = TRUE)
  colnames(pcl.to.store) <- gsub(".y", paste0("_", flu.names[2]), colnames(pcl.to.store), fixed = TRUE)
  pcl.to.store <- merge(pcls[, .(parcel_id, county_id, large_area_id, city_id, faz_id, zone_id, 
                                 growth_center_id, control_id, control_rgs_id, parcel_sqft)],
                        pcl.to.store)
  if(store.pcl.data)
    fwrite(pcl.to.store, file = paste0("parcel_capacity_data_flu-", flu.date[2], ".csv"))
  if(store.pcl.data.for.housing.analysis){
    pcl.to.store.hu <- copy(allpcls[[2]])
    setnames(pcl.to.store.hu, "residential-units", "DUzoned")
    pcl.to.store.hu[pclbld, DUbuilt := i.residential_units, on = "parcel_id"]
    pcl.to.store.hu[is.na(DUzoned), DUzoned := 0]
    pcl.to.store.hu[is.na(DUbuilt), DUbuilt := 0]
    pcl.to.store.hu[, DUdif := pmax(0, DUzoned - DUbuilt)]
    pcl.to.store.hu <- pcl.to.store.hu[DUbuilt + DUzoned > 0]
    pcl.to.store.hu[pcls, county_id := i.county_id, on = "parcel_id"]
    fwrite(pcl.to.store.hu[, .(parcel_id, plan_type_id, county_id, DUzoned = round(DUzoned, 3), 
                               DUbuilt, DUdif = round(DUdif,3))], 
           file = paste0("parcel_DUcapacity_flu-", flu.date[2], ".csv"))
  }
}


# choose one developable factor and subset results to it for plotting purposes
developable.factor <- developable.factors[1]
res <- allres[developfac == developable.factor]

if(include.ct){
  # load control totals for plotting targets
  ct <- fread(file.path(data.dir, "annual_household_control_totals.csv"))
  ct <- ct[year == 2050, .(hh = sum(total_number_of_households)), by = "subreg_id"]

  # correct JBLM control (needed in old CTs)
  #ct[subreg_id == 405, subreg_id := 403]
  
  # aggregate to control_id
  ct[, control_id := subreg_id][subreg_id > 1000, control_id := control_id - 1000]
  ct <- ct[, .(hh = sum(hh)), by = "control_id"]
  ct[controls, `:=`(name = i.name, county_id = i.county_id, county_name = i.county_name,
                    control_rgs_id = i.control_rgs_id),
     on = "control_id"]
  # aggregate to control_rgs and county
  ct[rgs, `:=`(rgs_name = i.name), on = "control_rgs_id"]
  ctcnty <- ct[, .(hh = sum(hh)), by = c("county_id", "county_name")]
  ctrgs <- ct[, .(hh = sum(hh)), by = c("control_rgs_id", "rgs_name")]
  
  # attach the three CT datasets to the main dataset
  res <- rbind(res, ct[, .(id = control_id, type = "residential-units", 
                           total_capacity = hh, res_ratio = 50, name, 
                           geography = "controls", flu = "target")], fill = TRUE)
  res <- rbind(res, ctcnty[, .(id = county_id, type = "residential-units", 
                               total_capacity = hh,
                               res_ratio = 50, name = county_name,
                               geography = "counties", flu = "target")], fill = TRUE)
  res <- rbind(res, ctrgs[, .(id = control_rgs_id, type = "residential-units", 
                               total_capacity = hh,
                               res_ratio = 50, name = rgs_name,
                               geography = "rgs", flu = "target")], fill = TRUE)
}


# prepare for plotting results

# create a long format dataset
res2 <- melt(res, id.vars = c("flu", "id", "name", "geography", "type", "res_ratio"), 
             variable.name = "indicator")

# filter out data for the desired indicators
reseb <- res2[id > 0 &  indicator %in% c("remaining_capacity", "total_capacity")][type == "non-residential-jobs" & indicator == "total_capacity", value := NA]

# convert to a wide format
reseb <- dcast(reseb, flu + geography + id + name + type + indicator ~ res_ratio, value.var = "value")
reseb[, flu := factor(flu, levels = c(rev(flu.names), "target"))]

# create a list with ggplot objects
allg <- gt <- NULL
for(geo in names(geographies)){
  # main plot 
  g <- ggplot(reseb[geography == geo & indicator == "total_capacity" & type != "non-residential-jobs" & flu != "target"], 
               aes(x = name, group = flu, color = flu)) + 
    geom_errorbar(aes(ymin = `40`, ymax = `60`), position = position_dodge(width=0.3), na.rm = TRUE)  + 
    geom_point(aes(y = `50`), na.rm = TRUE, position = position_dodge(width=0.3)) +
    facet_grid(type ~ . , scales = "free") + xlab("") + ylab("") +
    guides(x =  guide_axis(angle = 90))
  # add targets
  if(include.ct && nrow(reseb[geography == geo & indicator == "total_capacity"& flu == "target"]) > 0)
    g <- g + geom_point(data = reseb[geography == geo & indicator == "total_capacity" & flu == "target"], 
                       aes(y = `50`, fill = "HH target"), shape = 4, color = "black") +
      scale_fill_manual(name = "", values = c("HH target" = "yellow"))
  if(include.ct && geo == "controls"){
    # create a plot with only those ids where target is larger than capacity
    datct <- reseb[geography == geo & indicator == "total_capacity" & type == "residential-units"]
    datct[datct[flu == "target"], target := `i.50`, on = c("id")]
    show.ids <- unique(datct[flu == "new" & `60` - target < -5, id])
    if(length(show.ids) > 0) {
      dat <- datct[id %in% show.ids]
      gt <- ggplot(dat[flu != "target"], 
                aes(x = name, group = flu, color = flu)) + 
        geom_errorbar(aes(ymin = `40`, ymax = `60`), position = position_dodge(width=0.3), na.rm = TRUE)  + 
        geom_point(aes(y = `50`), na.rm = TRUE, position = position_dodge(width=0.3)) +
        facet_grid(type ~ . , scales = "free") + xlab("") + ylab("") +
        guides(x =  guide_axis(angle = 90)) +
        geom_point(data = dat[flu == "target"], aes(y = `50`, fill = "HH target"), shape = 4, color = "black") +
        scale_fill_manual(name = "", values = c("HH target" = "yellow"))
    }
    caplack <- datct # keep that dataset for debugging purposes
  }
  allg[[geo]] <- g
}

print(allg[["counties"]])
print(allg[["rgs"]])
print(allg[["cities"]])
print(allg[["large_areas"]])
print(allg[["growth_centers"]])
print(allg[["controls"]])


# save plots into file
pdf(file = paste0("capacity_comparisons_various_gegraphies_flu-", flu.date[1], "_", flu.date[2], 
                  if(consider.current.built) "with_curbuilt" else "", ".pdf"), width = 14, height = 8)
#pdf(file = paste0("capacity_no_lc_comparisons_various_gegraphies_flu-", flu.date[1], "_", flu.date[2], ".pdf"), width = 14, height = 8)

print(allg[["counties"]] + ggtitle("Counties"))
print(allg[["rgs"]] + ggtitle("Regional Geographies"))
print(allg[["large_areas"]]+ ggtitle("Large Areas"))
print(allg[["growth_centers"]] + ggtitle("Growth Centers"))
print(allg[["cities"]]  + ggtitle("Cities"))
print(allg[["controls"]]  + ggtitle("Controls"))

if(include.ct && !is.null(gt)){
  print(gt + ggtitle("Controls lacking residential capacity"))
}
dev.off()


# create a parcel dataset for further debugging below
if(!exists("pcl.to.store")) {
  pcl.to.store <- merge(allpcls[[1]], allpcls[[2]])
  colnames(pcl.to.store) <- gsub(".x", paste0("_", flu.names[1]), colnames(pcl.to.store), fixed = TRUE)
  colnames(pcl.to.store) <- gsub(".y", paste0("_", flu.names[2]), colnames(pcl.to.store), fixed = TRUE)
  pcl.to.store <- merge(pcls[, .(parcel_id, county_id, large_area_id, city_id, faz_id, zone_id, 
                                 growth_center_id, control_id, control_rgs_id, parcel_sqft)],
                        pcl.to.store)
}

stop("End of processing")

# below is exploration code
############################
spcls <- pcl.to.store[city_id == 38]
spcls <- pcl.to.store[growth_center_id == 604]
spcls <- pcl.to.store[control_id == 176]
spcls[, .(DUnew = sum(`residential-units_new`, na.rm = TRUE), 
          NRSFnew = sum(`non-residential-sqft_new`, na.rm = TRUE)
          ), by = "plan_type_id_new"][order(-NRSFnew)]#[order(-DUnew)]
spcls[, .(DUold = sum(`residential-units_old`, na.rm = TRUE), 
          NRSFold = sum(`non-residential-sqft_old`, na.rm = TRUE)
          ), by = "plan_type_id_old"][order(-NRSFold)]#[order(-DUold)]
spcls[, .(DUnew = sum(`residential-units_new`, na.rm = TRUE), DUold = sum(`residential-units_old`, na.rm = TRUE)
), by = "plan_type_id_new"][order((DUnew - DUold))]
spcls[, .(NRSFnew = sum(`non-residential-sqft_new`, na.rm = TRUE),
          NRSFold = sum(`non-residential-sqft_old`, na.rm = TRUE)
), by = "plan_type_id_new"][order(NRSFnew- NRSFold)]#[order(-DUnew)]

spcls[, .N, by = "plan_type_id_new"][order(-N)]
spcls[, .N, by = "plan_type_id_old"][order(-N)]
spcls[plan_type_id_new %in% c(606), .N, by = "plan_type_id_old"][order(-N)]
  
constr.old <- fread(file.path(constr.dir, paste0("devconstr_final_", flu.date[1], ".csv")))
constr.new <- fread(file.path(constr.dir, paste0("devconstr_final_", flu.date[2], ".csv")))
flu.old <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date[1], ".csv")))
flu.new <- fread(file.path(flu.dir, paste0("flu_imputed_ptid_", flu.date[2], ".csv")))
flu.new[plan_type_id %in% c(623)]
flu.old[plan_type_id %in% c(1714)]

# check where there is only Mixed_Use and nothing else
length(flu.new[Mixed_Use == 1 & Res_Use == 0 & Comm_Use == 0 & Office_Use == 0 & Indust_Use == 0, juris_zn])
paste(sort(flu.new[Mixed_Use == 1 & Res_Use == 0 & Comm_Use == 0 & Office_Use == 0 & Indust_Use == 0, juris_zn]), collapse = ", ")

spcls <- pcl.to.store[control_id %in% show.ids]
spcls <- pcl.to.store[control_id %in% c(76, 124, 176)]
spcls[pcls, gross_sqft := i.gross_sqft, on = "parcel_id"]
spcls[plan_type_id_new >= 9000, plan_type_id_new := 9000]
spcls2 <- spcls[, .(Npcl = .N, acres = round(sum(parcel_sqft / 43560)),
                            gross_acres = round(sum(gross_sqft / 43560)),
                            DUcap = round(sum(`residential-units_new`, na.rm = TRUE))), 
     by = c("control_id", "plan_type_id_new")][control_id %in% c(76, 124, 176) & gross_acres > 0]
setnames(spcls2, "plan_type_id_new", "plan_type_id")
spcls2[flu.new, `:=`(fluLC = round(i.LC_Res*100), fluDUA = i.MaxDU_Res), on = "plan_type_id"]#[
  #, DUA := round(DUcap/(acres * fluLC/100), 3)]
spcls2[, is_dev := plan_type_id < 9000]
spcls2[caplack, `:=`(target = i.target, name = i.name), on = c(control_id = "id")][control_id == 176][, pcnt_acres := round(acres/sum(acres)*100, 1)][order(-gross_acres)]
25682 * 0.35 * 0.29
spcls2[order(name, -gross_acres)]


lc <- 0.35
stbl <- spcls2[control_id %in% c(176, 76, 124) & is_dev, .(
  inc_lockouts = 0,
  DUcap = sum(DUcap), Target = mean(target),
           Npcl = sum(Npcl), 
           acres = sum(acres), gross_acres = sum(gross_acres)
           ), by = c("name")][order(name)]

stbl <- rbind(stbl, 
              spcls2[control_id %in% c(176, 76, 124), .(
                inc_lockouts = 1,
                DUcap = sum(DUcap), Target = mean(target),
                Npcl = sum(Npcl), 
                acres = sum(acres), gross_acres = sum(gross_acres)
              ), by = c("name")][order(name)])
stbl[, `:=`(DUA = round(DUcap / (lc * acres), 3), grossDUA = round(DUcap/(lc * gross_acres), 3), 
            targetDUA = round(Target / (lc * acres), 3), 
            target_grossDUA = round(Target/(lc * gross_acres), 3))]

setnames(stbl, "DUcap", "DU capacity")
#fwrite(stbl, file = "rural_cap.csv")

ppcls2 <- spcls2[control_id == 124]
spcls[control_id == 124 & plan_type_id_new == 1132 & `residential-units_old` > 0, 
      .(DU = sum(`residential-units_old`), 
        DUA = sum(`residential-units_old`)/(sum(gross_sqft)/ 43560))]
