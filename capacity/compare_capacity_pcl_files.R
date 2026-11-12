library(data.table)
library(ggplot2)

# Compare parcel-level capacity from two capacity files (as produced by
# parcels_capacity_from_FLU.R and parcels_capacity.R) across various geographies.

# Settings
#==========
data.dir <- "../data/BY2023updgeo" # data with updated geographies

# capacity files to compare and their labels
#cap.files <- c("CapacityPclFLU_res50_flu-2026-09-30_2026-10-13.csv",
#               "CapacityPclNoSampling_res50-2026-10-14.csv")
#cap.names <- c("FLU 10-13", "NoSampling 10-14")
cap.files <- c("CapacityPclFLU_res50_flu-2026-10-21_2026-10-26.csv",
               "CapacityPclNoSampling_res50-2026-11-10.csv")
cap.files <- c("CapacityPclNoSampling_res50-2026-10-26.csv",
               "CapacityPclNoSampling_res50-2026-11-10.csv")
#cap.names <- c("FLU 10-26", "NoSampling 10-26")
cap.names <- c("FLU 11-10", "urbansim 11-10")
cap.names <- c("urbansim 10-26", "urbansim 11-10")

# include targets in the plots
include.ct <- TRUE

# output file
#out.file <- "capacity_comparisons_various_geographies_FLU-2026-10-13_NoSampling-2026-10-14.pdf"
out.file <- "capacity_comparisons_various_geographies_FLU-2026-10-26_urbansim-2026-11-10.pdf"
out.file <- "capacity_comparisons_various_geographies_urbansim-2026-10-26_urbansim-2026-11-10.pdf"

# counties names
counties <- data.table(name = c("King", "Kitsap", "Pierce", "Snohomish"),
                       county_id = c(33, 35, 53, 61))

# load cities and change names, so that names are unique (either adding RG name or county name)
cities <- fread(file.path(data.dir, "cities.csv"))
cities[, N := .N, by = "city_name"]
cities[, Ncnty := .N, by = c("city_name", "county_id")]
cities[N > 1 & Ncnty > 1  & rg_proposed != "CitiesTowns", city_name := paste(city_name, rg_proposed)]
cities[, N2 := .N, by = "city_name"]
cities[, Ncnty2 := .N, by = c("city_name", "county_id")]
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
                    rgs = list(xwalk = rgs, id_name = "control_rgs_id", name_col = "control_rgs_name"),
                    region = list(xwalk = data.table(name = "region", region_id = 1),
                                  id_name = "region_id", name_col = "name")
                    )
for(geo in names(geographies))
  setnames(geographies[[geo]]$xwalk, geographies[[geo]]$name_col, "name")

# Load parcels with geography ids
#==============
pcls <- fread(file.path(data.dir, "parcels.csv"),
              select = c("parcel_id", "county_id", "city_id", "large_area_id",
                         "growth_center_id", "control_id"))
setkey(pcls, "parcel_id")
pcls[city_id == 107, city_id := 109] # correct one city
pcls[controls, control_rgs_id := i.control_rgs_id, on = "control_id"]
pcls[, region_id := 1]

# Load capacity files and aggregate
#==============
allres <- NULL
for(i in seq_along(cap.files)){
  cap <- fread(cap.files[i], select = c("parcel_id", "DUcapacity", "NRSQFcapacity"))
  cap <- merge(pcls, cap, by = "parcel_id")
  for(geo in names(geographies)){
    id <- geographies[[geo]]$id_name
    res <- cap[, .(`residential-units` = sum(DUcapacity, na.rm = TRUE),
                   `non-residential-sqft` = sum(NRSQFcapacity, na.rm = TRUE)), by = id]
    res[geographies[[geo]]$xwalk, name := i.name, on = id]
    setnames(res, id, "id")
    res <- melt(res, id.vars = c("id", "name"), variable.name = "type", value.name = "capacity")
    allres <- rbind(allres, res[, `:=`(geography = geo, source = cap.names[i])])
  }
}

if(include.ct){
  # load control totals for plotting targets
  ct <- fread(file.path(data.dir, "annual_household_control_totals.csv"))
  ct <- ct[year == 2050, .(hh = sum(total_number_of_households)), by = "subreg_id"]
  ct[, control_id := subreg_id][subreg_id > 1000, control_id := control_id - 1000]
  ct <- ct[, .(hh = sum(hh)), by = "control_id"]
  ct[controls, `:=`(name = i.name, county_id = i.county_id, county_name = i.county_name,
                    control_rgs_id = i.control_rgs_id), on = "control_id"]
  ct[rgs, rgs_name := i.name, on = "control_rgs_id"]
  ctcnty <- ct[, .(hh = sum(hh)), by = c("county_id", "county_name")]
  ctrgs <- ct[, .(hh = sum(hh)), by = c("control_rgs_id", "rgs_name")]

  allres <- rbind(allres,
                  ct[, .(id = control_id, name, type = "residential-units", capacity = hh,
                         geography = "controls", source = "target")],
                  ctcnty[, .(id = county_id, name = county_name, type = "residential-units", capacity = hh,
                             geography = "counties", source = "target")],
                  ctrgs[, .(id = control_rgs_id, name = rgs_name, type = "residential-units", capacity = hh,
                            geography = "rgs", source = "target")])
}
allres <- allres[id > 0]
allres[, type := factor(type, levels = c("residential-units", "non-residential-sqft"))]
allres[, source := factor(source, levels = c(rev(cap.names), "target"))]

# Plot
#==============
make.plot <- function(dat){
  g <- ggplot(dat[source != "target"], aes(x = name, y = capacity, group = source, color = source)) +
    geom_point(position = position_dodge(width = 0.3)) +
    facet_grid(type ~ . , scales = "free") + xlab("") + ylab("") +
    guides(x = guide_axis(angle = 90))
  if(include.ct && nrow(dat[source == "target"]) > 0)
    g <- g + geom_point(data = dat[source == "target"], aes(fill = "HH target"),
                        shape = 4, color = "black") +
      scale_fill_manual(name = "", values = c("HH target" = "yellow"))
  g
}

allg <- NULL
for(geo in names(geographies))
  allg[[geo]] <- make.plot(allres[geography == geo])

gt <- NULL
if(include.ct){
  # controls where target is larger than capacity of the second source
  datct <- allres[geography == "controls" & type == "residential-units"]
  datct[datct[source == "target"], target := i.capacity, on = "id"]
  show.ids <- unique(datct[source == cap.names[2] & capacity - target < -5, id])
  if(length(show.ids) > 0)
    gt <- make.plot(datct[id %in% show.ids])
}

pdf(file = out.file, width = 14, height = 8)
print(allg[["region"]] + ggtitle("Region"))
print(allg[["counties"]] + ggtitle("Counties"))
print(allg[["rgs"]] + ggtitle("Regional Geographies"))
print(allg[["large_areas"]] + ggtitle("Large Areas"))
print(allg[["growth_centers"]] + ggtitle("Growth Centers"))
print(allg[["cities"]] + ggtitle("Cities"))
print(allg[["controls"]] + ggtitle("Controls"))
if(!is.null(gt))
  print(gt + ggtitle(paste("Controls lacking residential capacity in", cap.names[2])))
dev.off()

# summary table on regional level
print(dcast(allres[geography == "region"], type ~ source, value.var = "capacity"))
