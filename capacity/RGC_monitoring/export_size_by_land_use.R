library(data.table)

# Settings
#==========
# were are parcels and various Xwalk tables, exported from the base year DB
data.dir <- "../../data/BY2018"

# read parcels file and parcels with updated MICs and update mic_id
pcls <- fread(file.path(data.dir, "parcels.csv"))
pcls.upd <- fread(file.path(data.dir, "parcels18_with_updated_mics.csv"))
pcls <- merge(pcls, pcls.upd, by = "parcel_id")
pcls[, mic_id := updated_mic_id][, updated_mic_id := NULL]

# read land use types
lus <- fread(file.path(data.dir, "land_use_types.csv"))
glus <- fread(file.path(data.dir, "generic_land_use_types.csv"))

# join parcels with info in the updated MIC Xwalk table
mics <- fread(file.path(data.dir, "updated_mics.csv"))
# add not-a-center row
mics <- rbind(mics, data.table(updated_mic_id = 0, mic = "no MIC"))
pcls <- merge(pcls, mics[, .(updated_mic_id, mic_name = mic)], 
              by.x = "mic_id", by.y = "updated_mic_id") # this merge keeps only parcels with MICs being included in mics

# update various land use type categories
pcls[land_use_type_id %in% c(0, 22), land_use_type_id := 17] # ROW and missing codes are "no-code"
pcls[land_use_type_id == 23, land_use_type_id := 7] # school is moved to government

# join with land use types to derive generic LUT
pcls[lus, generic_land_use_type_id := i.generic_land_use_type_id, on = "land_use_type_id"]
glus[, generic_land_use_type_name := gsub(" ", "_", generic_land_use_type_name)]
# make the GLUT a factor in order to sort it the way we want
glus[, generic_land_use_type := factor(generic_land_use_type_id, labels = generic_land_use_type_name)]
pcls[glus, generic_land_use_type := i.generic_land_use_type, on = "generic_land_use_type_id"]

# add county info (in the end it's not needed)
county_x_rgc <- unique(pcls[, .(county_id, mic_id)])[mic_id > 0]
county_x_rgc[, county := factor(county_id, levels = c(33, 35, 53, 61, 0), 
                                labels = c("King", "Kitsap", "Pierce", "Snohomish", "Region (rest UGB)"))]
pcls2 <- merge(pcls, county_x_rgc, by = c("county_id", "mic_id"))

# aggregate
aggr.size <- pcls2[, .(Nparcels = .N, size_net = round(sum(as.double(parcel_sqft))/43560),
                      size_gross = round(sum(as.double(gross_sqft))/43560)
                ), by = c("mic_name", "generic_land_use_type")]

# convert to long format
aggr.size.long <- melt(aggr.size, measure.vars = c("Nparcels", "size_net", "size_gross"),
                       variable.name = "indicator")

# convert to wide format so that MICs are columns
aggr.size.wide <- dcast(aggr.size.long, generic_land_use_type + indicator ~ mic_name,
                        value.var = "value", fill = 0)

# make indicator a factor for sorting purposes
aggr.size.wide[, indicator := factor(indicator, levels = c("size_gross", "size_net", "Nparcels"))]

# order rows
aggr.size.wide <- aggr.size.wide[order(indicator, generic_land_use_type)]

# order columns (here hard-coded for the specific MICs; double-check if MICs change)
setcolorder(aggr.size.wide, c(2, 1, 4, 6, 10, 11, 3, 5, 7:9, 12))

# write to a csv file
fwrite(aggr.size.wide, file = "MICs_size_by_land_use_type.csv")
