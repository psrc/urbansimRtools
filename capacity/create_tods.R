# Script to create a lookup dataset of TODs.
# It also updates the parcel dataset by adding the MIC tod.
# HS 2019-3-06
##################

#dir <- "J:/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"
dir <- "/Volumes/DataTeam/Projects/Parcel\ Data/Capacity/lookup-2018-10-01"

# create a dataset of TODs and write into lookup directory
tods <- data.frame(tod_name = c("LR", "CR", "Ferries", "BRT", "RGC", "MIC", "Rest"),
                   tod_id = c(4, 2, 5, 1, 6, 7, 0)
                   )
write.csv(tods, file = file.path(dir, "tods.csv"), row.names = FALSE)

# update parcels with MICs being tod_id of 7
library(data.table)
pcls <- fread(file.path(dir, "parcels_geos.csv"))
pcls[tod_id == 0 & growth_center_id > 600, tod_id := 7]
fwrite(pcls, file = file.path(dir, "parcels_geos_mic.csv"))
