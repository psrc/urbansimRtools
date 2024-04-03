library(data.table)
library(readxl)

# Settings
#==========
# were are parcels and various Xwalk tables, exported from the base year DB
data.dir <- "../../data/BY2018"

# Results from the script compute_capacity_with_pop.R
capacity.file <- "RGC_capacity_data_2024-04-02.csv"

# which developable factor and residential ratio we want in the summary 
# (must be present in the capacity file; if not, rerun compute_capacity_with_pop.R
#  with the right settings)
developable.factors <- c(1, 2, 3)
res.ratios <- c(40, 50, 60)

# Load data
# ==========
# read capacity results
capacity <- fread(capacity.file)

# read parcels file and update with the updated RGCs
pcls <- fread(file.path(data.dir, "parcels.csv"))
pcls.upd <- fread(file.path(data.dir, "parcels18_with_updated_growth_center_id.csv"))
pcls <- merge(pcls, pcls.upd, by = "parcel_id")
pcls[, growth_center_id := updated_growth_center_id][, updated_growth_center_id := NULL]

# join parcels with info in the updated RGC Xwalk table
gcs <- fread(file.path(data.dir, "growth_centers_updated.csv"))
# add not-a-center row
gcs <- rbind(gcs, data.table(growth_center_id = 0, growth_center_name = "no RGC", growth_center_class = "None"))
pcls <- merge(pcls, gcs[, .(growth_center_id, class = growth_center_class)], 
              by = "growth_center_id") # this merge keeps only parcels with growth_center_id being included in gcs

# Processing

# filter res ratio only 
capacity <- capacity[res_ratio %in% res.ratios][, `:=`(percent_rem_cap = NULL, remaining_total_capacity = NULL)]

county_x_rgc <- unique(pcls[, .(county_id, growth_center_id, class)])
#county_x_rgc <- rbind(county_x_rgc, data.table(county_id = 0, growth_center_id = 0, class = "None", county = "Region (rest)"))
county_x_rgc[, county := factor(county_id, levels = c(33, 35, 53, 61), 
                                labels = c("King", "Kitsap", "Pierce", "Snohomish"))]

aggrwall <- NULL
for(developable.factor in developable.factors){
    # filter to get the medium series and the desired developable factor
    capacity50 <- capacity[res_ratio == 50 & developfac == developable.factor][, res_ratio := NULL]
    
    # aggregate RGCs parcels and acres
    aggr.size <- pcls[, .(Nparcels = .N, size = round(sum(as.double(parcel_sqft))/43560)), by = c("county_id", "class")]
    
    # add county_id to capacity50 and aggregate
    capacity50 <- merge(capacity50, county_x_rgc, by = c("growth_center_id", "class"))
    #capacity50[growth_center_id == 0, `:=`(county_id = 0, county = "Region")]
    aggr.cap <- capacity50[, .(total_capacity = sum(total_capacity), 
                               total_developable_capacity = sum(total_developable_capacity), 
                               remaining_capacity = sum(remaining_capacity)), 
                           by = .(county_id, county, class, type)]
    aggr.cap.tot <- dcast(aggr.cap, county_id + county + class ~ type, value.var = "total_capacity")
    colnames(aggr.cap.tot)[4:ncol(aggr.cap.tot)] <- paste0("totcap-", colnames(aggr.cap.tot)[4:ncol(aggr.cap.tot)])
    aggr.cap.totdev <- dcast(aggr.cap, county_id + county + class ~ type, value.var = "total_developable_capacity")
    colnames(aggr.cap.totdev)[4:ncol(aggr.cap.totdev)] <- paste0("totdevcap-", colnames(aggr.cap.totdev)[4:ncol(aggr.cap.totdev)])
    aggr.cap.net <- dcast(aggr.cap, county_id + county + class ~ type, value.var = "remaining_capacity")
    colnames(aggr.cap.net)[4:ncol(aggr.cap.net)] <- paste0("netcap-", colnames(aggr.cap.net)[4:ncol(aggr.cap.net)])
    
    aggrw <- merge(aggr.size, merge(merge(aggr.cap.tot, aggr.cap.totdev, by = c("county_id", "county", "class")),
                                    aggr.cap.net,  by = c("county_id", "county", "class")), by = c("county_id", "class"))
    setcolorder(aggrw, c("county_id", "county", "class"))
    
    aggrwall <- rbind(aggrwall, aggrw[, developfac := developable.factor])
}
aggrwall[, class := factor(class, levels = c("Metro", "Urban", "None"))]
aggrwall <- aggrwall[order(developfac, county_id, class)]
fwrite(capacity, paste0("RGCcapacity-", Sys.Date(), ".csv"))
fwrite(aggrwall, paste0("sumCapacity-", Sys.Date(), ".csv"))
