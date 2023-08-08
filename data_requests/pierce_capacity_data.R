# Extract capacity data for Pierce county
# Hana Sevcikova, 2023/08/08

library(data.table)

data.dir <- "J:/Projects/Bill-Analysis/2023/data"
#data.dir <- "~/psrc/R/bill-analysis/data"
parcel.file <- "parcels_for_bill_analysis.csv"
plan.type.file <- "plan_type_id_summary_r109_script.csv"

# Read data
pcl <- fread(file.path(data.dir, parcel.file))
plan.type <- fread(file.path(data.dir, plan.type.file))

# Replace "-" with NAs in plan.type table
for(col in setdiff(colnames(plan.type), c("plan_type_id", "zoned_use", "zoned_use_sf_mf"))){ # iterate over all columns except 3 columns 
    plan.type[get(col) == "-", (col) := NA]
    plan.type[, (col) := gsub(",", "", get(col))] # remove ',' 
    plan.type[, (col) := as.numeric(get(col))] # convert to numeric
}

outpcl <- pcl[county_id == 53, .(parcel_id, lat, lon, plan_type_id, DU = residential_units, nrSQFT = non_residential_sqft, DUcap = round(DUcap), nrSQFTcap = round(SQFTcap))]
outpcl <- merge(outpcl, plan.type[, .(plan_type_id, max_du, max_far)], by = "plan_type_id", sort = FALSE)

# Replace NAs with 0
outpcl[is.na(max_du), max_du := 0]
outpcl[is.na(max_far), max_far := 0]

# Remove unnecessary columns
outpcl[, plan_type_id := NULL]

# Write into file
fwrite(outpcl, file = "pierce_parcel_capacity.csv")

