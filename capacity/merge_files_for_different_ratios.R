##################################################
# Script for merging multiple parcel-level capacity files
# that were computed for different residential ratios
# Hana Sevcikova, PSRC, 2019-01-07
##################################################

library(data.table)

# files to merge
inputs <- c("CapacityPcl_res50-2018-10-02.csv", "CapacityPcl_res0-2019-01-07.csv", "CapacityPcl_res100-2019-01-07.csv")

# which res.ratio each file corresponds to (in the same order as inputs)
res.ratios <- c(50, 0, 100)

# output file
outfile <- paste0("CapacityPcl_res0-50-100-", Sys.Date(), ".csv")

rename.and.remove.cols <- function(dt, value, remove.base.cols = FALSE) {
  for(col in capacity.cols) {
    setnames(dt, paste0(col, "capacity"), paste0(col, "capacity", value))
    if(remove.base.cols) {
      to.remove <- paste0(col, "base")
      dt[[to.remove]] <- NULL
    }
  }
  dt
}

capacity.cols <- c("DU", "NRSQF", "JOBSP", "BLSQF")
dat <- fread(inputs[1])
dat <- dat[, c("parcel_id", grep("base", colnames(dat), value = TRUE)), with = FALSE]
for(iratio in 1:length(res.ratios)) {
  dt <- fread(inputs[iratio])
  dt <- rename.and.remove.cols(dt, res.ratios[iratio], remove.base.cols = TRUE)
  dat <- merge(dat, dt)
}

fwrite(dat, file = outfile)
