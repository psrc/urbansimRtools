# Read Mark's spreadsheet, extract PPH data and save

library(data.table)
library(readxl)

# were to save results
data.dir <- "../../data/BY2018"

# read Mark's spreadsheet
marks.file.name <- "Centers Capacity Monitoring - Working File.xlsx" # should include the full path

#pph.header <- as.vector(read_xlsx(marks.file.name, sheet = "PPH by Control ID", skip=2, n_max = 1, col_names = FALSE))
pph.df <- read_xlsx(marks.file.name, sheet = "PPH by Control ID", #range = cell_cols(c("A:F", "AG:AH")), 
                    skip = 2)
# subset only what we need 
pph.dt <- data.table(pph.df[, c(1:6, 29:34)])
#fix column headers
colnames(pph.dt)[-(2:5)] <- c("control_id", "name", "pph2020_nonhct", "pph2050_nonhct", 
                              "pph2020_hct", "pph2050_hct", "pph2020_tot", "pph2050_tot")

fwrite(pph.dt, file = file.path(data.dir, "pph.csv"))
