library(dplyr)
library(readr)
library(car)

setwd("J:/Projects/2018_base_year/Costar_Downloads_July2021/Complete_Extracts/")

#import and merge all three CSV files into one data frame

all_costar <- list.files(path="J:/Projects/2018_base_year/Costar_Downloads_July2021/Complete_Extracts/") %>% 
  lapply(read_csv) %>% 
  bind_rows 

myvars <- c("Property Address","City","State","Zip","Property Name","PropertyType","RBA","Land Area (SF)","Land Area (AC)","Year Built","Constr Status")
extract_cols_costar <- df[myvars]

extract_cols_costar <- extract_cols_costar %>%
  rename("Building_SQFT" = "RBA", "Parcel_Size_SQFT" = "Land Area (SF)", "Parcel_Size_Acres" = "Land Area (AC)")

subextract_costar_nonzero_acres <- subset(extract_cols_costar, extract_cols_costar$Parcel_Size_SQFT > 0,)

subextract_costar_nonzero_acres_far <- subextract_costar_nonzero_acres %>% mutate(far = Building_SQFT /Parcel_Size_SQFT)

subextract_costar_nonzero_acres_bigfar <- subset(subextract_costar_nonzero_acres_far, subextract_costar_nonzero_acres_far$far >= 4,)

attach(subextract_costar_nonzero_acres_bigfar)
plot(Parcel_Size_SQFT, far, main="Scatterplot Example",
     xlab="Parcel Size in SQFT", ylab="Floor Area Ratio", pch=19)

Parcel_size <- subextract_costar_nonzero_acres_bigfar$Parcel_Size_SQFT
hist(Parcel_size, xlim=c(0,10000), ylim=c(0,1000), breaks=100)

write.csv(subextract_costar_nonzero_acres_bigfar, "T:\\2023February\\MarkS\\Costar_FAR4plus.csv", row.names=FALSE)

quantile(subextract_costar_nonzero_acres_bigfar$Parcel_Size_SQFT, c(0, 0.1, 0.25, 0.5, 0.75, 0.9, 1))
