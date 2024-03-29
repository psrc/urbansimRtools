##################################################
# Script for plotting control totals generated by the split_ct_to_hct.R script
# by jurisdictions and HCT areas, as well as their aggregations into RGs.
#
# Hana Sevcikova, PSRC, updated on 2023/03/27
##################################################

library(data.table)
library(openxlsx)
library(ggplot2)
library(scales)

setwd("~/psrc/R/urbansimRtools/control_totals")

do.plots <- TRUE # should pdf files be created
do.write <- FALSE # should a data file with the values shown in the graphs be created 

# directory where the input file for the split_ct_to_hct.R script lives
ctnew.dir <- '/Users/hana/psrc/R/control-total-vision2050' 

# directory where file with RGS and control ids live
data.dir <- '/Users/hana/psrc/control_totals'

CTaggr.file <- file.path(ctnew.dir, paste0('Control-Totals-LUVit-2022-11-15.xlsx')) # input to split_ct_to_hct.R
CTnew.file <- paste0('LUVit_ct_by_tod_generator_90-90-90_2023-01-11.xlsx') # output of split_ct_to_hct.R

# spreadsheets with RGS and control_id definition
rgs.file <- file.path(data.dir, 'ConTot\ R\ Script\ Inputs\ 071222.xlsx')
control.def.file <- file.path(data.dir, 'control_id_working_111522.xlsx') # should have columns control_id, name, county_id, RGID

# legends used in the plots
CTdata.legend <- list(updCT = "LUV-it", hct = "HCT", nohct = "non HCT", currentCT = "V2050 RGS", vision = "Vision",
                      upd.target = "Draft Targets", luv2 = "LUV.2", rebased.rgs = "2020 adjusted RGS",
                      orig.rgs = "Orig RGS", adj.rgs = "Orig adjusted RGS"
                      )

# which indicators 
indicator.names <- c("population", "HHpopulation", "households", "employment")

# read the output of split_ct_to_hct.R
CTnew <- data.table(read.xlsx(CTnew.file, sheet = "unrolled"))

# rename column names
setnames(CTnew, c("subreg_id", "total_hhpop", "total_hh", "total_emp"#, "total_pop"
                  ), c("control_id", "HHpopulation", "households", "employment"#, "population"
                       ))

# convert to long format
CTnewl <- melt(CTnew, measure.vars = setdiff(indicator.names, "population"), 
               id.vars = c("control_id", "year"), variable.name = "indicator")[, CTdata := CTdata.legend[["updCT"]]]

# read the input to split_ct_to_hct.R (used for aggregates into jurisdictions)
CTagg <- data.table(read.xlsx(CTaggr.file, sheet = "unrolled"))

# rename column names
setnames(CTagg, c("subreg_id", "total_hhpop", "total_hh", "total_emp", "total_pop"), 
         c("control_id", "HHpopulation", "households", "employment", "population"))

# convert to long format
CTaggl <- melt(CTagg, measure.vars = "population", id.vars = c("control_id", "year"), 
               variable.name = "indicator")[, CTdata := CTdata.legend[["updCT"]]][year > 2018]

# combine jurisdictions and hct controls together
CTnewl <- rbind(CTnewl, CTaggl)

# function to get line colors
gg_color_hue <- function(n) {
    hues = seq(15, 375, length = n + 1)
    hcl(h = hues, l = 65, c = 100)[1:n]
}

# read control_id definition and join with RG and county names
jurXwalk <- data.table(read.xlsx(control.def.file, sheet = 1, startRow = 1))
jurXwalk <- jurXwalk[, .(control_id, name, county_id, RGID)]
rgs <- data.table(RGID = 1:6, RG = c("Metro", "Core", "HCT", "Cities & Towns", "UU", "Rural"))
cnties <- data.table(county_id = c(33, 35, 53, 61), county = c("King", "Kitsap", "Pierce", "Snohomish"))
jurXwalk <- merge(jurXwalk, cnties, by = "county_id")
jurXwalk[RGID > 6, RGID := 6]
jurXwalk <- merge(jurXwalk, rgs, by = "RGID")

rgft <- unique(jurXwalk[, .(RGID, RG)])[order(RGID)]
rgf <- rgft[, RG]

# Merge everything together and add the corresponding legends
CTnewl[, `:=`(subreg_id = control_id)][control_id > 1000, control_id := control_id - 1000]
hctjurs <- unique(CTnewl[subreg_id > 1000, control_id])
CTnewljur <- CTnewl[, .(value = sum(value)), by = .(control_id, indicator, year, CTdata)]
CTnewljur <- rbind(CTnewljur, CTnewl[subreg_id > 1000 & control_id %in% hctjurs & indicator != "population", .(control_id, indicator, year, CTdata, value)][, CTdata := CTdata.legend[["hct"]]])
CTnewljur <- rbind(CTnewljur, CTnewl[subreg_id < 1000 & control_id %in% hctjurs & indicator != "population", .(control_id, indicator, year, CTdata, value)][, CTdata := CTdata.legend[["nohct"]]])

CT <- merge(CTnewljur, jurXwalk, by = "control_id")[indicator %in% indicator.names]
CT[, CTdata := factor(CTdata, levels = unlist(CTdata.legend))]
CT[, RG := factor(RG, levels = rgf)]
CT[, year := as.integer(year)]
CT[is.na(value) & indicator == "employment", value := 0]

# how many rows of plots on one page
nrows <- 4

# create plot titles by combining jurisdiction names with their RG names
juris.to.plot <- jurXwalk[order(county_id, RGID, control_id)]
juris.to.plot <- juris.to.plot[control_id %in% CT$control_id][, title := paste0(name, ", ", county, " - ", RG)]
CT[juris.to.plot, title := i.title, on = "control_id"]


if(do.plots) {
    # Create jurisdiction plots
    CTcities <- CT[indicator != "population"]
    # assure the scale on the y axis is at least 500
    CTcities.tmp <- CTcities[year == 2020 & CTdata == CTdata.legend[["updCT"]]]
    CTcities.tmp <- CTcities.tmp[CTcities[year == 2050 & CTdata == CTdata.legend[["updCT"]]], `:=`(value.start = value, value.end = i.value, dif = i.value - value), on = .(control_id, indicator, RGID, county_id, name, county, RG)]
    CTcities.tmp <- CTcities.tmp[dif < 500]
    CTcities.tmp2 <- rbind(CTcities.tmp[value < 500 & value.end < 500][, `:=`(year = 2020, value = 0)],
                           CTcities.tmp[value < 500 & value.end < 500][, `:=`(year = 2050, value = 500)])
    CTcities.tmp <- CTcities.tmp[!(value < 500 & value.end < 500)]
    CTcities.tmp2 <- rbind(CTcities.tmp2, 
                           copy(CTcities.tmp)[, `:=`(year = 2020, value = pmax(0, value.start - round((500-dif)/2)))],
                           copy(CTcities.tmp)[, `:=`(year = 2050, value = value.end + round((500-dif)/2))]
                           )

    pdf(paste0("LUVit_CTs_juris_total-", Sys.Date(), ".pdf"), width = 12, height = 9)
    cols <- gg_color_hue(5)
    names(cols) <- CTdata.legend[c("updCT", "hct", "nohct", "rebased.rgs", "orig.rgs")]
    this.col <- cols[names(cols) %in% CTcities[, CTdata]]
    for(row in seq(1, nrow(juris.to.plot), by = nrows)){
        g <- ggplot(CTcities[control_id %in% juris.to.plot[row:(row+nrows-1), control_id]], aes(x = year, y = value, group = CTdata, color = CTdata)) +
            geom_line() + geom_point() + ylab("") + labs(color = NULL) + 
            facet_wrap(title ~ indicator, ncol = length(indicator.names)-1, scales = "free_y", as.table = FALSE) + 
            scale_color_manual(values=this.col, breaks = names(cols)) + 
            scale_y_continuous(labels = comma) + 
            geom_point(data = CTcities.tmp2[control_id %in% juris.to.plot[row:(row+nrows-1), control_id]], alpha = 0, show.legend = FALSE)
        print(g)
    }
    dev.off()
}


# Aggregations

# Load and process re-based RGS
RGSrb.pop <- data.table(read.xlsx(rgs.file, sheet = "Summary Reset RGS TotPop", 
                                    startRow = 2))[-(1:5)][County != "TOT"][, County := as.integer(County)]
RGSrb.pop <- RGSrb.pop[, .(county_id = County, RGID, `population.2020` = `2020.Estimates.(Census)`,
                               `population.2050` = `Reset.RGS.2050.Numbers.(20-50.Version)`)]

RGSrb.emp <- data.table(read.xlsx(rgs.file, sheet = "Summary Reset RGS TotEmp", 
                                  startRow = 2))[-(1:5)][County != "TOT"][, County := as.integer(County)]
RGSrb.emp <- RGSrb.emp[, .(county_id = County, RGID, `employment.2020` = `Updated.2020.Estimates.(No.Military)`,
                           `employment.2050` = `Reset.RGS.2050.Numbers.(20-50.Version)`)]

RGSrb <- merge(RGSrb.pop, RGSrb.emp, by = c("RGID", "county_id"))
RGSrbl <- melt(RGSrb, id.vars = c("county_id", "RGID"), variable.name = "indicator")
RGSrbl <- RGSrbl[, c("indicator", "year") := tstrsplit(indicator, ".", fixed=TRUE)][indicator %in% unique(CTnewl[,indicator])]
RGSrbl[, CTdata := CTdata.legend[["rebased.rgs"]]]
RGSrbl[, year := as.integer(year)]


# load alternative RGS
altRGS <- data.table(read.xlsx(rgs.file, sheet = "Better Format RGS All Versions", rows = c(1, 11:34)))
altRGSorig <- altRGS[, .(county_id = County, RGID = RegGeog, population.2017 = OrigRGS_TotPop_17, 
                         population.2044 = OrigRGS_TotPop_44, population.2050 = OrigRGS_TotPop_50,
                         employment.2017 = OrigRGS_Emp_17, employment.2044 = OrigRGS_Emp_44, 
                         employment.2050 = OrigRGS_Emp_50)]

# interpolate original RGS to 2020
for(row in 1:nrow(altRGSorig)) {
    altRGSorig[row, population.2020 := approx(c(2017, 2050), .(population.2017, population.2050), xout = 2020)$y]
    altRGSorig[row, employment.2020 := approx(c(2017, 2050), .(employment.2017, employment.2050), xout = 2020)$y]
}
altRGSorigl <- melt(altRGSorig, id.vars = c("county_id", "RGID"), variable.name = "indicator")
altRGSorigl <- altRGSorigl[, c("indicator", "year") := tstrsplit(indicator, ".", fixed=TRUE)][indicator %in% unique(CTnewl[,indicator])]
altRGSorigl[, `:=`(CTdata = CTdata.legend[["orig.rgs"]], year = as.integer(year))]


altRGSadj <- altRGS[, .(county_id = County, RGID = RegGeog, population.2017 = AdjRGS_TotPop_17,
                         population.2044 = AdjRGS_TotPop_44, population.2050 = AdjRGS_TotPop_50,
                         employment.2017 = AdjRGS_Emp_17, employment.2044 = AdjRGS_Emp_44,
                         employment.2050 = AdjRGS_Emp_50)]
# interpolate adjusted RGS to 2020
for(row in 1:nrow(altRGSadj)) {
    altRGSadj[row, population.2020 := approx(c(2017, 2050), .(population.2017, population.2050), xout = 2020)$y]
    altRGSadj[row, employment.2020 := approx(c(2017, 2050), .(employment.2017, employment.2050), xout = 2020)$y]
}
altRGSadjl <- melt(altRGSadj, id.vars = c("county_id", "RGID"), variable.name = "indicator")
altRGSadjl <- altRGSadjl[, c("indicator", "year") := tstrsplit(indicator, ".", fixed=TRUE)][indicator %in% unique(CTnewl[,indicator])]
altRGSadjl[, `:=`(CTdata = CTdata.legend[["adj.rgs"]], year = as.integer(year))]


# combine together
Visl <- copy(RGSrbl)
Visl <- rbind(Visl, #altRGSorigl#, 
             altRGSadjl#, # original adjusted
            )
Visl <- merge(rgs, Visl, by = "RGID")[, RG := factor(RG, levels = rgf)][, RGID := NULL]
Visl <- merge(Visl, cnties, by = "county_id")[, county_id := NULL]

# aggregate to county x RG
CTaggr <- CT[, .(value = sum(value)), by = .(year, indicator, CTdata, county, RG)]

CTaggr <- CTaggr[!CTdata %in% c(CTdata.legend[["currentCT"]], CTdata.legend[["luv2"]])]
CTaggr <- rbind(CTaggr, Visl)

CTaggr[, CTdata := factor(CTdata, levels = unlist(CTdata.legend))]

CTaggr[county == "Snohomish" & CTdata == CTdata.legend[["rebased.rgs"]], value := NA] # remove rebased RGS from Snohomish
CTaggr[county != "Snohomish" & CTdata == CTdata.legend[["adj.rgs"]], value := NA] # remove orig RGS from non-Snohomish counties

# compute delta      
CTaggr[CTaggr[year == 2020], delta := value - i.value, on = .(indicator, CTdata, county, RG)]


# # compute differences for county x RG
CTaggrdif <- CTaggr[year == 2050 & CTdata == CTdata.legend[["updCT"]]][
                       CTaggr[year == 2050 &
                                  ((county != "Snohomish" & CTdata == CTdata.legend[["rebased.rgs"]]) | (county == "Snohomish" & CTdata == CTdata.legend[["adj.rgs"]]))],
                                                                       .(RG, county, indicator, CTdata = i.CTdata,
                                                                         dif.value = value - i.value,
                                                                         dif.value.perc = round((value - i.value)/(i.value/100), 1),
                                                                         dif.delta = delta - i.delta,
                                                                         dif.delta.perc = round((delta - i.delta)/(i.delta/100), 1)
                                                                         ),
                                                                       on = .(RG, county, indicator)][, vjustvar := -3]
                  
CTaggrdif[, `:=`(year = 2015, value = Inf, delta = Inf, hjustvar = 3)]

# aggregate to county
CTcnty <- CTaggr[, .(value = sum(value), delta = sum(delta)), by = .(year, indicator, CTdata, county)]
# # compute differences for county-level data
CTcntydif <- CTcnty[year == 2050 & CTdata == CTdata.legend[["updCT"]]][
                        CTcnty[year == 2050 & 
                                   ((county != "Snohomish" & CTdata == CTdata.legend[["rebased.rgs"]]) | (county == "Snohomish" & CTdata == CTdata.legend[["adj.rgs"]]))], 
                                                                              .(county, indicator, CTdata = i.CTdata, 
                                                                                dif.value = value - i.value,
                                                                                dif.value.perc = round((value - i.value)/(i.value/100), 1),
                                                                                dif.delta = delta - i.delta,
                                                                                dif.delta.perc = round((delta - i.delta)/(i.delta/100), 1)
                                                                              ), 
                                                                              on = .(county, indicator)][, vjustvar := -3]

CTcntydif[, `:=`(year = 2015, value = Inf, delta = Inf, hjustvar = 3)]



if(do.plots) {
    # Create RG plots
    # creates 3 files: 1. totals, 2. total with same y-scale, 3. delta with same y-scale
    CTtoplot <- names(CTdata.legend)
    CTaggr <- CTaggr[CTdata %in% CTdata.legend[CTtoplot]]
    CTaggr[county == "Kitsap" & RG == "Cities & Towns", `:=`(value = NA, delta = NA)]
    CTaggrdif <- CTaggrdif[CTdata %in% CTdata.legend[CTtoplot]]
    indicators.to.plot <- indicator.names[c(1,2,4)]
    cols <- gg_color_hue(5)
    names(cols) <- CTdata.legend[c("updCT", "hct", "nohct", #"vision", "upd.target", 
                                   "rebased.rgs", "adj.rgs")]
    CTaggr[, text := NA]
          
    # add points to be used for equal scale plots
    CTaggr.tmp <- CTaggr[indicator %in% indicators.to.plot, 
                         .(value = max(value, na.rm = TRUE), delta = max(delta, na.rm = TRUE)), 
                         by = .(indicator, year, county)][, CTdata := "max"]
    CTaggr.tmp <- rbind(CTaggr.tmp, CTaggr[indicator %in% indicators.to.plot, 
                                           .(value = min(value, na.rm = TRUE), delta = min(delta, na.rm = TRUE)), 
                            by = .(indicator, year, county)][, CTdata := "min"])
    
    for(measure in c("value", "delta")) {
        for(same.scale in c(FALSE, TRUE)){
            if(measure == "delta" && same.scale == FALSE) next # do not create delta on different y scale
            pdf(paste0("LUVit_CTs_aggr_", if(measure == "value") "total" else "delta", "-", Sys.Date(), 
                if(same.scale) "_equal_yscale" else "", ".pdf"), 
                    width = 20, height = 10)
            
            for(ind in indicators.to.plot){
                this.col <- cols[names(cols) %in% CTaggr[indicator == ind, CTdata]]
                this.data <- CTaggr[indicator == ind]
                if(measure == "delta") this.data <- this.data[year >= 2020]
                this.data.extra <- CTaggr.tmp[indicator == ind]
                this.data[["measure"]] <- this.data[[measure]]
                this.data.extra[["measure"]] <- this.data.extra[[measure]]
                this.data[, measure := measure / 1000]
                this.data.extra[, measure := measure / 1000]
                ylab <- paste(ind, if(measure == "value") "" else "delta", "in thousands")
                main <- paste(ind, if(measure == "value") "totals" else "growth")
                this.data <- rbind(this.data, data.table(year = 2035, indicator = ind, CTdata = "CT total", county = "Kitsap", RG = "Cities & Towns", 
                                                         measure = NA, text = "Not applicable"), fill = TRUE)
                ag <- ggplot(this.data, aes(x = year, y = measure, group = CTdata, color = CTdata)) +
                    geom_line() + geom_point() + ylab(ylab) + xlab("") + labs(color = NULL) + 
                    scale_color_manual(values=this.col, breaks = names(this.col[names(this.col) != "Total"])) + 
                    facet_wrap(county ~ RG, nrow = 4, scales = "free_y", as.table = TRUE) +
                    geom_text(data = this.data[!is.na(text)], aes(x= year, y = 0, label = text), color = "black", show.legend = FALSE) + 
                    ggtitle(main)
                
                if(same.scale)
                    ag <- ag + geom_point(data = this.data.extra, alpha = 0, show.legend = FALSE)
                
                if(nrow(CTaggrdif[indicator == ind]) > 0) {
                    CTaggrdif[["measure"]] <- CTaggrdif[[measure]]
                    CTaggrdif[["measure.dif"]] <- CTaggrdif[[paste0("dif.", measure)]]
                    CTaggrdif[["measure.perc"]] <- CTaggrdif[[paste0("dif.", measure, ".perc")]]
                    CTaggrdif[, dif.text := paste0("Dif 2050: ", label_comma(accuracy = 10)(measure.dif), " (", round(measure.perc, 1), "%)")]
                    CTaggrdif[county == "Kitsap" & RG == "Cities & Towns", `:=`(dif.text = NA)]
                    
                    ag <- ag + geom_text(data = CTaggrdif[indicator == ind], 
                                         aes(label = dif.text, #hjust = hjustvar, 
                                             vjust = 1.5), size = 3, hjust = 0, color = "darkgrey") 
                }
                print(ag)
            }
            
            # plot counties
            sCTcnty <- CTcnty[CTdata %in% CTdata.legend[CTtoplot]]
            sCTcnty <- sCTcnty[indicator %in% indicators.to.plot][, indicator:= factor(indicator, levels = indicators.to.plot)]
            sCTcnty[["measure"]] <- sCTcnty[[measure]]
            this.col <- cols[names(cols) %in% sCTcnty[, CTdata]]
            
            CTcntydif <- CTcntydif[CTdata %in% CTdata.legend[CTtoplot]]
            CTcntydif[["measure"]] <- CTcntydif[[measure]]
            CTcntydif[["measure.dif"]] <- CTcntydif[[paste0("dif.", measure)]]
            CTcntydif[["measure.perc"]] <- CTcntydif[[paste0("dif.", measure, ".perc")]]
            CTcntydif[, dif.text := paste0("Dif 2050: ", label_comma(accuracy = 10)(measure.dif), " (", round(measure.perc, 1), "%)")]
            sCTcnty[, measure := measure / 1000]
            ylab <- paste(measure, "in thousands")
            main <- paste("Counties'", if(measure == "value") "totals" else "growth")
            agc <- ggplot(sCTcnty, aes(x = year, y = measure, group = CTdata, color = CTdata)) +
                geom_line() + geom_point() + 
                scale_color_manual(values=this.col, breaks = names(this.col[names(this.col) != "Total"])) + 
                ylab(ylab) + xlab("") + labs(color = NULL) +
                geom_text(data = CTcntydif[indicator %in% indicators.to.plot], 
                          aes(label = dif.text, 
                              vjust = 1.5), size = 3, hjust = 0, color = "darkgrey") + 
                facet_wrap(indicator ~ county, nrow = 3, scales = "free_y", as.table = TRUE) +
                ggtitle(main)
            print(agc)
            dev.off()
        }
    }
}
 
# write data into data files
CTaggr2 <- merge(CTaggr, rgs, by = "RG")
CTaggrw <- dcast(CTaggr2, indicator + county + RGID + RG + year ~ CTdata)[order(county, RGID)]
# 
hhs <- CTaggrw[indicator == "households"][, `:=`(indicator = NULL)]
pop <- CTaggrw[indicator == "population"][, indicator := NULL]
hhpop <- CTaggrw[indicator == "HHpopulation"][, indicator := NULL]
emp <- CTaggrw[indicator == "employment"][, indicator := NULL]
# 
if(do.write)
    write.xlsx(list(Households = hhs, HHpopulation = hhpop, Population = pop, Employment = emp), 
               paste0("CTaggr_tables-", Sys.Date(), ".xlsx"), colNames = TRUE)

# 
CTw <- dcast(CT, indicator + county + RGID + RG + control_id + name + year ~ CTdata)[order(county, RGID, control_id)]
# 
hhs <- CTw[indicator == "households"][, `:=`(indicator = NULL)]
pop <- CTw[indicator == "population"][, indicator := NULL]
hhpop <- CTw[indicator == "HHpopulation"][, indicator := NULL]
emp <- CTw[indicator == "employment"][, indicator := NULL]
# 
if(do.write)
     write.xlsx(list(Households = hhs, HHpopulation = hhpop, Population = pop, Employment = emp), 
            paste0("CTdraft_tables-", Sys.Date(), ".xlsx"), colNames = TRUE)
