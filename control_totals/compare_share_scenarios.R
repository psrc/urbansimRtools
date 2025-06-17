# Compare different outputs of the split_ct_to_hct.R script
#

library(data.table)
library(readxl)

files <- list(LUVit = "outputs/LUVit_ct_by_tod_generator_90-90-90_2022-11-22.xlsx",
              Base23 = "outputs/LUVit_ct_by_tod_generator-2025-06-17_90-90-90.xlsx",
              HB1110 = "outputs/LUVit_ct_by_tod_generator_hb1110-2025-06-17_90-90-90.xlsx"
              )
RGdf <- data.table(RGid = c(1:3, -1), RG = c("Metro", "Core Cities", "HCT Comm", "Region"))

dt <- NULL
for(s in names(files)){
    for(ind in c("HH", "EMP")){
        dat <- data.table(read.xlsx(files[[s]], sheet = paste0(ind, "work")))
        if(! "geo_id" %in% colnames(dat))
            colnames(dat)[2] <- "geo_id"
        colnames(dat) <- gsub("DU|EMP", "", colnames(dat))
        dt <- rbind(dt, dat[is_tod == TRUE, .(subreg_id, geo_id, name, RGid, start=capshare, end=target_share, scenario = s, indicator = ind)])
    }
}
dt <- merge(dt, RGdf)

# bar chart of starting and final shares
main.bar <- 90
share <- 10

pdf(file.path("outputs", paste0("hct_shares_3scenarios-", Sys.Date(), ".pdf")), width = 20, height = 8)


for(ind in c("HH", "EMP")){

    datbar <- melt(dt[indicator == ind], measure.vars = c("start", "end"))[, `:=`(max_share = 100-share)]
    
    datbar[name == "Bothell" & geo_id == 126, name := "Bothell (Sno)"]
    datbar[startsWith(name, "Mid-County"), name := "Uninc. Pierce HCT"]
    #datbar[, name_short := substr(gsub(" ", "", name), 1, 8)]
    sortdat <- datbar[max_share == main.bar & variable == "start" & scenario == "LUVit"]
    sortdat$name <- reorder(sortdat$name, sortdat$value, decreasing = TRUE)
    #sortdat$name_short <- reorder(sortdat$name_short, sortdat$value, decreasing = TRUE)
    datbar[variable == "start", `:=`(what = "capacity")]
    datbar[variable == "end", `:=`(what = paste("max:", max_share))]
    datbar[datbar[variable == "start"], value := ifelse(variable == "start", value, value - i.value), on = c("geo_id", "scenario")]
    datbar <- datbar[order(value, decreasing = TRUE)]
    datbar[, what := factor(what, levels = c("max: 95", "max: 90", "capacity"))]
    datbar[, name := factor(name, levels = levels(sortdat$name))]
    #datbar[, name_short := factor(name_short, levels = levels(sortdat$name_short))]
    datbar[, scenario := factor(scenario, levels = names(files))]
    
    for(rgname in c("Metro", "Core Cities", "HCT Comm")){
        gbar <- ggplot(data = datbar[RG == rgname]) + 
            geom_bar(position="stack", stat="identity", aes(x = scenario, y = value, fill = what)) + 
            scale_x_discrete(guide = guide_axis(angle = 90)) + ylim(0, 100) +
            scale_fill_discrete(name = "HCT shares") + xlab("") + ylab("HCT shares") +
            guides(fill = guide_legend(override.aes = list(shape = c(NA, NA) ), order = 1 )) +
            ggtitle(paste(rgname, list(HH = "Households", EMP = "Employment")[[ind]])) #+
        #theme(strip.placement = "outside",
        #      strip.background = element_rect(fill = NA, color = "white"),
        #      panel.spacing = unit(-.01,"cm"))
        if(rgname == "HCT Comm") {
            gbar <- gbar + facet_grid(. ~ name, scale = "free_x", space = "free")+
                theme(strip.text = element_text(angle = 80))
        } else gbar <- gbar + facet_grid(. ~ name, scale = "free_x", space = "free")
        print(gbar)
    }
}
dev.off()
