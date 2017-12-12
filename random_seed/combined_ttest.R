library(data.table)

# which runs to include (grouped into groups)
runs <- list(
  c("run_8.run_2017_10_31_14_10", "run_11.run_2017_11_13_08_49"), # group I
  c("run_13.run_2017_11_16_15_38", "run_15.run_2017_11_20_15_09", "run_16.run_2017_11_25_15_27"), # group II
  c("run_18.run_2017_11_28_11_27", "run_19.run_2017_11_29_22_07"),
  c("run_12.run_2017_12_04_15_08", "run_13.run_2017_12_05_11_35", "run_14.run_2017_12_05_11_35")
)
# which groups to test (select two groups)
groups.to.test <- c(2,3)

# urbansim run directories for each group
dir.modelsrv8 <- "/Volumes/d$-1/opusgit/urbansim_data/data/psrc_parcel/runs"
dir.modelsrv6 <- "/Volumes/d$/opusgit/urbansim_data/data/psrc_parcel/runs"
#rundir <- "/Users/hana/d$/opusgit/urbansim_data/data/psrc_parcel/runs"
# order of the directories should correspond to the order of groups in "runs"
rundirs <- c(dir.modelsrv8, dir.modelsrv8, dir.modelsrv8, dir.modelsrv6)

indicator.names <- c('Households', 'Employment'
                     #, 'Population'
                      )

all.data <- NULL
i <- 1
# Load data for each indicator and run
for (ind in indicator.names) {
  ind.name <- tolower(indicator.names[i])
  for(igroup in seq_along(runs)) {
    ind.data.all <- NULL
    for (run.name in runs[[igroup]]) {
      # load data
      rdir <- file.path(rundirs[igroup], run.name, 'indicators')
      ind.data <- fread(file.path(rdir, paste0('faz__table__', ind.name, '.csv')))[,c('faz_id', paste0(ind.name, '_2040')), with=FALSE]
      colnames(ind.data)[2] <- "value"
      ind.data.all <- rbind(ind.data.all, ind.data)
    }
    mrdata <- ind.data.all
    mrdata[, group := igroup]
    all.data[[ind]] <- rbind(all.data[[ind]], mrdata)
  }
  i <- i+1
}

ttest <- function(groups) {
  do.ttest <- function(faz) {
    d <- subset(data, faz_id == faz)
    if(all(d$value == d$value[1])) return(c())
    return(c(faz, t.test(value ~ group, data=d, var.equal = TRUE)$p.value))
  }
  
  pv.df <- NULL
  for (ind in names(all.data)) {
    fazes <- unique(all.data[[ind]]$faz_id)
    data <- subset(all.data[[ind]], group %in% groups)
    p.values <- do.call(rbind, lapply(fazes, do.ttest))
    colnames(p.values) <- c("faz_id", "p.value")
    pv.df <- rbind(pv.df, cbind(data.frame(p.values), indicator=ind))
  }
  return(data.table(pv.df))
}

fisher <- function(p.values) {
  chisq <- -2 * sum(log(p.values))
  return(list(chisq = chisq, p=pchisq(chisq, df=2*length(p.values), lower.tail=FALSE)))
}

fisher.by.ind <- function(df, ind, level=0.05) {
  d <- subset(pvdf, indicator==ind)
  ft <- fisher(d$p.value)
  sortd <- d[order(d$p.value),]
  sortd <- subset(sortd, p.value < level)
  return(c(ft, list(fazes=sortd)))
}

pvdf <- ttest(groups.to.test)
ft <- fisher(pvdf$p.value)
ft.hh <- fisher.by.ind(pvdf, "Households")
ft.jobs <- fisher.by.ind(pvdf, "Employment")

ft
ft.hh
ft.jobs

