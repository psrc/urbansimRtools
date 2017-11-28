# Script for plotting largest differences on FAZ level among multiple runs.
# Intended to explore variations in runs with different random number seed.

library(ggplot2)
library(grid)
library(gridExtra)
library(reshape2)
library(data.table)

# urbansim run directory
rundir <- "/Users/hana/d$/opusgit/urbansim_data/data/psrc_parcel/runs"
# which runs to include 
# (grouped into groups that will be offset on the x-axis)
runs <- list(c("run_8.run_2017_10_31_14_10", "run_11.run_2017_11_13_08_49"), # group I
             c("run_13.run_2017_11_16_15_38", "run_15.run_2017_11_20_15_09", 
               "run_16.run_2017_11_25_15_27") # group II
          )
# how many FAZes to plot
nlargest <- 20

# output file name
outfile <- 'multiple_runs_8_13.pdf'

# If indicators modified, edit the grid.arrange call
# at the end of this script
indicator.names <- c('Households', 'Employment'
                     #, 'Population'
                     )

all.data <- NULL
i <- 1
# Load data for each indicator and run, 
# and compute the max difference by faz
for (ind in indicator.names) {
    ind.name <- tolower(indicator.names[i])
    for(igroup in seq_along(runs)) {
        ind.data.all <- NULL
        for (run.name in runs[[igroup]]) {
            # load data
            rdir <- file.path(rundir, run.name, 'indicators')
            ind.data <- fread(file.path(rdir, paste0('faz__table__', ind.name, '.csv')))[,c('faz_id', paste0(ind.name, '_2040')), with=FALSE]
            indcolname <- strsplit(run.name, "[.]")[[1]][1]
            colnames(ind.data)[2] <- indcolname
            ind.data.all <- if(is.null(ind.data.all)) ind.data else cbind(ind.data.all, ind.data[,indcolname, with=FALSE])
        }
        mrdata <- melt(ind.data.all, id.vars='faz_id')
        # add difference between min and max by faz
        mrdata[, dif := diff(range(value)), by=faz_id]
        mrdata[, group := igroup]
        all.data[[ind]] <- rbind(all.data[[ind]], mrdata)
    }
    # obtain the nlargest fazes with max differences among all groups
    udif <- unique(all.data[[ind]][, .(faz_id, dif, group)])
    udif.max <- udif[, .(max.dif=max(dif)), by=faz_id]
    fidx <- order(udif.max$max.dif, decreasing=TRUE)
    selected.zones <- udif.max[fidx, faz_id][1:nlargest]
    all.data[[ind]] <- subset(all.data[[ind]], faz_id %in% selected.zones)
    # this sets the order of fazes on the x axis 
    all.data[[ind]][, faz_id := factor(faz_id, levels=selected.zones)]
    i <- i+1
}
# generate plots
pd <- position_dodge(.3) # how far apart are the groups
g <- list()
i <- 1
for (ind in indicator.names) {
    g[[ind]] <- ggplot(all.data[[ind]], aes(x=faz_id, y=value, color=variable, group=group)) + 
        geom_point(position=pd, shape=1) + xlab('') + ylab('') + 
        labs(title=indicator.names[i]) + 
        scale_colour_discrete(name = '') + 
        theme(
          legend.position=c(1,1),
          legend.justification=c(1,1), legend.key=element_blank(),
          legend.key.size = unit(0.012, "npc"), plot.title=element_text(size=12), 
          legend.background = element_rect(fill="gray90"),
          plot.margin=unit(c(0.5,0.5,-0.4,0.5), "cm"))
    i <- i+1
}

pdf(outfile, width=11, height=8)
grid.arrange(g[['Households']], 
			#g[['Population']], 
			g[['Employment']],
			ncol=1, heights=rep(1, length(indicator.names))
			)
dev.off()
