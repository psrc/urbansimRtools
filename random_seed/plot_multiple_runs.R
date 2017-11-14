# Script for plotting largest differences on FAZ level among multiple runs.
# Intended to explore variations in runs with different random number seed.

library(ggplot2)
library(grid)
library(gridExtra)
library(reshape2)

# urbansim run directory
rundir <- "/Users/hana/d$/opusgit/urbansim_data/data/psrc_parcel/runs"
# which runs to include
runs <- c("run_8.run_2017_10_31_14_10", 
          #"run_9.run_2017_11_06_10_48", 
          #"run_10.run_2017_11_08_11_58", 
          "run_11.run_2017_11_13_08_49"
          )
# how many FAZes to plot
nlargest <- 20

# output file name
outfile <- 'multiple_runs_8_11.pdf'

# Should points at the same position be overplotted (TRUE) or shifted (FALSE)
overplot.points <- TRUE

indicator.names <- c('Households', 'Employment', 'Population')
all.data <- NULL
i <- 1
for (ind in indicator.names) {
	ind.data.all <- NULL
	for (run.name in runs) {
		rdir <- file.path(rundir, run.name, 'indicators')
		ind.name <- tolower(indicator.names[i])
		ind.data <- read.table(file.path(rdir, paste0('faz__table__', ind.name, '.csv')), header=TRUE, sep=",")[,c('faz_id', paste0(ind.name, '_2040'))]
		indcolname <- strsplit(run.name, "[.]")[[1]][1]
		colnames(ind.data)[2] <- indcolname
		ind.data.all <- if(is.null(ind.data.all)) ind.data else cbind(ind.data.all, ind.data[,indcolname, drop=FALSE])
	}
	d <- data.frame(faz_id=ind.data.all$faz_id, dif=apply(apply(ind.data.all[,-1], 1, range), 2, diff))
	fidx <- order(as.numeric(d$dif), decreasing=TRUE)[1:nlargest]
	mrdata <- melt(cbind(ind.data.all[fidx,], order=1:length(fidx)), id.vars=c('faz_id', 'order'))
	mrdata$faz_id <- reorder(mrdata$faz_id, mrdata$order)
	all.data[[ind]] <- mrdata
	i <- i+1
}

pd <- position_dodge(.2 * (overplot.points == FALSE))
g <- list()
i <- 1
for (ind in indicator.names) {
    g[[ind]] <- ggplot(all.data[[ind]], aes(x=faz_id, y=value, color=variable)) + 
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
			g[['Population']], 
			g[['Employment']],
			ncol=1, heights=c(1,1,1)
			)
dev.off()
