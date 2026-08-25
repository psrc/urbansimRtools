library(data.table)
library(openxlsx)
library(ggplot2)
library(scales)
library(RMySQL)

setwd("~/psrc/R/urbansimRtools/control_totals")

## Functions
###############
# Connecting to Mysql
mysql.connection <- function(dbname = "2023_parcel_baseyear") {
    # credentials can be stored in a file (as one column: username, password, host)
    if(file.exists("creds.txt")) {
        creds <- read.table(".creds.txt", stringsAsFactors = FALSE)
        un <- creds[1,1]
        psswd <- creds[2,1]
        if(nrow(creds) > 2) h <- creds[3,1] 
        else h <- .rs.askForPassword("host:")
    } else {
        un <- .rs.askForPassword("username:")
        psswd <- .rs.askForPassword("password:")
        h <- .rs.askForPassword("host:")
    }
    dbConnect(MySQL(), user = un, password = psswd, dbname = dbname, host = h)
}

CT1db <- "sandbox_james"
CT2db <- "2023_parcel_baseyear_rtp_update"
    
CTnames <- c("new", "old")
xwalkDB <- "2023_parcel_baseyear_luvit_update_scenario"

hhtbl.name <- "annual_household_control_totals"
emptbl.name <- "annual_employment_control_totals"


# load CTs
mydb <- mysql.connection(CT1db)
qr <- dbSendQuery(mydb, paste0("select * from ", hhtbl.name))
CThh1 <- data.table(fetch(qr, n = -1))
dbClearResult(qr)
qr <- dbSendQuery(mydb, paste0("select * from ", emptbl.name))
CTemp1 <- data.table(fetch(qr, n = -1))
dbClearResult(qr)
dbDisconnect(mydb)

mydb <- mysql.connection(CT2db)
qr <- dbSendQuery(mydb, paste0("select * from ", hhtbl.name))
CThh2 <- data.table(fetch(qr, n = -1))
dbClearResult(qr)
qr <- dbSendQuery(mydb, paste0("select * from ", emptbl.name))
CTemp2 <- data.table(fetch(qr, n = -1))
dbClearResult(qr)
dbDisconnect(mydb)

mydb <- mysql.connection(xwalkDB)
qr <- dbSendQuery(mydb, paste0("select * from control_hcts"))
subregs <- data.table(fetch(qr, n = -1))
dbClearResult(qr)
qr <- dbSendQuery(mydb, paste0("select * from controls"))
controls <- data.table(fetch(qr, n = -1))
dbClearResult(qr)

datHH <- rbind(CThh2[, name := CTnames[2]], CThh1[, name := CTnames[1]])
datHH <- datHH[, .(value = sum(total_number_of_households)), by = .(year, subreg_id, name)]
datEmp <- rbind(CTemp2[, name := CTnames[2]], CTemp1[, name := CTnames[1]])
datEmp <- datEmp[, .(value = sum(total_number_of_jobs)), by = .(year, subreg_id, name)]

dat <- rbind(datHH[, indicator := "Households"][subreg_id > 0], 
             datEmp[, indicator := "Employment"][subreg_id > 0])

dat[ , control_id := subreg_id]
dat[control_id > 1000, control_id := control_id - 1000]
dat[, type := ifelse(subreg_id > 1000, paste(name, "- HCT"), 
                      paste(name, "- non HCT"))]
# get names
dat <- merge(dat, subregs, by.x = "subreg_id", by.y = "control_hct_id")
dat <- merge(dat, controls, by = "control_id")
dat <- dat[order(control_id, subreg_id)]
dat[, control_name2 := paste(control_id, control_name)]



nrows <- 4
pdf(paste0("CTs_comp-", Sys.Date(),  ".pdf"), width = 10, height = 9)

for(row in seq(1, nrow(controls), by = nrows)){
    g <- ggplot(dat[control_id %in% controls[row:(row + nrows - 1), control_id]], 
                aes(x = year, y = value, group = type, color = type)) +
        geom_line() + geom_point() + ylab("") + 
        facet_wrap(reorder(control_name2, -control_id) ~ indicator, ncol = 2, scales = "free_y", as.table = FALSE) + 
        #scale_color_manual(values=this.col, breaks = names(cols)) + 
        scale_y_continuous(labels = scales::comma)
    print(g)
}
dev.off()

