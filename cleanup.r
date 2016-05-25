suppressMessages(library(sqldf))

##Set up database directory
setwd('//President/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/')

##directory for database
dbfolder<-paste0(getwd(),"/database/")

## function f_cleanup to keep nday days of records in db
f_cleanup<-function(nday) {
cday <- Sys.Date()

bday<-as.numeric(gsub('-','',cday-nday))

db <- dbConnect(SQLite(), dbname=paste0(dbfolder,"Rawdata.sqlite"))

##clean up Table output_daily
if ("output_daily" %in% dbListTables(db))
{ 
  dbSendQuery(db, paste('Delete from output_daily where SettlementDate < ', bday))
}

##clean up Table Raw_PSMS
if ("Raw_PSMS" %in% dbListTables(db)) 
{
  dbSendQuery(db, paste('Delete from Raw_PSMS where SettlementDate < ', bday))
}

dbClearResult(dbListResults(db)[[1]])


dbDisconnect(db)

}

f_cleanup(62)


