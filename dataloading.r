suppressMessages(library(sqldf))
##library(gdata)

##Set up working directory
##setwd('/Users/yic1/Desktop/SGX/Off Exchange Trades')
setwd('\\\\PRESIDENT\\PTS\\Product Management - Depository Services\\Securities Pricing\\Automation Project')

source("D:/Off-Exchange/R Scripts/commonfunctions.r")
##create or connect to database
db <- dbConnect(SQLite(), dbname= paste0(dbfolder,"Rawdata.sqlite"))

##Read raw PSMS data
##df_all<-read.table('11062014 mergedCSV.txt',header = FALSE, sep=',')
df_all<-read.table('Input/PSMS/mergedCSV.txt',header = FALSE, sep=',')


if (dim(df_all)[2]==33)
 df_all<-df_all[,-33]

##add column names
names(df_all)<-c("BuySellIndicator","TransactionType","Sender","Receiver","SendersRef", 
"Function","TradeRef","PreviousRef","TradeDate","SettlementDate","DealCcy","DealPrice",                
"SGXSecurityCode","ISIN","SecurityDescription","Quantity","BuyerSeller","ClientAccount",            
"CDPSafeKeepingAccount","PlaceofTrade","PlaceofSettlement","SettlementCcy","SettlementAmt",  
"AccruedInterestCcy","AccruedInterestAmt","Blocktrade","PaymentIndicator","LocalorForeignIndicator",
"StatusID","Status","PSMSMatchedReference","TransferType")

df_all<-df_all[df_all$Status=='SETTLED',]
current_date<-Sys.Date()
cdate<-as.numeric(gsub('-','',current_date))
b_flag<-df_all$SettlementDate < cdate
latest_date<-max(df_all[b_flag,c('SettlementDate')])

##latest_date<-max(df_all$SettlementDate)
##latest_date<-20140611
idx<-with(df_all, SettlementDate==latest_date)
if (sum(idx)==0) stop('No records with settled status in PSMS file')
df_all<-df_all[idx,]

##Add PSMS data to database
##if Table Raw_PSMS doesn't exist,then create and add data to it
##if Table Raw_PSMS exists, check if the data already in the table. If yes, don't load it again.
 if ("Raw_PSMS" %in% dbListTables(db)) {
   sd<-dbGetQuery(db, 'select distinct SettlementDate from Raw_PSMS')
  ##sd<-dbGetQuery(db, 'select max(SettlementDate) from Raw_PSMS')
  ##if (latest_date>sd[1,1])  
  if (!(latest_date %in% sd[,1]))    
    dbWriteTable(conn = db, name = "Raw_PSMS", value = df_all, row.names = FALSE, append=TRUE)
   } else {
  dbWriteTable(conn = db, name = "Raw_PSMS", value = df_all, row.names = FALSE, append=TRUE)
   }

##Add Exchange rate data
rate<-readLines(sprintf("Input/Daily_Exchange_Rates/%s%i.txt",'EXCHANGE_RATE_',latest_date)) 
##temp<-data.frame(data=rate[2:(length(rate)-1)])
rate_df<-read.table(text = rate[2:(length(rate)-1)], sep = "|")
rate_df<-rate_df[,1:3]
rate_df[,1]<-as.numeric(latest_date)
names(rate_df)<-c("SettlementDate","Currency","Rate")
rate_df[,2]<-as.character(rate_df[,2])

##if THB not being in raw file, it would be hard-coded
thai<-c(latest_date, 'THB', 0.0415861)
if (!('THB' %in% rate_df[,2]))
  rate_df<-rbind(rate_df,thai)
rate_df$UpdateTime<-format(Sys.time(), "%b %d %X %Y")

if ("Exchange_rate" %in% dbListTables(db)) {
  sd<-dbGetQuery(db, 'select distinct SettlementDate from Exchange_rate')
 ##sd<-dbGetQuery(db, 'select max(SettlementDate) from Exchange_rate')
 ##if (latest_date>sd[1,1])  
 if (!(latest_date %in% sd[,1])) 
   dbWriteTable(conn = db, name = "Exchange_rate", value = rate_df, row.names = FALSE, append=TRUE)
  } else {
 dbWriteTable(conn = db, name = "Exchange_rate", value = rate_df, row.names = FALSE, append=TRUE)
  }


##Add Last_done_price data to database

closing_price<-readLines(sprintf("Input/Last_Done_Price/%s%i.txt",'LAST_DONE_PRICE_',latest_date)) 
##closingprice_df<-read.table(text = closing_price[2:(length(closing_price)-1)], sep = "|")

x<-closing_price[2:(length(closing_price) - 1)]
for (i in 1:length(x))
 {
  x_vector<-unlist(strsplit(x[i],'|', fixed=TRUE))
  x_vector[4]<-as.numeric(x_vector[4])
  if (i>1) 
   closingprice_df<-rbind(closingprice_df,x_vector)
  else
   closingprice_df<-x_vector
 }
closingprice_df<-as.data.frame(closingprice_df)
rownames(closingprice_df) <- NULL
closingprice_df[,1]<-as.numeric(latest_date)
names(closingprice_df)<-c("SettlementDate","Code","SecurityDescription","Price","Currency","ISIN")
closingprice_df$UpdateTime<-format(Sys.time(), "%b %d %X %Y")

if ("Closing_price" %in% dbListTables(db)) {
 ##sd<-dbGetQuery(db, 'select max(SettlementDate) from Closing_price')
 sd<-dbGetQuery(db, 'select distinct SettlementDate from Closing_price')
 ##if (latest_date>sd[1,1])  
 if (!(latest_date %in% sd[,1])) 
   dbWriteTable(conn = db, name = "Closing_price", value = closingprice_df, row.names = FALSE, append=TRUE)
  } else {
 dbWriteTable(conn = db, name = "Closing_price", value = closingprice_df, row.names = FALSE, append=TRUE)
  }

##close connection to database
dbDisconnect(db)
