suppressMessages(library(xlsx))
suppressMessages(library(sqldf))
suppressMessages(library(plyr))

#directory for autorun - daily detailed report (for all)
detailedallfolder<-'//PRESIDENT/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/Output/Daily_Detailed_Report(All_Settlements)/'

#directory for autorun - daily detailed report (off-exchange only)
detailedoffexchangefolder<-'//PRESIDENT/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/Output/Daily_Detailed_Report(Off-Exchange)/'

dailysummaryfolder<-'//PRESIDENT/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/Output/Daily_Summary_Report/'

weeklysummaryfolder<-'//PRESIDENT/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/Output/Weekly_Summary_Report/'

monthlysummaryfolder<-'//PRESIDENT/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/Output/Monthly_Summary_Report/'

##directory for manual run
manualinputfolder<-'//PRESIDENT/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/Input/Manual_Run/'
manualfolder<-'//PRESIDENT/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/Output/Manual_Run_Reports/'

##directory for database
dbfolder<-'//PRESIDENT/PTS/Product Management - Depository Services/Securities Pricing/Automation Project/database/'

##manualfolder<-'/Users/yic1/Desktop/SGX/Off Exchange Trades/manualrun/'
## Getting the first day of month

som <- function(x) {
     as.Date(format(x, "%Y-%m-01"))
  }


##Function for individual linking (refer to definition of business rules)

individual_linking<-function(A, B)
{
  if (nrow(B)==0 | nrow(A)==0) return(NULL)
  
  row_id<-c()
  b_index<-c()
  total_A<-sum(A$Quantity)
  
  for (m in 1:nrow(B)) {
    if (nrow(A)==0) break 
    
    for (n in 1:nrow(A)) 
      if (B[m,c('Quantity')] == A[n,c('Quantity')]) {
        row_id<-c(row_id, row.names(B[m,]))
        b_index<-c(b_index, m)
        A<-A[-n,]
        break
      } 
  }
  
  if (nrow(A) != 0) {
    T_quantity<-sum(A$Quantity)
    
    if (!(is.null(b_index))) B<-B[-b_index,]
    B_sort<-B[order(-B$Quantity),]
    
    for (i in 1:nrow(B_sort)) 
      if (T_quantity>=B_sort[i,c('Quantity')]) {
        row_id<-c(row_id, row.names(B_sort[i,]))
        T_quantity<-T_quantity-B_sort[i,c('Quantity')]
      }
  }
  row_id
}

procEle_sub <- function(df1, df2) {
  if (nrow(df1)==0 | nrow(df2)==0) {  
    ret_t <- c()      
    ret_i <- c()
  } else {
    if (sum(df1$Quantity)>=sum(df2$Quantity)) { 
      ret_t <- row.names(df2) 
      ret_i <- c()
    } else {
      ret_t <- c()
      ret_i <- individual_linking(df1,df2) 
    }
  }
  list(ret_t, ret_i)  
}

procEle <- function(df_list) {
  
  df_A<-df_list[(df_list$flag==1 & df_list$BuySellIndicator=='B'),]
  df_B<-df_list[(df_list$flag==0 & df_list$BuySellIndicator=='S'),]
  ret <- procEle_sub(df_A, df_B)
  
  df_C<-df_list[(df_list$flag==1 & df_list$BuySellIndicator=='S'),]
  df_D<-df_list[(df_list$flag==0 & df_list$BuySellIndicator=='B'),]
  ret2 <- procEle_sub(df_C, df_D)
  
  mapply(c,ret,ret2,SIMPLIFY=FALSE)
}

## Function to reclassify transfer type vector
transfertype_reclass <- function(transfertype_vec) {
  myTradeRelated<-transfertype_vec=='On-Exchange'
  myOffExchange<-transfertype_vec =='Off-Exchange'
  myMarketRelated<-transfertype_vec %in% c('SBLT', 'CONV', 'COLI', 'COLO', 'ETFT', 'SECB', 'SECL')
  myOwnAccount<-transfertype_vec %in% c('OWNI', 'JONT')
  myIssuerRelated<-transfertype_vec %in% c('ISSU', 'PLAC')
  
  transfertype_vec[myTradeRelated]<-'Trade-related'
  transfertype_vec[myOffExchange]<-'Off-exchange'
  transfertype_vec[myMarketRelated]<-'Market-related'
  transfertype_vec[myOwnAccount]<-'Own account'
  transfertype_vec[myIssuerRelated]<-'Issuer-related'
  
  transfertype_vec
}

overwrite <- function(df) {
  
  if (!is.null(df) && nrow(df) > 0) {
    plOfTadeOrdered <- c("OTCO", "XSES", "XLON", "")
    transferTypeOrdered <- c("OWNI", "JONT", "ISSU", "PLAC",
      "ETFT", "CONV", "SECB", "SECL", "COLI", "COLO", "TRAD", "", "SBLT")
    df$PlaceofTrade <- factor(df$PlaceofTrade, levels = plOfTadeOrdered)
    df$TransferType <-
      factor(df$TransferType, levels = transferTypeOrdered)
    
    o <- order(df$PSMSMatchedReference, df$PlaceofTrade, df$TransferType)
    
    prevRef <- -1
    replacement <- NULL
    
    for (i in 1:nrow(df)) {
      if (df[o[i], "PSMSMatchedReference"] == prevRef) {
        if (df[o[i], "TransactionType"] == 'FDVP') {
          df[o[i], "PlaceofTrade"] <- replacement$PlaceofTrade
        }
        else if (df[o[i], "TransactionType"] == "OFOP") {
          # replace Place of Tade & Transfer Type
          df[o[i], c("PlaceofTrade", "TransferType")] <- replacement
        }
      }
      else {
        prevRef <- df[o[i], "PSMSMatchedReference"]
        replacement <- df[o[i], c("PlaceofTrade", "TransferType")]
      }
    }
    
    # Convert factor columns back to string
    facCols <- c("PlaceofTrade", "TransferType")
    df[, facCols] <- sapply(df[, facCols], as.character)
  }

  return(df)
}

##Function for generation of Daily Detailed Report
Daily_Detailed_Report<-function(sd, manual=FALSE) {
  
  db <- dbConnect(SQLite(), dbname=paste0(dbfolder,"Rawdata.sqlite"))
  
  df_all<-dbGetQuery(db, paste('select * from Raw_PSMS where SettlementDate =', sd))
  ##exclude DA code= '103'
  excl_da<-"103"
  df_all<-df_all[df_all$Sender != excl_da & df_all$Receiver != excl_da, ]
  if (nrow(df_all)==0) stop('There is no relevant data in Table Raw_PSMS!')
  
  ##convert lowercase to uppercase
  df_all$BuySellIndicator<-toupper(df_all$BuySellIndicator)
  
  x_rate<-dbGetQuery(db, paste('select Currency, rate from Exchange_rate where SettlementDate =', sd))
  if (nrow(x_rate)==0) stop('There is no relevant data in Table Exchange_rate!')
  
  ##last_price<-dbGetQuery(db, paste('select ISIN, Price from Closing_price where SettlementDate =', sd))
  last_price<-dbGetQuery(db, paste('select ISIN, Price, Currency as Trd_Currency from Closing_price where SettlementDate =', sd))
  if (nrow(last_price)==0) stop('There is no relevant data in Table Closing_price!')
  
  da<-dbGetQuery(db, 'select Sender, DaName from Depository_Agent')
  if (nrow(da)==0) stop('Table Depository_Agent is empty!')
  
  sub_account<-dbGetQuery(db, 'select CDPSafeKeepingAccount,CDPAccountName, Location from Sub_Account')  
  if (nrow(sub_account)==0) stop('Table Sub_Account is empty!')
  
  # Close connection
  dbDisconnect(db)
  
  ##Excluding DCSS type
  df_DCSS<-df_all[df_all$TransactionType=='DCSS',]
  df_all<-df_all[df_all$TransactionType!='DCSS',]
  a<-nrow(df_all)
  if (a==0) stop('Invalid PSMS Data! Please have a check')
  row.names(df_all)<-1:a
  if (nrow(df_DCSS)>0) row.names(df_DCSS)<-(a+1) : (a+dim(df_DCSS)[1])
  
  df_all <- within(df_all, {
    subsequent <- ifelse(TransactionType %in% c("FDVP", "OFOP"), 1, 0)
    Code_CDP <- paste(SGXSecurityCode, CDPSafeKeepingAccount, sep='.')
    SubsequentBuySell <- paste(subsequent, BuySellIndicator, sep='.')
  })
  
  if (nrow(df_DCSS)>0) {
    df_DCSS <- within(df_DCSS, {
      subsequent <- 'DCSS'
      Code_CDP <- NA
      SubsequentBuySell <- NA
    })
  }
  
  ## perform overwrite for 2nd settlements (subsequent = 1)
  selectCols<-c("PSMSMatchedReference", "PlaceofTrade",
                "TransactionType", "TransferType")
  df_all[df_all$subsequent==1, selectCols] <- overwrite(df_all[df_all$subsequent==1, selectCols])
  
  df_all$flag<-0
  if (nrow(df_all[df_all$subsequent==0,])>0) df_all[df_all$subsequent==0,]$flag<-1
  
  ## Flag OTOC/TRAD as off-exchange
  conds<-df_all$PlaceofTrade=='OTCO' & df_all$TransferType=='TRAD'
  if (nrow(df[conds,])>0) df_all[conds,]$flag<-'Off-Exchange'
  
  if (nrow(df_DCSS)>0) df_DCSS$flag<-1
  
  ## Copy Transfer Type to flag for all non TRAD records
  transferTypeToLink <- df_all$TransferType %in% c("TRAD", "")
  df_all[ !transferTypeToLink ,]$flag<-as.character(df_all[ !transferTypeToLink ,]$TransferType)
  
  ## select records with cdp account number, transfer type = 'TRAD' 
  ## or blank (for backward compatibility) and exclude already identified
  ## off-exchange trades

  df_sub<-subset(df_all, transferTypeToLink & CDPSafeKeepingAccount!=''
                 & flag != 'Off-Exchange')
  
  ##Add one more column to keep matching details
  df_all$comments<-'null'
  
  if (nrow(df_DCSS)>0) df_DCSS$comments<-'null'
  
  aggdata <-aggregate(df_sub$Quantity, by=list(df_sub$Code_CDP,df_sub$SubsequentBuySell), FUN=sum)
  
  names(aggdata)<-c('Code_CDP','SubsequentBuySell','TotalQuantity')
  aggdata[,1]<-as.factor(aggdata[,1])
  aggdata[,2]<-as.factor(aggdata[,2])
  aggdata.tab<-xtabs(TotalQuantity~Code_CDP + SubsequentBuySell, aggdata)
  
  ##Total quantity of first settlement is more than or equal to that of subsequent settlement
  row_index<-c()
  index<-c()
  codecdp.0b<-row.names(aggdata.tab[aggdata.tab[,1]>=aggdata.tab[,4],])
  #df.0b<-data.frame(Code_CDP=row.names(aggdata.tab[aggdata.tab[,1]>=aggdata.tab[,4],]),SubsequentBuySell='1.S')   
  idx<-with(df_sub,SubsequentBuySell=='1.S')
  df_sub[idx,]$flag<-ifelse(df_sub[idx,]$Code_CDP %in% codecdp.0b,1,0) 
  #df_sub[df_sub$SubsequentBuySell=='1.S',]$flag<-ifelse((df_sub[df_sub$SubsequentBuySell=='1.S',]$Code_CDP %in% codecdp.0b),1,0)  
  ##row_index<-row.names(df_sub[(df_sub$Code_CDP %in% codecdp.0b) & df_sub$SubsequentBuySell=='1.S',])
  if (nrow(df_sub[idx&df_sub$flag==1,])>0)
    row_index<-row.names(df_sub[idx&df_sub$flag==1,])
  
  codecdp.0s<-row.names(aggdata.tab[aggdata.tab[,2]>=aggdata.tab[,3],])
  idy<-with(df_sub,SubsequentBuySell=='1.B')
  df_sub[idy,]$flag<-ifelse(df_sub[idy,]$Code_CDP %in% codecdp.0s,1,0)
  if(nrow(df_sub[idy&df_sub$flag==1,])>0)
    index<-row.names(df_sub[idy&df_sub$flag==1,])
  ##index<-row.names(df_sub[(df_sub$Code_CDP %in% codecdp.0s) & df_sub$SubsequentBuySell=='1.B',])
  
  row_index<-c(row_index,index)
  
  if (!(is.null(row_index))) {
    df_all[row_index,]$flag<-1
    df_all[row_index,]$comments<-'Total amount linking'
  }
  ##For those subsequent settlements whose total quantity is more than that of first settlements, do individual linking
  
  temp.code.cdp<-row.names(aggdata.tab[aggdata.tab[,2]<aggdata.tab[,3],])
  ##linking_raw<-df_sub[(df_sub$SubsequentBuySell %in% c('0.S','1.B')) & (df_sub$Code_CDP %in%  temp.code.cdp),]
  idr<-with(df_sub, SubsequentBuySell %in% c('0.S','1.B') & Code_CDP %in% temp.code.cdp )
  linking_raw<-df_sub[idr,]
  
  mylist<-split(linking_raw , linking_raw$Code_CDP)
  
  row_index<- c()
  for (i in 1:length(mylist))
  {
    df_list<- mylist[[i]]
    df_A<-df_list[df_list$SubsequentBuySell=='0.S',]
    df_B<-df_list[df_list$SubsequentBuySell=='1.B',]
    row_index<-c(row_index,individual_linking(df_A,df_B))
  } 
  
  temp.code.cdp.1s<-row.names(aggdata.tab[aggdata.tab[,1]<aggdata.tab[,4],])
  ids<-with(df_sub, SubsequentBuySell %in% c('0.B','1.S') & Code_CDP %in% temp.code.cdp.1s)
  linking_raw_1s<-df_sub[ids,]
  ##linking_raw_1s<-df_sub[(df_sub$SubsequentBuySell %in% c('0.B','1.S')) & (df_sub$Code_CDP %in%  temp.code.cdp.1s),]
  mylist.1s<-split(linking_raw_1s , linking_raw_1s$Code_CDP)
  
  for (i in 1:length(mylist.1s))
  {
    df_list<- mylist.1s[[i]]
    df_A<-df_list[df_list$SubsequentBuySell=='0.B',]
    df_B<-df_list[df_list$SubsequentBuySell=='1.S',]
    row_index<-c(row_index,individual_linking(df_A,df_B))
  }
  
  if (!(is.null(row_index))) {
    df_all[row_index,]$flag<-1
    df_all[row_index,]$comments<-'Individual linking'
  }
  ##rm(list = c("mylist", "mylist.1s"))
  
  ###PSMS reference matching
  ida<-with(df_all, flag==1 & subsequent==1)
  idb<-with(df_all, flag==0 & subsequent==1)
  reference.no<-df_all[ida,]$PSMSMatchedReference
  ##reference.no<-df_all[(df_all$flag==1) & (df_all$subsequent==1),]$PSMSMatchedReference
  
  if (sum(idb)>0) {
    df_all[idb,]$comments<-ifelse(df_all[idb,]$PSMSMatchedReference %in% reference.no, 'PSMS reference matching', 'NULL')
    ##df_all[(df_all$flag==0) & (df_all$subsequent==1),]$comments<-ifelse(df_all[(df_all$flag==0) & (df_all$subsequent==1),]$PSMSMatchedReference %in% reference.no,'PSMS reference matching', 'NULL')
    df_all[idb,]$flag<-ifelse(df_all[idb,]$PSMSMatchedReference %in% reference.no, 1, 0)
    ##df_all[(df_all$flag==0) & (df_all$subsequent==1),]$flag<-ifelse(df_all[(df_all$flag==0) & (df_all$subsequent==1),]$PSMSMatchedReference %in% reference.no, 1, 0)
  }
  ##sum((df_all$flag==1) & (df_all$subsequent==1))
  ##sum((df_all$flag==0) & (df_all$subsequent==1))
  
  ##Second Turnaround starting off...
  
  iteration<-1
  
  ##relevant_idxs <- with(df_all, subsequent==1 & TransferType!='SBLT')
  relevant_idxs <- with(df_all, subsequent==1 & flag %in% c(0, 1))
  split_grps <- df_all[relevant_idxs,]$Code_CDP
  
  repeat { 
    if (iteration > 10000) stop('Iteration goes beyond 10000!')
    mylist <- split(df_all[relevant_idxs,], split_grps)
    result_list<-lapply(mylist, procEle)    # this can be parallelised with mclapply if useful
    row_index <- unlist(sapply(result_list, function(lx) lx[[1]] ))
    ##unlist(result_list[[1]])    
    ind_index <- unlist(sapply(result_list, function(lx) lx[[2]] ))
    
    if (is.null(row_index) & is.null(ind_index)) break
    
    iteration <- iteration+1
    
    if (!(is.null(row_index))) {
      df_all[row_index,]$flag <- 1
      df_all[row_index,]$comments <- paste('Total amount linking at Turnaround', iteration,'')
    }
    
    if (!(is.null(ind_index))) {
      df_all[ind_index,]$flag <- 1
      df_all[ind_index,]$comments <- paste('Individual linking at Turnaround', iteration,'')
    }
    
    reference.no <- df_all[(df_all$flag==1) & (df_all$subsequent==1),]$PSMSMatchedReference
    
    idxs <- with(df_all, flag==0 & subsequent==1)
    in_ref_idxs <- df_all[idxs,]$PSMSMatchedReference %in% reference.no
    
    df_all[idxs,]$flag <- ifelse(in_ref_idxs, 1, 0)
    df_all[idxs,]$comments <- ifelse(in_ref_idxs, paste('PSMS reference matching at Turnaround', iteration,''), 'NULL')
  }
  
 ## sum((df_all$flag==1) & (df_all$subsequent==1))
 ##  sum((df_all$flag==0) & (df_all$subsequent==1))
  
  
  
  if (nrow(df_DCSS)>0) df_all<-rbind(df_all, df_DCSS)
  df_all$NotReported<-NA
  df_all$Currency <-as.character(df_all$SettlementCcy)
  df_all$Quantity<-as.numeric(df_all$Quantity)
 
 # Replace Currency with trading currency from Closing_Price table
 idx<-with(df_all, TransactionType=='OFOP')
 if (sum(idx)>0)  {
   df_tmp<-join(df_all[idx,], last_price, by="ISIN",type="left")
   df_all[idx,]$Currency <- as.character(df_tmp$Trd_Currency)
 }
  
  #adding rate
  df_all<-join(df_all,x_rate,by='Currency',type = "left") 
  if (sum(df_all$Currency=='SGD') > 0)  df_all[df_all$Currency=='SGD',]$Rate<-1
  df_all$Rate<-as.numeric(df_all$Rate)
  
  #adding last_done_price
  last_price<-subset(last_price, select = -Trd_Currency)
  df_all<-join(df_all,last_price,by='ISIN',type = "left") 
  df_all$Price<-as.numeric(df_all$Price)
  
  #adding da name
  df_all<-join(df_all,da,by='Sender',type = "left") 
  
  
  #add subaccount name
  df_all<-join(df_all,sub_account,by='CDPSafeKeepingAccount',type = "left") 
  
  df_all$SettlementValue_BaseCurrency<-df_all$SettlementAmt
  df_all$SettlementValue_SGD<-df_all$SettlementValue_BaseCurrency*df_all$Rate
  
  idx<-with(df_all, TransactionType=='OFOP')
  if (sum(idx)>0)  {
    p<-df_all[idx,]$Price
    q<-df_all[idx,]$Quantity
    df_all[idx,]$SettlementValue_BaseCurrency<-p*q 
    # b<-df_all[idx,]$SettlementValue_BaseCurrency
    r<-df_all[idx,]$Rate
    df_all[idx,]$SettlementValue_SGD<-p*q*r
  }
  
  df_all$NewFee<-0
  
  idx<- with(df_all, TransactionType=='CDIS' & (Sender==561 | Sender==635 | Sender==690)) 
  df_all$NewFee[idx]<-0.5
    ##df_all[idx,]$NewFee<-0.5
    
  idx<- with(df_all, TransactionType=='FOPT' & CDPSafeKeepingAccount!='')
  df_all$NewFee[idx]<-0.5
  
  idx<- with(df_all, TransactionType=='DVPT' & CDPSafeKeepingAccount!='')
  df_all$NewFee[idx]<-3.5
  
  
  idx<- with(df_all, TransactionType=='DVPT' & CDPSafeKeepingAccount=='')
  df_all$NewFee[idx]<-2.5
  
  idx<- with(df_all, TransactionType=='FDVP' & flag==1)
  df_all$NewFee[idx]<-33
  
  idx<-with(df_all, TransactionType=='FDVP'&flag==0)
  ty<-3 + 0.00015 * df_all$SettlementValue_SGD[idx]
  df_all$NewFee[idx]<-ifelse(ty>78, ty, 78)
  
  ##df_all[(df_all$TransactionType=='FDVP'&df_all$flag==0),]$NewFee<-ifelse(3 + 0.00015 * df_all[(df_all$TransactionType=='FDVP'&df_all$flag==0),]$SettlementValue_SGD>75, 3 + 0.00015 * df_all[(df_all$TransactionType=='FDVP'&df_all$flag==0),]$SettlementValue_SGD,75)
  
  idx<-with(df_all,TransactionType=='OFOP'&flag==0)
  ty<- 0.00015 * df_all$SettlementValue_SGD[idx]
  df_all$NewFee[idx]<-ifelse(ty>75, ty, 75)
  
  idx<-with(df_all,TransactionType=='OFOP' & flag!=0 & BuySellIndicator=='S')
  tepfee<-round_any(0.01* df_all$Quantity[idx], 10, f = ceiling)
  df_all$NewFee[idx]<-ifelse(tepfee>100, 100, tepfee)
  
  idx<-with(df_all,TransactionType=='DCSS')
  df_all$NewFee[idx]<-2
  
  df_all$OldFee<-0
  
  idx<-with(df_all,TransactionType %in% c('CDIS', 'FOPT', 'DVPT'))
  df_all$OldFee[idx]<-df_all$NewFee[idx]
  
  ##idx<-with(df_all,TransactionType=='FOPT')
  ##df_all$OldFee[idx]<-df_all$NewFee[idx]
  
  ##idx<-with(df_all,TransactionType=='DVPT')
  ##df_all$OldFee[idx]<-df_all$NewFee[idx]
  
  idx<-with(df_all,TransactionType=='FDVP')
  df_all$OldFee[idx]<-33
  
  idx<-with(df_all,TransactionType=='OFOP' & flag!=0 & BuySellIndicator=='S')
  tepfee<-round_any(0.01* df_all$Quantity[idx], 10, f = ceiling)
  df_all$OldFee[idx]<-ifelse(tepfee>100, 100, tepfee)
  
  idx<-with(df_all,TransactionType=='OFOP' & flag==0 & BuySellIndicator=='S')
  tepfee<-round_any(0.01* df_all$Quantity[idx], 10, f = ceiling)
  df_all$OldFee[idx]<-ifelse(tepfee>100, 100, tepfee)
  
  idx<-with(df_all, TransactionType=='DCSS')
  df_all$OldFee[idx]<-2

  df_all$FeeGain<-df_all$NewFee-df_all$OldFee
  
  df_all[df_all$flag==1,]$flag<-'On-Exchange'
  df_all[df_all$flag==0,]$flag<-'Off-Exchange'
  
  oldname<-c('0.S','0.B','1.S','1.B') 
  newname<-c('Sell in First SI','Buy in First SI','Sell in Subseqent SI','Buy in Subseqent SI')
  for (i in 1:length(oldname))
  {
    index<-which(!(is.na(df_all$SubsequentBuySell)) & df_all$SubsequentBuySell==oldname[i])
    df_all[index,]$SubsequentBuySell<-newname[i]
  }
  
  
  if (manual==TRUE) {
    file_name<-paste(manualfolder,"Manual_Run_Output_SD",sd, sep="")
    write.csv(df_all, sprintf("%s.csv",file_name), row.names=FALSE)
    
    ##Export Daily detailed report(off-exchange)
    idx<-with(df_all, subsequent==1 & flag=='Off-Exchange')
    df_ssi<-subset(df_all[idx,],select=-c(subsequent,Code_CDP,SubsequentBuySell,comments,Currency))
    file_name<-paste(manualfolder,"Manual_Run_SD",sd, sep="")
    write.csv(df_ssi, sprintf("%s.csv",file_name), row.names=FALSE)
    
    db <- dbConnect(SQLite(), dbname=paste0(dbfolder,"Rawdata.sqlite"))
    ##dbSendQuery(db, "drop table if exists output_daily")
    if ("output_daily" %in% dbListTables(db)) {
      dbSendQuery(db, paste('Delete from output_daily where SettlementDate = ', sd))
      dbWriteTable(conn = db, name = "output_daily", value = df_all, row.names = FALSE, append=TRUE)
    } else {
      dbWriteTable(conn = db, name = "output_daily", value = df_all, row.names = FALSE, append=TRUE)
    }
    
  } else{
    
    file_name<-paste0("Output_SD",sd, ".csv")
    #  Z:\Securities Pricing
    write.csv(df_all, paste0(detailedallfolder, file_name), row.names=FALSE)
    
    ##Export Daily detailed report(off-exchange)
    idx<-with(df_all, subsequent==1 & flag=='Off-Exchange')
    df_ssi<-subset(df_all[idx,],select=-c(subsequent,Code_CDP,SubsequentBuySell,comments,Currency))
    
    file_name<-paste0("SD",sd,".csv")
    write.csv(df_ssi, paste0(detailedoffexchangefolder, file_name), row.names=FALSE)
    
    #writing all daily records to output_daily table 
    ##dbSendQuery(db, "drop table if exists output_daily")
    ##if Table output_daily doesn't exist,then create and add data to it
    ##if Table output_daily exists, check if the data already in the table. If yes, don't load it again.
    db <- dbConnect(SQLite(), dbname=paste0(dbfolder,"Rawdata.sqlite"))
    ##dbSendQuery(db, "drop table if exists output_daily")
    if ("output_daily" %in% dbListTables(db)) {
      old_sd<-dbGetQuery(db, 'select distinct SettlementDate from output_daily')
      if (!(sd %in% old_sd[,1]))  
        dbWriteTable(conn = db, name = "output_daily", value = df_all, row.names = FALSE, append=TRUE)
    } else {
      dbWriteTable(conn = db, name = "output_daily", value = df_all, row.names = FALSE, append=TRUE)
    }
    
  }
  
  dbDisconnect(db)
}

##Function for daily summary report
Daily_Summary_Report<-function(sd, manual=FALSE) {
  db <- dbConnect(SQLite(), dbname=paste0(dbfolder,"Rawdata.sqlite"))
  wb<-createWorkbook()
  sheet1<-createSheet(wb, sheetName="Breakdown by Settlement type")
  sheet2<-createSheet(wb, sheetName="Breakdown by Transaction type")
  sheet3<-createSheet(wb, sheetName="Breakdown by Participants")
  
  a<-Alignment(h="ALIGN_CENTER")
  fontbold<-Font(wb,isBold=TRUE)
  csrow<-CellStyle(wb,border=Border(color="black", position=c("TOP","BOTTOM"))
                   ,font=fontbold)

  csdollar<-CellStyle(wb, dataFormat=DataFormat('_($* #,##0_);_($* (#,##0);_($* "-"??_);_(@_)')
                      ,border=Border(color="black", position=c("TOP","BOTTOM"))
                      ,font=fontbold)
  
  csborder<- CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)')
                       ,border=Border(color="black", position=c("TOP","BOTTOM"))
                       ,font=fontbold)
  
  cscenter<- CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)')
                       ,border=Border(color="black", position=c("TOP","BOTTOM"))
                       ,font=fontbold,alignment=a)
  
  cscurrency<-CellStyle(wb, dataFormat=DataFormat('_($* #,##0_);_($* (#,##0);_($* "-"??_);_(@_)'))
  cscomma<-CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'))
  
  
  ##b<-Alignment(indent=2)
  
  ##csindent<-CellStyle(wb,alignment=b)
  
  T_settlement<-dbGetQuery(db, paste('select count(*) Number_of_Instructions, 
                                     sum(SettlementValue_SGD)  Settlement_Value,
                                     sum(Quantity) Volume,
                                     sum(NewFee) Revenue,
                                     sum(FeeGain) Net_Fee
                                     from output_daily where SettlementDate =', sd))
  
  row.names(T_settlement)<-'Total Settlements'
  names(T_settlement)<-c('Number of Instructions', 'Settlement Value', 'Volume', 'Revenue (New Fee)', 'Off-Exchange Fee Gain (Net Fee)')
  ##Total_settlement<-sapply(T_settlement, function(x) format(round(x), big.mark = ","))
  
  
  
  addDataFrame(T_settlement, sheet1, col.names=TRUE, row.names=TRUE,
               startRow=2, startColumn=2, colStyle=list('1'=csborder,'2'=csdollar,'3'=csborder,'4'=csdollar,'5'=csdollar),
               colnamesStyle=cscenter,
               rownamesStyle=csrow)
  
  ##
  
  F_settlement<-dbGetQuery(db, paste('select count(*) Number_of_Instructions, 
                                     sum(SettlementValue_SGD)  Settlement_Value,
                                     sum(Quantity) Volume,
                                     sum(NewFee) Revenue,
                                     sum(FeeGain) Net_Fee
                                     from output_daily where subsequent=0 and
                                     SettlementDate =', sd))
  row.names(F_settlement)<-'First Settlement'
  
  addDataFrame(F_settlement, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=4, startColumn=2, colStyle=list('1'=cscomma,
               '2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  S_settlement<-dbGetQuery(db, paste('select count(*) Number_of_Instructions, 
                                     sum(SettlementValue_SGD)  Settlement_Value,
                                     sum(Quantity) Volume,
                                     sum(NewFee) Revenue,
                                     sum(FeeGain) Net_Fee
                                     from output_daily where subsequent=1 and
                                     SettlementDate =', sd))
  row.names(S_settlement)<-'Subsequent Settlement'
  
  addDataFrame(S_settlement, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=5, startColumn=2, colStyle=list('1'=cscomma,
               '2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  on_off_SBLT<-dbGetQuery(db, sprintf('select flag as type,
                                      count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD) Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily
                                      where SettlementDate = %s
                                      and subsequent=1
                                      group by flag', sd))
  
  on_off_SBLT$type<-transfertype_reclass(on_off_SBLT$type)
  
  ## Aggregate data frame again based on new groupings
  on_off_SBLT <- aggregate(. ~ type, on_off_SBLT, sum)
  
  rownames(on_off_SBLT)<-paste0("   ", on_off_SBLT$type)
  on_off_SBLT$type<-NULL
  
  addDataFrame(on_off_SBLT, sheet1, col.names=FALSE, row.names=TRUE, 
               startRow=6, startColumn=2, colStyle=list('1'=cscomma,
              '2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  ##rownamesStyle=csindent)
  
  DCSS<-dbGetQuery(db, paste('select count(*) Number_of_Instructions, 
                             sum(SettlementValue_SGD)  Settlement_Value,
                             sum(Quantity) Volume,
                             sum(NewFee) Revenue,
                             sum(FeeGain) Net_Fee
                             from output_daily 
                             where subsequent="DCSS" and SettlementDate =', sd))
  row.names(DCSS)<-'OTC Bonds Settlement'
  addDataFrame(DCSS, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=11, startColumn=2, colStyle=list('1'=cscomma,
               '2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  T_S_settlement<-cbind('Total Subsequent Settlement', '', S_settlement)
  
  names(T_S_settlement)<-c('Transaction Type', 'Settlement Type', 'Number of Instructions',
                           'Settlement Value', 'Volume', 'Revenue (New Fee)', 'Off-Exchange Fee Gain (Net Fee)')
  
  addDataFrame((T_S_settlement), sheet1, col.names=TRUE, row.names=FALSE,
               startRow=13, startColumn=1, 
               colStyle=list('1'=csrow,'2'=csrow,'3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar, '7'=csdollar),
               colnamesStyle=cscenter
  )
  
  Sub_settlement<-dbGetQuery(db, sprintf('select TransactionType, flag Settlement_Type,
                                         count(*) Number_of_Instruction, 
                                         sum(SettlementValue_SGD)  Settlement_Value,
                                         sum(Quantity) as Volume,
                                         sum(NewFee) as Fee,
                                         sum(FeeGain) Net_Fee
                                         from output_daily
                                         where TransactionType in ("FDVP","OFOP") and 
                                         SettlementDate = %s
                                         group by TransactionType, flag', sd))
  
  Sub_settlement$Settlement_Type<-transfertype_reclass(Sub_settlement$Settlement_Type)
  
  ## Aggregate data frame again based on new groupings
  Sub_settlement <- aggregate(. ~ TransactionType + Settlement_Type, Sub_settlement, sum)
  
  addDataFrame(Sub_settlement, sheet1, col.names=FALSE, row.names=FALSE,
               startRow=15, startColumn=1, 
               colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency, '7'=cscurrency)
               
  )
  
  ##sheet2
  
  all_by_trans<-dbGetQuery(db, sprintf('select subsequent, TransactionType type,
                                       count(*) Number_of_Instruction, 
                                       sum(SettlementValue_SGD) Settlement_Value,
                                       sum(Quantity) Volume,
                                       sum(NewFee) Fee,
                                       sum(FeeGain) Net_Fee
                                       from output_daily
                                       where SettlementDate = %s
                                       group by subsequent, TransactionType', sd))
  
  names<-all_by_trans$type
  row.names(all_by_trans)<-names
  all_by_trans<-all_by_trans[,-c(1,2)]
  
  addDataFrame(T_settlement, sheet2, col.names=TRUE, row.names=TRUE,
               startRow=1, startColumn=1, colStyle=list('1'=csborder,'2'=csdollar,'3'=csborder,'4'=csdollar,'5'=csdollar),
               colnamesStyle=cscenter,
               rownamesStyle=csrow)
  
  addDataFrame(all_by_trans, sheet2, col.names=FALSE, row.names=TRUE,
               startRow=3, startColumn=1, colStyle=list('1'=cscomma,'2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency),
  )
  
  ##sheet 3
  off_exchange_da<-dbGetQuery(db, sprintf('select DaName as Depository_Agent, Sender, 
                                          count(*) Number_of_Instruction, 
                                          sum(SettlementValue_SGD) as Settlement_Value,
                                          sum(Quantity) Volume,
                                          sum(NewFee) Fee,
                                          sum(FeeGain) Net_Fee
                                          from output_daily
                                          where SettlementDate = %s
                                          group by Sender, DaName
                                          order by Settlement_Value DESC ', sd))
  
  ##names(off_exchange_da)<-c('Depository Agents/Members', 'DA Code', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  t_sum<-dbGetQuery(db, sprintf('select   count(*) Number_of_Instruction, 
                                          sum(SettlementValue_SGD) as Settlement_Value,
                                          sum(Quantity) Volume,
                                          sum(NewFee) Fee,
                                          sum(FeeGain) Net_Fee
                                          from output_daily
                                          where SettlementDate = %s
                                          order by Settlement_Value DESC ', sd))
  
  
  total_par <- cbind('Total', '', t_sum)
  names(total_par)<-c('Depository Agents/Members', 'Code', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(total_par, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=1, startColumn=1, colStyle=list('1'=csrow,'2'=csrow, '3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar,'7'=csdollar),
               colnamesStyle=cscenter
  )
  
  addDataFrame(off_exchange_da, sheet3, col.names=FALSE, row.names=FALSE,
               startRow=3, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )
  
  non_member<- dbGetQuery(db, sprintf('select DaName as Depository_Agent, Sender, 
                                          count(*) Number_of_Instruction, 
                                          sum(SettlementValue_SGD) as Settlement_Value,
                                          sum(Quantity) Volume,
                                          sum(NewFee) Fee,
                                          sum(FeeGain) Net_Fee
                                          from output_daily
                                          where CDPSafeKeepingAccount!="" and SettlementDate = %s
                                          group by Sender
                                          order by Settlement_Value DESC ', sd))
  
  t_nmember<-dbGetQuery(db, sprintf('select count(*) Number_of_Instruction, 
                                          sum(SettlementValue_SGD) as Settlement_Value,
                                          sum(Quantity) Volume,
                                          sum(NewFee) Fee,
                                          sum(FeeGain) Net_Fee
                                          from output_daily
                                          where CDPSafeKeepingAccount!="" and SettlementDate = %s
                                          order by Settlement_Value DESC ', sd))
  
  total_non_member <- cbind('Total', '', t_nmember)
  
  names(total_non_member)<-c('Depository Agents', 'Code', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  s_row<-nrow(off_exchange_da) + 4
  
  addDataFrame( total_non_member, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=s_row, startColumn=1, colStyle=list('1'=csrow,'2'=csrow,'3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar,'7'=csdollar),
               colnamesStyle=cscenter
  )
  
  s_row<-s_row+2
  addDataFrame(non_member, sheet3, col.names=FALSE, row.names=FALSE,
               startRow=s_row, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )
  
  member<- dbGetQuery(db, sprintf('select DaName as Depository_Agent, Sender, 
                                          count(*) Number_of_Instruction, 
                                          sum(SettlementValue_SGD) as Settlement_Value,
                                          sum(Quantity) Volume,
                                          sum(NewFee) Fee,
                                          sum(FeeGain) Net_Fee
                                          from output_daily
                                          where CDPSafeKeepingAccount="" and SettlementDate = %s
                                          group by Sender, DaName
                                          order by Settlement_Value DESC ', sd))

  t_member<-dbGetQuery(db, sprintf('select count(*) Number_of_Instruction, 
                                          sum(SettlementValue_SGD) as Settlement_Value,
                                          sum(Quantity) Volume,
                                          sum(NewFee) Fee,
                                          sum(FeeGain) Net_Fee
                                          from output_daily
                                          where CDPSafeKeepingAccount="" and SettlementDate = %s
                                          order by Settlement_Value DESC ', sd))
  total_member <- cbind('Total', '', t_member)
  
  
  s_row<-s_row + nrow(non_member) +1
  
  names(total_member)<-c('Members', 'Code', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame( total_member, sheet3, col.names=TRUE, row.names=FALSE,
                startRow=s_row, startColumn=1, colStyle=list('1'=csrow,'2'=csrow, '3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar,'7'=csdollar),
                colnamesStyle=cscenter
  )
  
  s_row<-s_row + 2
  addDataFrame(member, sheet3, col.names=FALSE, row.names=FALSE,
               startRow=s_row, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )
  
  
  autoSizeColumn(sheet1, c(1,2,3,4,5,6,7))
  autoSizeColumn(sheet2, c(1,2,3,4,5,6))
  autoSizeColumn(sheet3, c(1,2,3,4,5,6,7))
  
  if (manual==TRUE) file_name<-paste(manualfolder, "Manual_Run_Daily_Summary_Report_",sd, sep="")
  else {
    file_name<-paste0(dailysummaryfolder, "Daily_Summary_Report_", sd)
  }
  ##saveWorkbook(wb,sprintf("%s.csv",file_name))
  saveWorkbook(wb,paste0(file_name,'.csv'))
  dbDisconnect(db)
}

##Function for Weekly Summary Report

Weekly_Summary_Report<-function(start, end, manual=FALSE) {
  db <- dbConnect(SQLite(), dbname=paste0(dbfolder,"Rawdata.sqlite"))
  if (start > end)
    stop('Start date is after end date, please have a check')
  
  wb<-createWorkbook()
  sheet1<-createSheet(wb, sheetName="Breakdown by Settlement type")
  sheet2<-createSheet(wb, sheetName="Breakdown by Transaction type")
  sheet3<-createSheet(wb, sheetName="Breakdown by Participants")
  
  a<-Alignment(h="ALIGN_CENTER")
  fontbold<-Font(wb,isBold=TRUE)
  csrow<-CellStyle(wb,border=Border(color="black", position=c("TOP","BOTTOM"))
                   ,font=fontbold)
  
  csdollar<-CellStyle(wb, dataFormat=DataFormat('_($* #,##0_);_($* (#,##0);_($* "-"??_);_(@_)')
                      ,border=Border(color="black", position=c("TOP","BOTTOM"))
                      ,font=fontbold)
  
  csborder<- CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)')
                       ,border=Border(color="black", position=c("TOP","BOTTOM"))
                       ,font=fontbold)
  
  cscenter<- CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)')
                       ,border=Border(color="black", position=c("TOP","BOTTOM"))
                       ,font=fontbold,alignment=a)
  
  cscurrency<-CellStyle(wb, dataFormat=DataFormat('_($* #,##0_);_($* (#,##0);_($* "-"??_);_(@_)'))
  cscomma<-CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'))
  
  
  ##b<-Alignment(indent=2)
  
  ##csindent<-CellStyle(wb,alignment=b)
  
  T_settlement<-dbGetQuery(db, paste0('select count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD)  Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily where SettlementDate >=', start,' and SettlementDate <=',end))
  
  row.names(T_settlement)<-'Total Settlements'
  names(T_settlement)<-c('Number of Instructions', 'Settlement Value', 'Volume', 'Revenue (New Fee)', 'Off-Exchange Fee Gain (Net Fee)')
  ##Total_settlement<-sapply(T_settlement, function(x) format(round(x), big.mark = ","))
  
  
  
  addDataFrame(T_settlement, sheet1, col.names=TRUE, row.names=TRUE,
               startRow=2, startColumn=2, colStyle=list('1'=csborder,'2'=csdollar,'3'=csborder,'4'=csdollar,'5'=csdollar),
               colnamesStyle=cscenter,
               rownamesStyle=csrow)
  
  ##
  
  F_settlement<-dbGetQuery(db, paste0('select count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD)  Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily where subsequent=0 and
                                      SettlementDate >=', start,' and SettlementDate <=',end))
  row.names(F_settlement)<-'First Settlement'
  
  addDataFrame(F_settlement, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=4, startColumn=2, colStyle=list('1'=cscomma,'2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  S_settlement<-dbGetQuery(db, paste0('select count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD)  Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily where subsequent=1 and
                                      SettlementDate >=', start,' and SettlementDate <=',end))
  row.names(S_settlement)<-'Subsequent Settlement'
  
  addDataFrame(S_settlement, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=5, startColumn=2, colStyle=list('1'=cscomma,'2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  on_off_SBLT<-dbGetQuery(db, sprintf('select flag as type,
                                      count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD) Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily
                                      where SettlementDate >= %s and SettlementDate <= %s
                                      and subsequent=1
                                      group by flag', start, end))
  
  on_off_SBLT$type<-transfertype_reclass(on_off_SBLT$type)
  
  ## Aggregate data frame again based on new groupings
  on_off_SBLT <- aggregate(. ~ type, on_off_SBLT, sum)
  
  rownames(on_off_SBLT)<-paste0("  ", on_off_SBLT$type)
  on_off_SBLT$type<-NULL
  
  addDataFrame(on_off_SBLT, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=6, startColumn=2, colStyle=list('1'=cscomma,'2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  ##rownamesStyle=csindent)
  
  DCSS<-dbGetQuery(db, paste0('select count(*) Number_of_Instructions, 
                              sum(SettlementValue_SGD)  Settlement_Value,
                              sum(Quantity) Volume,
                              sum(NewFee) Revenue,
                              sum(FeeGain) Net_Fee
                              from output_daily 
                              where subsequent="DCSS" and SettlementDate >=', start,' and SettlementDate <=',end))
  row.names(DCSS)<-'OTC Bonds Settlement'
  addDataFrame(DCSS, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=10, startColumn=2, colStyle=list('1'=cscomma,'2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency)
  )
  
  T_S_settlement<-cbind('Total Subsequent Settlement', '', S_settlement)
  
  names(T_S_settlement)<-c('Transaction Type', 'Settlement Type', 'Number of Instructions',
                           'Settlement Value', 'Volume', 'Revenue (New Fee)', 'Off-Exchange Fee Gain (Net Fee)')
  
  addDataFrame((T_S_settlement), sheet1, col.names=TRUE, row.names=FALSE,
               startRow=12, startColumn=1, 
               colStyle=list('1'=csrow,'2'=csrow,'3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar, '7'=csdollar),
               colnamesStyle=cscenter
  )
  
  Sub_settlement<-dbGetQuery(db, sprintf('select TransactionType, flag Settlement_Type,
                                         count(*) Number_of_Instruction, 
                                         sum(SettlementValue_SGD)  Settlement_Value,
                                         sum(Quantity) as Volume,
                                         sum(NewFee) as Fee,
                                         sum(FeeGain) Net_Fee
                                         from output_daily
                                         where TransactionType in ("FDVP","OFOP") and 
                                         SettlementDate >= %s and SettlementDate <= %s
                                         group by TransactionType, flag', start, end))
  
  Sub_settlement$Settlement_Type<-transfertype_reclass(Sub_settlement$Settlement_Type)
  
  ## Aggregate data frame again based on new groupings
  Sub_settlement <- aggregate(. ~ TransactionType + Settlement_Type, Sub_settlement, sum) 
 
  addDataFrame(Sub_settlement, sheet1, col.names=FALSE, row.names=FALSE,
               startRow=14, startColumn=1, 
               colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency, '7'=cscurrency)
               
  )
  
  ##sheet2
  
  all_by_trans<-dbGetQuery(db, sprintf('select subsequent, TransactionType type,
                                       count(*) Number_of_Instruction, 
                                       sum(SettlementValue_SGD) Settlement_Value,
                                       sum(Quantity) Volume,
                                       sum(NewFee) Fee,
                                       sum(FeeGain) Net_Fee
                                       from output_daily
                                       where SettlementDate >= %s and SettlementDate <= %s
                                       group by subsequent, TransactionType', start, end))
  
  names<-all_by_trans$type
  row.names(all_by_trans)<-names
  all_by_trans<-all_by_trans[,-c(1,2)]
  
  addDataFrame(T_settlement, sheet2, col.names=TRUE, row.names=TRUE,
               startRow=1, startColumn=1, colStyle=list('1'=csborder,'2'=csdollar,'3'=csborder,'4'=csdollar,'5'=csdollar),
               colnamesStyle=cscenter,
               rownamesStyle=csrow)
  
  addDataFrame(all_by_trans, sheet2, col.names=FALSE, row.names=TRUE,
               startRow=3, startColumn=1, colStyle=list('1'=cscomma,'2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency)
  )
  
  ##sheet 3
  #write.xlsx(all_by_trans, sprintf("%s.xlsx",file_name), sheetName="Breakdown by Transaction type", append=TRUE)
  
  off_exchange_da<-dbGetQuery(db, sprintf('select DaName as Depository_Agent, Sender, 
                                          count(*) Number_of_Instruction, 
                                          sum(SettlementValue_SGD) as Settlement_Value,
                                          sum(Quantity) Volume,
                                          sum(NewFee) Fee,
                                          sum(FeeGain) Net_Fee
                                          from output_daily
                                          where SettlementDate >= %s and SettlementDate <= %s
                                          group by Sender, DaName
                                          order by Settlement_Value DESC ', start, end))
  
  ##names(off_exchange_da)<-c('Depository Agents/Members', 'DA Code', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  t_sum<-dbGetQuery(db, sprintf('select   count(*) Number_of_Instruction, 
                                sum(SettlementValue_SGD) as Settlement_Value,
                                sum(Quantity) Volume,
                                sum(NewFee) Fee,
                                sum(FeeGain) Net_Fee
                                from output_daily
                                where SettlementDate >= %s and SettlementDate <= %s
                                order by Settlement_Value DESC ', start, end))
  
  
  total_par <- cbind('Total', '', t_sum)
  names(total_par)<-c('Depository Agents/Members', 'Code', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(total_par, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=1, startColumn=1, colStyle=list('1'=csrow,'2'=csrow, '3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar,'7'=csdollar),
               colnamesStyle=cscenter
  )
  
  addDataFrame(off_exchange_da, sheet3, col.names=FALSE, row.names=FALSE,
               startRow=3, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )
  
  non_member<- dbGetQuery(db, sprintf('select DaName as Depository_Agent, Sender, 
                                      count(*) Number_of_Instruction, 
                                      sum(SettlementValue_SGD) as Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Fee,
                                      sum(FeeGain) Net_Fee
                                      from output_daily
                                      where CDPSafeKeepingAccount!="" and SettlementDate >= %s and SettlementDate <= %s
                                      group by Sender
                                      order by Settlement_Value DESC ', start, end))
  
  t_nmember<-dbGetQuery(db, sprintf('select count(*) Number_of_Instruction, 
                                    sum(SettlementValue_SGD) as Settlement_Value,
                                    sum(Quantity) Volume,
                                    sum(NewFee) Fee,
                                    sum(FeeGain) Net_Fee
                                    from output_daily
                                    where CDPSafeKeepingAccount!="" and SettlementDate >= %s and SettlementDate <= %s
                                    order by Settlement_Value DESC ', start, end))
  
  total_non_member <- cbind('Total', '', t_nmember)
  
  names(total_non_member)<-c('Depository Agents', 'Code', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  s_row<-nrow(off_exchange_da) + 4
  
  addDataFrame( total_non_member, sheet3, col.names=TRUE, row.names=FALSE,
                startRow=s_row, startColumn=1, colStyle=list('1'=csrow,'2'=csrow,'3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar,'7'=csdollar),
                colnamesStyle=cscenter
  )
  
  s_row<-s_row+2
  addDataFrame(non_member, sheet3, col.names=FALSE, row.names=FALSE,
               startRow=s_row, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )
  
  member<- dbGetQuery(db, sprintf('select DaName as Depository_Agent, Sender, 
                                  count(*) Number_of_Instruction, 
                                  sum(SettlementValue_SGD) as Settlement_Value,
                                  sum(Quantity) Volume,
                                  sum(NewFee) Fee,
                                  sum(FeeGain) Net_Fee
                                  from output_daily
                                  where CDPSafeKeepingAccount="" and SettlementDate >= %s and SettlementDate <= %s
                                  group by Sender, DaName
                                  order by Settlement_Value DESC ', start, end))
  
  t_member<-dbGetQuery(db, sprintf('select count(*) Number_of_Instruction, 
                                   sum(SettlementValue_SGD) as Settlement_Value,
                                   sum(Quantity) Volume,
                                   sum(NewFee) Fee,
                                   sum(FeeGain) Net_Fee
                                   from output_daily
                                   where CDPSafeKeepingAccount="" and SettlementDate >= %s and SettlementDate <= %s
                                   order by Settlement_Value DESC ', start, end))
  total_member <- cbind('Total', '', t_member)
  
  
  s_row<-s_row + nrow(non_member) +1
  
  names(total_member)<-c('Members', 'Code', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame( total_member, sheet3, col.names=TRUE, row.names=FALSE,
                startRow=s_row, startColumn=1, colStyle=list('1'=csrow,'2'=csrow, '3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar,'7'=csdollar),
                colnamesStyle=cscenter
  )
  
  s_row<-s_row + 2
  addDataFrame(member, sheet3, col.names=FALSE, row.names=FALSE,
               startRow=s_row, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )
  
  
  
  
  
  autoSizeColumn(sheet1, c(1,2,3,4,5,6,7))
  autoSizeColumn(sheet2, c(1,2,3,4,5,6))
  autoSizeColumn(sheet3, c(1,2,3,4,5,6,7))
  
  if (manual==TRUE)  file_name<-paste0(manualfolder, "Manual_Run_Weekly_Summary_Report_",start,'_to_',end )
  else {
    file_name<-paste0(weeklysummaryfolder,"Weekly_Summary_Report_",start,'_to_',end )
  }
    ##saveWorkbook(wb,sprintf("%s.csv",file_name))
  saveWorkbook(wb,paste0(file_name,'.csv'))
  dbDisconnect(db)
}

##Function for monthly report
Monthly_Summary_Report<-function(start, end, manual=FALSE) {
  
  if (start > end)
    stop('Start date is after end date, please have a check')
  db <- dbConnect(SQLite(), dbname=paste0(dbfolder,"Rawdata.sqlite"))
  wb<-createWorkbook()
  sheet1<-createSheet(wb, sheetName="Breakdown by Settlement type")
  sheet2<-createSheet(wb, sheetName="Breakdown by Transaction type")
  sheet3<-createSheet(wb, sheetName="Breakdown by Securities Type (Top 20)")
  sheet4<-createSheet(wb, sheetName="Breakdown by DA (Top 20)")
  sheet5<-createSheet(wb, sheetName="Breakdown by Securities Account (Top 20)")
  sheet6<-createSheet(wb, sheetName="Breakdown by Geographical Split(Top 20)")
  
  a<-Alignment(h="ALIGN_CENTER")
  fontbold<-Font(wb,isBold=TRUE)
  csrow<-CellStyle(wb,border=Border(color="black", position=c("TOP","BOTTOM"))
                   ,font=fontbold)
  
  csdollar<-CellStyle(wb, dataFormat=DataFormat('_($* #,##0_);_($* (#,##0);_($* "-"??_);_(@_)')
                      ,border=Border(color="black", position=c("TOP","BOTTOM"))
                      ,font=fontbold)
  
  csborder<- CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)')
                       ,border=Border(color="black", position=c("TOP","BOTTOM"))
                       ,font=fontbold)
  
  cscenter<- CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)')
                       ,border=Border(color="black", position=c("TOP","BOTTOM"))
                       ,font=fontbold,alignment=a)
  
  cscurrency<-CellStyle(wb, dataFormat=DataFormat('_($* #,##0_);_($* (#,##0);_($* "-"??_);_(@_)'))
  cscomma<-CellStyle(wb, dataFormat=DataFormat('_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'))
  
  
  ##b<-Alignment(indent=2)
  
  ##csindent<-CellStyle(wb,alignment=b)
  
  T_settlement<-dbGetQuery(db, paste0('select count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD)  Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily where SettlementDate >=', start,' and SettlementDate <=',end))
  
  row.names(T_settlement)<-'Total Settlements'
  names(T_settlement)<-c('Number of Instructions', 'Settlement Value', 'Volume', 'Revenue (New Fee)', 'Off-Exchange Fee Gain (Net Fee)')
  ##Total_settlement<-sapply(T_settlement, function(x) format(round(x), big.mark = ","))
  
  
  
  addDataFrame(T_settlement, sheet1, col.names=TRUE, row.names=TRUE,
               startRow=2, startColumn=2, colStyle=list('1'=csborder,'2'=csdollar,'3'=csborder,'4'=csdollar,'5'=csdollar),
               colnamesStyle=cscenter,
               rownamesStyle=csrow)
  
  ##
  
  F_settlement<-dbGetQuery(db, paste0('select count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD)  Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily where subsequent=0 and
                                      SettlementDate >=', start,' and SettlementDate <=',end))
  row.names(F_settlement)<-'First Settlement'
  
  addDataFrame(F_settlement, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=4, startColumn=2, colStyle=list('1'=cscomma,
               '2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  S_settlement<-dbGetQuery(db, paste0('select count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD)  Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily where subsequent=1 and
                                      SettlementDate >=', start,' and SettlementDate <=',end))
  row.names(S_settlement)<-'Subsequent Settlement'
  
  addDataFrame(S_settlement, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=5, startColumn=2, colStyle=list('1'=cscomma,
               '2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  on_off_SBLT<-dbGetQuery(db, sprintf('select flag as type,
                                      count(*) Number_of_Instructions, 
                                      sum(SettlementValue_SGD) Settlement_Value,
                                      sum(Quantity) Volume,
                                      sum(NewFee) Revenue,
                                      sum(FeeGain) Net_Fee
                                      from output_daily
                                      where SettlementDate >= %s and SettlementDate <= %s
                                      and subsequent=1
                                      group by flag', start, end))
  
  on_off_SBLT$type<-transfertype_reclass(on_off_SBLT$type)
  
  ## Aggregate data frame again based on new groupings
  on_off_SBLT <- aggregate(. ~ type, on_off_SBLT, sum)

  rownames(on_off_SBLT)<-paste0("  ", on_off_SBLT$type)
  on_off_SBLT$type<-NULL
  
  addDataFrame(on_off_SBLT, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=6, startColumn=2, colStyle=list('1'=cscomma,'2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  ##rownamesStyle=csindent)
  
  DCSS<-dbGetQuery(db, paste0('select count(*) Number_of_Instructions, 
                              sum(SettlementValue_SGD)  Settlement_Value,
                              sum(Quantity) Volume,
                              sum(NewFee) Revenue,
                              sum(FeeGain) Net_Fee
                              from output_daily 
                              where subsequent="DCSS" and SettlementDate >=', start,' and SettlementDate <=',end))
  row.names(DCSS)<-'OTC Bonds Settlement'
  addDataFrame(DCSS, sheet1, col.names=FALSE, row.names=TRUE,
               startRow=10, startColumn=2, colStyle=list('1'=cscomma,
               '2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  T_S_settlement<-cbind('Total Subsequent Settlement', '', S_settlement)
  
  names(T_S_settlement)<-c('Transaction Type', 'Settlement Type', 'Number of Instructions',
                           'Settlement Value', 'Volume', 'Revenue (New Fee)', 'Off-Exchange Fee Gain (Net Fee)')
  
  addDataFrame((T_S_settlement), sheet1, col.names=TRUE, row.names=FALSE,
               startRow=12, startColumn=1, 
               colStyle=list('1'=csrow,'2'=csrow,'3'=csborder,'4'=csdollar,'5'=csborder,'6'=csdollar, '7'=csdollar),
               colnamesStyle=cscenter
  )
  
  Sub_settlement<-dbGetQuery(db, sprintf('select TransactionType, flag Settlement_Type,
                                         count(*) Number_of_Instruction, 
                                         sum(SettlementValue_SGD)  Settlement_Value,
                                         sum(Quantity) as Volume,
                                         sum(NewFee) as Fee,
                                         sum(FeeGain) Net_Fee
                                         from output_daily
                                         where TransactionType in ("FDVP","OFOP") and 
                                         SettlementDate >= %s and SettlementDate <= %s
                                         group by TransactionType, flag', start, end))
  
  Sub_settlement$Settlement_Type<-transfertype_reclass(Sub_settlement$Settlement_Type)
  
  ## Aggregate data frame again based on new groupings
  Sub_settlement <- aggregate(. ~ TransactionType + Settlement_Type, Sub_settlement, sum) 
  
  addDataFrame(Sub_settlement, sheet1, col.names=FALSE, row.names=FALSE,
               startRow=14, startColumn=1, 
               colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency, '7'=cscurrency)
               
  )
  
  ##sheet2
  
  all_by_trans<-dbGetQuery(db, sprintf('select subsequent, TransactionType type,
                                       count(*) Number_of_Instruction, 
                                       sum(SettlementValue_SGD) Settlement_Value,
                                       sum(Quantity) Volume,
                                       sum(NewFee) Fee,
                                       sum(FeeGain) Net_Fee
                                       from output_daily
                                       where SettlementDate >= %s and SettlementDate <= %s
                                       group by subsequent, TransactionType', start, end))
  
  names<-all_by_trans$type
  row.names(all_by_trans)<-names
  all_by_trans<-all_by_trans[,-c(1,2)]
  
  addDataFrame(T_settlement, sheet2, col.names=TRUE, row.names=TRUE,
               startRow=1, startColumn=1, colStyle=list('1'=csborder,'2'=csdollar,'3'=csborder,'4'=csdollar,'5'=csdollar),
               colnamesStyle=cscenter,
               rownamesStyle=csrow)
  
  addDataFrame(all_by_trans, sheet2, col.names=FALSE, row.names=TRUE,
               startRow=3, startColumn=1, colStyle=list('1'=cscomma,
               '2'=cscurrency,'3'=cscomma,'4'=cscurrency,'5'=cscurrency))
  
  ##sheet 3
  #write.xlsx(all_by_trans, sprintf("%s.xlsx",file_name), sheetName="Breakdown by Transaction type", append=TRUE)
  
  F_secu<-dbGetQuery(db, sprintf('select  SecurityDescription, 
                                 count(*) Number_of_Instruction,
                                 sum(SettlementValue_SGD) as Settlement_Value,
                                 sum(Quantity) Volume,
                                 sum(NewFee) Fee,
                                 sum(FeeGain) Net_Fee
                                 from output_daily
                                 where subsequent=0 and
                                 SettlementDate >= %s and SettlementDate <= %s
                                 group by SecurityDescription
                                 order by Settlement_Value DESC
                                 limit 20', start, end))
  
  names(F_secu)<-c('First Settlement', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(F_secu, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=1, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )
  
  
  S_secu_breakdown<-dbGetQuery(db, sprintf('select flag as type,
                                        SecurityDescription, 
                                        count(*) Number_of_Instruction,
                                        sum(SettlementValue_SGD) as Settlement_Value,
                                        sum(Quantity) Volume,
                                        sum(NewFee) Fee,
                                        sum(FeeGain) Net_Fee
                                        from output_daily
                                        where subsequent=1 and
                                        SettlementDate >= %s and SettlementDate <= %s
                                        group by flag, SecurityDescription',
                                        start, end))
  if (nrow(S_secu_breakdown) > 0) {
    S_secu<- aggregate(S_secu_breakdown[3:7], by=list(S_secu_breakdown$SecurityDescription), sum)
  }
  S_secu<-head(arrange(S_secu, desc(Settlement_Value)),n=20)
  S_secu$type<-NULL
  
  names(S_secu)<-c('Subseqent Settlement', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(S_secu, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=23, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter)  
  
  S_secu_breakdown$type<-transfertype_reclass(S_secu_breakdown$type)
  if (nrow(S_secu_breakdown) > 0) {
    S_secu_breakdown<-aggregate(. ~ type + SecurityDescription, S_secu_breakdown, sum)
  }
  
  trade_related<-head(arrange(S_secu_breakdown[S_secu_breakdown$type=="Trade-related", ], 
                         desc(Settlement_Value)),n=20)
  trade_related$type<-NULL

  names(trade_related)<-c('Trade Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(trade_related, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=45, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter) 
  
  market_related<-head(arrange(S_secu_breakdown[S_secu_breakdown$type=="Market-related",],
                              desc(Settlement_Value)), n=20)
  market_related$type<-NULL
  
  names(market_related)<-c('Market Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(market_related, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=67, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter)            
  
  off_exchange<-head(arrange(S_secu_breakdown[S_secu_breakdown$type=="Off-exchange",],
                               desc(Settlement_Value)), n=20)
  off_exchange$type<-NULL
  
  names(off_exchange)<-c('Off-exchange', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(off_exchange, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=89, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter)

  own_account<-head(arrange(S_secu_breakdown[S_secu_breakdown$type=="Own Account",],
                             desc(Settlement_Value)), n=20)
  own_account$type<-NULL
  
  names(own_account)<-c('Own Account', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(own_account, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=111, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter)
  
  issuer_related<-head(arrange(S_secu_breakdown[S_secu_breakdown$type=="Issuer-related",],
                             desc(Settlement_Value)), n=20)
  issuer_related$type<-NULL
  
  names(issuer_related)<-c('Issuer Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(issuer_related, sheet3, col.names=TRUE, row.names=FALSE,
               startRow=133, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter)  
  
  ##sheet 4
  #write.xlsx(all_by_trans, sprintf("%s.xlsx",file_name), sheetName="Breakdown by Transaction type", append=TRUE)
  
  F_da<-dbGetQuery(db, sprintf('select  DaName, 
                               count(*) Number_of_Instruction,
                               sum(SettlementValue_SGD) as Settlement_Value,
                               sum(Quantity) Volume,
                               sum(NewFee) Fee,
                               sum(FeeGain) Net_Fee
                               from output_daily
                               where subsequent=0 and
                               SettlementDate >= %s and SettlementDate <= %s
                               group by DaName
                               order by Settlement_Value DESC
                               limit 20', start, end))
  
  names(F_da)<-c('First Settlement', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(F_da, sheet4, col.names=TRUE, row.names=FALSE,
               startRow=1, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )
  
  S_da_breakdown<-dbGetQuery(db, sprintf('select flag as type, DaName, 
                               count(*) Number_of_Instruction,
                               sum(SettlementValue_SGD) as Settlement_Value,
                               sum(Quantity) Volume,
                               sum(NewFee) Fee,
                               sum(FeeGain) Net_Fee
                               from output_daily
                               where subsequent=1 and
                               SettlementDate >= %s and SettlementDate <= %s
                               group by flag, DaName', 
                               start, end))
  if (nrow(S_da_breakdown) > 0) {
    S_da<- aggregate(S_da_breakdown[3:7], by=list(S_da_breakdown$DaName), sum)
  }
  S_da<-head(arrange(S_da, desc(Settlement_Value)),n=20)
  S_da$type<-NULL
  
  names(S_da)<-c('Subsequent Settlement', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(S_da, sheet4, col.names=TRUE, row.names=FALSE,
               startRow=23, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )             
  
  S_da_breakdown$type<-transfertype_reclass(S_da_breakdown$type)
  if (nrow(S_da_breakdown) > 0) {
    S_da_breakdown<-aggregate(. ~ type + DaName, S_da_breakdown, sum)
  }

  trade_da<-head(arrange(S_da_breakdown[S_da_breakdown$type=="Trade-related", ], 
                         desc(Settlement_Value)),n=20)
  trade_da$type<-NULL
  
  names(trade_da)<-c('Trade Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(trade_da, sheet4, col.names=TRUE, row.names=FALSE,
               startRow=45, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )   
  
  market_da<-head(arrange(S_da_breakdown[S_da_breakdown$type=="Market-related", ], 
                         desc(Settlement_Value)),n=20)
  market_da$type<-NULL
  
  names(market_da)<-c('Market Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(market_da, sheet4, col.names=TRUE, row.names=FALSE,
               startRow=67, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  ) 
  
  off_da<-head(arrange(S_da_breakdown[S_da_breakdown$type=="Off-exchange", ], 
                          desc(Settlement_Value)),n=20)
  off_da$type<-NULL
  
  names(off_da)<-c('Off-exchange', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(off_da, sheet4, col.names=TRUE, row.names=FALSE,
               startRow=89, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )
  
  own_da<-head(arrange(S_da_breakdown[S_da_breakdown$type=="Own account", ], 
                       desc(Settlement_Value)),n=20)
  own_da$type<-NULL
  
  names(own_da)<-c('Own Account', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(own_da, sheet4, col.names=TRUE, row.names=FALSE,
               startRow=111, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter)
               
  issuer_da<-head(arrange(S_da_breakdown[S_da_breakdown$type=="Issuer-related", ], 
                       desc(Settlement_Value)),n=20)
  issuer_da$type<-NULL
  
  names(issuer_da)<-c('Issuer Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(issuer_da, sheet4, col.names=TRUE, row.names=FALSE,
               startRow=133, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter)  
  
  ##sheet 5
  #write.xlsx(all_by_trans, sprintf("%s.xlsx",file_name), sheetName="Breakdown by Transaction type", append=TRUE)
  
  F_cdp<-dbGetQuery(db, sprintf('select CDPAccountName, DaName,
                               count(*) Number_of_Instruction,
                               sum(SettlementValue_SGD) as Settlement_Value,
                                sum(Quantity) Volume,
                                sum(NewFee) Fee,
                                sum(FeeGain) Net_Fee
                                from output_daily
                                where subsequent=0 and
                                CDPAccountName is not null and
                                SettlementDate >= %s and SettlementDate <= %s
                                group by CDPAccountName
                                order by Settlement_Value DESC
                                limit 20', start, end))
  
  names(F_cdp)<-c('First Settlement', 'DA', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(F_cdp, sheet5, col.names=TRUE, row.names=FALSE,
               startRow=1, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )
  
  
  S_cdp_breakdown<-dbGetQuery(db, sprintf('select flag as type, 
                                CDPAccountName, DaName,
                                count(*) Number_of_Instruction,
                                sum(SettlementValue_SGD) as Settlement_Value,
                                sum(Quantity) Volume,
                                sum(NewFee) Fee,
                                sum(FeeGain) Net_Fee
                                from output_daily
                                where subsequent=1 and
                                SettlementDate >= %s and SettlementDate <= %s
                                group by flag, CDPAccountName, DaName',
                                start, end)) 
  
  if (nrow(S_cdp_breakdown) > 0) {
    S_cdp<- aggregate(S_cdp_breakdown[4:8], 
                     by=list(S_cdp_breakdown$CDPAccountName, S_cdp_breakdown$DaName), sum)
  }
  S_cdp<-head(arrange(S_cdp, desc(Settlement_Value)),n=20)
  S_cdp$type<-NULL
  
  names(S_cdp)<-c('Subsequent Settlement', 'DA', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(S_cdp, sheet5, col.names=TRUE, row.names=FALSE,
               startRow=23, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )            
  
  S_cdp_breakdown$type<-transfertype_reclass(S_cdp_breakdown$type)
  if (nrow(S_cdp_breakdown) > 0) {
    S_cdp_breakdown<-aggregate(. ~ type + CDPAccountName + DaName, 
                               S_cdp_breakdown, sum)
  }
  
  trade_cdp<-head(arrange(S_cdp_breakdown[S_cdp_breakdown$type=="Trade-related", ], 
                         desc(Settlement_Value)),n=20)
  trade_cdp$type<-NULL
  
  names(trade_cdp)<-c('Trade Related', 'DA', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(trade_cdp, sheet5, col.names=TRUE, row.names=FALSE,
               startRow=45, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )   
  
  
  market_cdp<-head(arrange(S_cdp_breakdown[S_cdp_breakdown$type=="Market-related", ], 
                          desc(Settlement_Value)),n=20)
  market_cdp$type<-NULL
  
  names(market_cdp)<-c('Market Related', 'DA', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(market_cdp, sheet5, col.names=TRUE, row.names=FALSE,
               startRow=67, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  )
  
  off_cdp<-head(arrange(S_cdp_breakdown[S_cdp_breakdown$type=="Off-exchange", ], 
                           desc(Settlement_Value)),n=20)
  off_cdp$type<-NULL
  
  names(off_cdp)<-c('Off-exchange', 'DA', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(off_cdp, sheet5, col.names=TRUE, row.names=FALSE,
               startRow=89, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  ) 
  
  own_cdp<-head(arrange(S_cdp_breakdown[S_cdp_breakdown$type=="Own account", ], 
                        desc(Settlement_Value)),n=20)
  own_cdp$type<-NULL
  
  names(own_cdp)<-c('Own Account', 'DA', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(own_cdp, sheet5, col.names=TRUE, row.names=FALSE,
               startRow=111, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  ) 
  
  issuer_cdp<-head(arrange(S_cdp_breakdown[S_cdp_breakdown$type=="Issuer-related", ], 
                        desc(Settlement_Value)),n=20)
  issuer_cdp$type<-NULL
  
  names(issuer_cdp)<-c('Issuer Related', 'DA', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(issuer_cdp, sheet5, col.names=TRUE, row.names=FALSE,
               startRow=133, startColumn=1, colStyle=list('3'=cscomma,'4'=cscurrency,'5'=cscomma,'6'=cscurrency,'7'=cscurrency),
               colnamesStyle=cscenter
  ) 

  ##sheet 6
  #write.xlsx(all_by_trans, sprintf("%s.xlsx",file_name), sheetName="Breakdown by Transaction type", append=TRUE)
  
  F_location<-dbGetQuery(db, sprintf('select Location, 
                                     count(*) Number_of_Instruction,
                                     sum(SettlementValue_SGD) as Settlement_Value,
                                     sum(Quantity) Volume,
                                     sum(NewFee) Fee,
                                     sum(FeeGain) Net_Fee
                                     from output_daily
                                     where subsequent=0 and
                                     Location is not null and
                                     SettlementDate >= %s and SettlementDate <= %s
                                     group by Location
                                     order by Settlement_Value DESC
                                     limit 20', start, end))
  
  names(F_location)<-c('First Settlement', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(F_location, sheet6, col.names=TRUE, row.names=FALSE,
               startRow=1, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )
  
  
  S_location_breakdown<-dbGetQuery(db, sprintf('select flag as type, Location, 
                                     count(*) Number_of_Instruction,
                                     sum(SettlementValue_SGD) as Settlement_Value,
                                     sum(Quantity) Volume,
                                     sum(NewFee) Fee,
                                     sum(FeeGain) Net_Fee
                                     from output_daily
                                     where subsequent=1 and
                                     SettlementDate >= %s and SettlementDate <= %s
                                     group by flag, Location',
                                     start, end))       
  if (nrow(S_location_breakdown) > 0) {
    S_location<- aggregate(S_location_breakdown[3:7], 
                      by=list(S_location_breakdown$Location), sum)
  }
  S_location<-head(arrange(S_location, desc(Settlement_Value)),n=20)
  S_location$type<-NULL
  
  names(S_location)<-c('Subsequent Settlement', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  
  addDataFrame(S_location, sheet6, col.names=TRUE, row.names=FALSE,
               startRow=23, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )            

  S_location_breakdown$type<-transfertype_reclass(S_location_breakdown$type)
  if (nrow(S_location_breakdown) > 0) {
    S_location_breakdown<-aggregate(. ~ type + Location, 
                                    S_location_breakdown, sum)
  }
  
  trade_location<-head(arrange(S_location_breakdown[S_location_breakdown$type=="Trade-related", ], 
                          desc(Settlement_Value)),n=20)
  trade_location$type<-NULL

  names(trade_location)<-c('Trade Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(trade_location, sheet6, col.names=TRUE, row.names=FALSE,
               startRow=45, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )   
  
  market_location<-head(arrange(S_location_breakdown[S_location_breakdown$type=="Market-related", ], 
                               desc(Settlement_Value)),n=20)
  market_location$type<-NULL
  
  names(market_location)<-c('Market Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(market_location, sheet6, col.names=TRUE, row.names=FALSE,
               startRow=67, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  ) 
  
  off_location<-head(arrange(S_location_breakdown[S_location_breakdown$type=="Off-exchange", ], 
                                desc(Settlement_Value)),n=20)
  off_location$type<-NULL
  
  names(off_location)<-c('Off-exchange', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(off_location, sheet6, col.names=TRUE, row.names=FALSE,
               startRow=89, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  ) 
  
  own_location<-head(arrange(S_location_breakdown[S_location_breakdown$type=="Own account", ], 
                             desc(Settlement_Value)),n=20)
  own_location$type<-NULL
  
  names(own_location)<-c('Own Account', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(own_location, sheet6, col.names=TRUE, row.names=FALSE,
               startRow=111, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )
  
  issuer_location<-head(arrange(S_location_breakdown[S_location_breakdown$type=="Issuer-related", ], 
                             desc(Settlement_Value)),n=20)
  issuer_location$type<-NULL
  
  names(issuer_location)<-c('Issuer Related', 'Number of Instruction', 'Settlement Value','Settlement Volume','Revenue (New Fee)','Off-Exchange Fee Gain')
  addDataFrame(issuer_location, sheet6, col.names=TRUE, row.names=FALSE,
               startRow=133, startColumn=1, colStyle=list('2'=cscomma,'3'=cscurrency,'4'=cscomma,'5'=cscurrency,'6'=cscurrency),
               colnamesStyle=cscenter
  )   
  
  autoSizeColumn(sheet1, c(1,2,3,4,5,6,7))
  autoSizeColumn(sheet2, c(1,2,3,4,5,6))
  autoSizeColumn(sheet3, c(1,2,3,4,5,6,7))
  autoSizeColumn(sheet4, c(1,2,3,4,5,6,7))
  autoSizeColumn(sheet5, c(1,2,3,4,5,6,7))
  autoSizeColumn(sheet6, c(1,2,3,4,5,6,7))
  
  
  if (manual==TRUE)  file_name<-paste0(manualfolder,"Manual_Run_Monthly_Summary_Report_",start,'_to_',end )
  else {
    file_name<-paste0(monthlysummaryfolder,"Monthly_Summary_Report_",start,'_to_',end )
  }
  ##saveWorkbook(wb,sprintf("%s.csv",file_name))
  saveWorkbook(wb,paste0(file_name,'.csv'))
  dbDisconnect(db)
}
