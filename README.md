# Microsoft-R

rm(list = ls())

library(dplyr)

setwd("C:/Users/Ankit/Desktop/BIG_DATA")

sqlServerConnString <- "SERVER=SERVER_NAME;DATABASE=DBN;trusted_connection =T"

sqlServerDataDS <- RxSqlServerData(sqlQuery = "write SQL query",
                                   connectionString = sqlServerConnString)

# Create an xdf file name
claimsXdfFileName <- file.path(tempdir(), "importedClaims.xdf")

# Import the data into the xdf file
rxImport(sqlServerDataDS, claimsXdfFileName, overwrite = TRUE)

# Read xdf file into a data frame
res <- rxDataStep(inData = claimsXdfFileName ,maxRowsByCols = 1000000000)
head(res)

res$Date <- substr(res$TransactionDate,1,10)

res$Date <- as.Date(res$Date , "%Y-%m-%d")

res$UNIQUE ID <- as.character(res$UNIQUE ID)

sapply(res , function(x) class(x))

sapply(res , function(x) sum(is.na(x)))

res$TransactionDate <- NULL

res <- na.omit(res)

res <- rxSort(inData = res , sortByVars = "Date" , decreasing = F,maxRowsByCols = 1000000000)

res$last.ref.days <- as.numeric(difftime(Sys.Date(),res$Date , units = "days"))

data1 <- res %>% group_by(UNIQUE ID) %>%
  summarise(netvalue = sum(HomeCollectionCharges),
            freq = length(unique(Date)),
            recency = min(last.ref.days))

data110 <- filter(data1 , data1$freq < 10)

leat <- data110$UNIQUE ID

use_data <- res[ ! res$UNIQUE ID %in% leat ,  ]

# work end

final <- use_data %>% group_by(UNIQUE ID) %>% 
  summarise(R_VALUE = min(last.ref.days),
            F_VALUE = length(Date),
            M_VALUE = sum(HomeCollectionCharges)
  )

final <- final[ ! final$M_VALUE == 0 ,]

write.csv(final,"test.csv",row.names = F)

summary(-final$R_VALUE)
summary(final$F_VALUE)
summary(final$M_VALUE)


Recency_cuts <- quantile(-final$R_VALUE , probs = seq(0.20 ,0.80 , by=0.20))
Frequency_cuts <- quantile(final$F_VALUE , probs = seq(0.20,0.80, by=0.20))
Monetary_cuts <- quantile(final$M_VALUE , probs = seq(0.20,0.80,by=0.20))


final$R_SCORE <- findInterval(-final$R_VALUE , c(-Inf,Recency_cuts,Inf))
final$F_SCORE <- findInterval(final$F_VALUE , c(-Inf,Frequency_cuts,Inf))
final$M_SCORE <- findInterval(final$M_VALUE , c(-Inf,Monetary_cuts,Inf))

r <- table(final$R_SCORE)
barplot(r , col = rainbow(45))
f <-table(final$F_SCORE)
barplot(f , col = rainbow(10))
m <- table(final$M_SCORE)
barplot(m ,col = rainbow(18))


final$COMB_F_M_SCORE <- ((final$F_SCORE + final$M_SCORE)/2)

final$RFM_SCORE <- paste(final$R_SCORE,final$F_SCORE,final$M_SCORE,sep = "")

#####MAPPING#####

MAP_DATA <- RxSqlServerData(sqlQuery = "SELECT UNIQUE ID,CITY,NAME FROM
                            [DrLal].[dbo].[tblPatientTransaction]" ,
                            connectionString = sqlServerConnString)

MAP_XDF <- file.path(tempdir() , "MAPP_DATA.xdf")

rxImport(MAP_DATA,MAP_XDF,overwrite = T)

MAPDA <- rxDataStep(inData = MAP_XDF ,maxRowsByCols = 1000000000 )

final$UNIQUE ID <- trimws(final$UNIQUE ID)

MAPDA$UNIQUE ID <- trimws(MAPDA$UNIQUE ID)
MAPDA$NAME <- trimws(MAPDA$NAME)
MAPDA$CITY <- trimws(MAPDA$CITY)

MAPDA$CITY <- toupper(MAPDA$CITY)
MAPDA$NAME <- toupper(MAPDA$NAME)

MAPDA$CITY <- gsub("[[:punct:][:blank:]]+", " ", MAPDA$CITY)
MAPDA$NAME <- gsub("[[:punct:][:blank:]]+", " ", MAPDA$NAME)

doc <- unique(subset(MAPDA,select = c("UNIQUE ID","NAME","CITY")))

target <- merge(final,doc,by.x = "UNIQUE ID",by.y = "UNIQUE ID",all.x = TRUE)

write.csv(target , "test.csv" , row.names = F)

outdatasource <- RxSqlServerData(table = "RFM_UPDATED",connectionString = sqlServerConnString)

rxDataStep(inData = target , outFile = outdatasource , append = "none",overwrite = T)

# append="rows" will not create a new table
# append="none" will create a new table (but check column types that are picked for you especially if you have strings in your dataframe!)
#overwrite = T it will truncate data in the table.


N/GqBtOSNCEJ8s3RLbYdXyJvLnoV17lmx9Fqms5XOsEqk2Io/YSYAc/8tT3df1p2jAErqUFFoW+VfLES60fHWg==







