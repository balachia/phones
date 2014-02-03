library(data.table)
library(ffbase)

rm(list=ls())

setwd('/archive/gsb/vashevko/CT/')

files <- paste0('csv/', c('2012-07', '2012-08', '2012-09',
                          '2012-10', '2012-11', '2012-12', '2013-01', '2013-02',
                          '2013-03', '2013-04', '2013-05', '2013-06'),
                          '.csv')

ffd <- NULL
for (file in files) {
  print(file)
  
  c.dt <- fread(file)

  #"SERV_ID"       "ACC_NBR"       "CALLED_NBR"    "CALLING_NBR"
  #[5] "ETL_TYPE_NAME" "ETL_TYPE_ID"   "CALL_TYPE"     "DURATION"
  #[9] "START_TIME"
  # recode all the damn columns
  c.dt[,SERV_ID := as.numeric(SERV_ID)]
  c.dt[,ACC_NBR := as.numeric(ACC_NBR)]
  c.dt[,CALLED_NBR := as.numeric(CALLED_NBR)]
  c.dt[,CALLING_NBR := as.numeric(CALLING_NBR)]
  c.dt[,ETL_TYPE_NAME := as.factor(ETL_TYPE_NAME)]
  c.dt[,ETL_TYPE_ID := as.numeric(ETL_TYPE_ID)]
  c.dt[,CALL_TYPE := as.numeric(CALL_TYPE)]
  c.dt[,DURATION := as.numeric(DURATION)]
  c.dt[,START_TIME:= as.POSIXct(START_TIME, tx='GMT')]
  
  if (is.null(ffd)) {
    ffd <- as.ffdf(c.dt)
  } else {
    ffd <- ffdfappend(ffd,c.dt)
  }

  rm(c.dt)
  gc()
}

save.ffdf(ffd, dir='./ffdb/calls/')
close(ffd)
