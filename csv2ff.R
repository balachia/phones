library(data.table)
library(ffbase)
library(lubridate)

rm(list=ls())

setwd('/archive/gsb/vashevko/CT/')

files <- paste0('csv/', c('2012-07', '2012-08', '2012-09',
                          '2012-10', '2012-11', '2012-12', '2013-01', '2013-02',
                          '2013-03', '2013-04', '2013-05', '2013-06'),
                          '.csv')

ffd <- NULL
# files <- files[1]
  
for (file in files) {
  print(file)

  print('reading')
  print(system.time(c.dt <- fread(file)))

  #"SERV_ID"       "ACC_NBR"       "CALLED_NBR"    "CALLING_NBR"
  #[5] "ETL_TYPE_NAME" "ETL_TYPE_ID"   "CALL_TYPE"     "DURATION"
  #[9] "START_TIME"
  # recode all the damn columns
  print('converting')
  print(system.time(c.dt[,SERV_ID := as.numeric(SERV_ID)]))
  print(system.time(c.dt[,ACC_NBR := as.numeric(ACC_NBR)]))
  print(system.time(c.dt[,CALLED_NBR := as.numeric(CALLED_NBR)]))
  print(system.time(c.dt[,CALLING_NBR := as.numeric(CALLING_NBR)]))
  print(system.time(c.dt[,ETL_TYPE_NAME := as.factor(ETL_TYPE_NAME)]))
  print(system.time(c.dt[,ETL_TYPE_ID := as.numeric(ETL_TYPE_ID)]))
  print(system.time(c.dt[,CALL_TYPE := as.numeric(CALL_TYPE)]))
  print(system.time(c.dt[,DURATION := as.numeric(DURATION)]))
  print(system.time(c.dt[,ST2 := START_TIME]))
  print(system.time(c.dt[,START_TIME := parse_date_time(START_TIME, orders = c('y/m/d H:M:S','y/m/d'), tz='UTC')]))
       #as.POSIXct(START_TIME, tz='UTC')]

  print('writing out')
  ptm <- proc.time()
  if (is.null(ffd)) {
    ffd <- as.ffdf(c.dt[,!'ST2', with=FALSE])
  } else {
    ffd <- ffdfappend(ffd,c.dt[,!'ST2', with=FALSE])
  }
  print(proc.time() - ptm)
  
  print(c.dt)
  print('EMPTIES')
  print(c.dt[is.na(START_TIME)])

  rm(c.dt)
  gc()
}

system.time(idx <- ffwhich(ffd,hour(START_TIME) == 0 &
  minute(START_TIME) == 0 & second(START_TIME) == 0))

save.ffdf(ffd, dir='./ffdb/calls/', overwrite=TRUE)
close(ffd)
