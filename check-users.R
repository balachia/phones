library(ffbase)
library(data.table)
library(parallel)
library(lubridate)

rm(list=ls())

setwd('/archive/gsb/vashevko/CT/')

rd <- readRDS('./Rds/demos-raw-procd.Rds')
md <- readRDS('./Rds/demos-more-procd.Rds')

ffdfns <- load.ffdf('./ffdb/calls')
ffd <- ffdfns$ffd

system.time(pre.c.serv.ids <- data.table(as.data.frame(ffd['SERV_ID'])))
system.time(c.serv.ids <- pre.c.serv.ids[,list(serv_id=unique(V1))])

# c.serv.ids in md$serv_id, in rd$member_id
setkey(c.serv.ids,serv_id)
setkey(md,serv_id)
setkey(rd,member_id)

# load in numbers and service ids
system.time(id2acc <-
            as.data.table(as.data.frame(ffd[c('SERV_ID','ACC_NBR')])))

nobs <- dim(id2acc)[1]
jump <- ceiling(nobs / 128)
jlist <- 0:127 * jump

# reduce to unique number/service id mappings
# just to check that each mapping is unique
res <- mclapply(jlist, mc.cores=64, function(x) {
    id2acc[(1+x):min(jump+x,nobs),1,by=list(SERV_ID,ACC_NBR)]
})

res.dt <- rbindlist(res)
id2acc.map <- res.dt[,sum(V1),by=list(SERV_ID,ACC_NBR)]
id2acc.map[,V1 := NULL]
ct.nums <- id2acc.map$ACC_NBR

# write out id/account number map for posterity...
saveRDS(id2acc.map, './Rds/id2number.Rds')

# find phone numbers that are in our data
system.time(both.idx <- ffwhich(ffd, CALLED_NBR %in% ct.nums &
                                  CALLING_NBR %in% ct.nums & CALLED_NBR !=
                                CALLING_NBR))
length(both.idx)

system.time(ct.all <- as.data.table(as.data.frame(ffd[both.idx,])))
dim(ct.all)

# merge in account numbers
ct.all[,c('ETL_TYPE_NAME','CALL_TYPE') := NULL]

# first for called numbers
setnames(id2acc.map,c('ACC_NBR','SERV_ID'),c('CALLED_NBR','CALLED_ID'))

setkey(id2acc.map, CALLED_NBR)
setkey(ct.all, CALLED_NBR)

ct.all <- merge(ct.all, id2acc.map, all.x=TRUE)

# then for calling numbers
setnames(id2acc.map,c('CALLED_NBR','CALLED_ID'),
         c('CALLING_NBR','CALLING_ID'))

setkey(id2acc.map, CALLING_NBR)
setkey(ct.all, CALLING_NBR)

ct.all <- merge(ct.all, id2acc.map, all.x=TRUE)

# generate call ids
system.time(ct.all[,cid := .GRP, by=list(CALLED_ID,CALLING_ID,ETL_TYPE_ID,START_TIME)])

# get master list of call ids
# let's provisionally kill convos that appear more than once for each person...
system.time(ct.all[,killconvo := 1:.N, by=list(cid,SERV_ID)])
table(ct.all$killconvo)
ct.all <- ct.all[killconvo==1]

# tag first obs by call id
system.time(ct.all[, tag := 1:.N, by=cid])
table(ct.all$tag)

system.time(ct.convos <- ct.all[tag==1])
ct.convos[,c('ACC_NBR','SERV_ID') := NULL]

called.serv <- ct.all[SERV_ID == CALLED_ID, list(cid, hascalled = TRUE)]
calling.serv <- ct.all[SERV_ID == CALLING_ID, list(cid, hascalling = TRUE)]

setkey(ct.convos,cid)
setkey(called.serv,cid)
setkey(calling.serv,cid)

ct.convos <- merge(ct.convos,called.serv, all.x=TRUE)
ct.convos <- merge(ct.convos,calling.serv, all.x=TRUE)

ct.convos[is.na(hascalled), hascalled := FALSE]
ct.convos[is.na(hascalling), hascalling := FALSE]

# merge in user start info
called.dates <- rd[,list(CALLED_ID = member_id, called_ocreate =
                         offer_create_dt, called_ostart = offer_start_dt,
                         called_start = start_dt, called_state = state_date)]

calling.dates <- rd[,list(CALLING_ID = member_id, calling_ocreate =
                          offer_create_dt, calling_ostart = offer_start_dt,
                          calling_start = start_dt, calling_state = state_date)]

setkey(called.dates, CALLED_ID)
setkey(calling.dates, CALLING_ID)

setkey(ct.convos,CALLED_ID)
system.time(ct.convos <- merge(ct.convos, called.dates, all.x=TRUE))

setkey(ct.convos,CALLING_ID)
system.time(ct.convos <- merge(ct.convos, calling.dates, all.x=TRUE))

# check whether times are bigger...
table(ct.convos[(!hascalled), list(START_TIME < called_ocreate)][,V1])
table(ct.convos[(!hascalled), list(START_TIME < called_ostart)][,V1])
table(ct.convos[(!hascalled), list(START_TIME < called_start)][,V1])
table(ct.convos[(!hascalled), list(START_TIME < called_state)][,V1])

table(ct.convos[(!hascalling), list(START_TIME < calling_ocreate)][,V1])
table(ct.convos[(!hascalling), list(START_TIME < calling_ostart)][,V1])
table(ct.convos[(!hascalling), list(START_TIME < calling_start)][,V1])
table(ct.convos[(!hascalling), list(START_TIME < calling_state)][,V1])

# so basically, we're boned
