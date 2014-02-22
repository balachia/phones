library(data.table)
library(ffbase)
library(lubridate)
library(parallel)

rm(list=ls())

setwd('/archive/gsb/vashevko/CT/')

ffdfns <- load.ffdf('./ffdb/calls')
ffd <- ffdfns$ffd

open(ffd)

system.time(days <- data.table(start_time = ffd[,'START_TIME']))
system.time(days <- days[,list(day=unique(floor(julian(start_time))))])

days <- days[order(day),day]

# days <- days[1:50]

ptm <- proc.time()
res <- mclapply(days, mc.cores=64, mc.preschedule=FALSE,
    function (x) {
        l.ptm <- proc.time()
        system.time(
            idx <- ffwhich(ffd,floor(julian(START_TIME)) == x))

        system.time(
            d.dt <- as.data.table(as.data.frame(ffd[idx,])))
        cat(x - days[1] + 1, '::', proc.time() - l.ptm, '\n')

        # kill self-convos
        d.dt <- d.dt[CALLED_NBR != CALLING_NBR]
        
        # filter out duplicate conversations
        # tag unique element in each convo...
        d.dt[,c('tag', 'cid', 'csize') := list(1:.N, .GRP, .N),
             by=list(CALLED_NBR, CALLING_NBR, ETL_TYPE_ID, START_TIME)]

        d.dt <- d.dt[tag == 1, !'tag', with=FALSE]

        d.dt
    })
print(proc.time() - ptm)

print(object.size(res), units='auto')

# write out the fucker to some file
# first kill old db
unlink('./ffdb/calls-dedup/', recursive=TRUE)

print('bind results')
print(system.time(res.dt <- rbindlist(res)))
res.dt[,c('SERV_ID','ACC_NBR','cid') := NULL]
setnames(res.dt, 'csize', 'CONV_SIZE')

print('write results')
print(system.time(ffd.dd <- as.ffdf(res.dt)))


# ffd.dd <- NULL
# lapply(res, function (x) {
#         ptm <- proc.time()
#         if(is.null(ffd.dd)) {
#             ffd.dd <- as.ffdf(x)
#         } else {
#             ffd.dd <- ffdfappend(ffd.dd, x)
#         }
#         print(proc.time() - ptm)
#         0
#     })
# 
# write out the file
print(system.time(save.ffdf(ffd.dd, dir='./ffdb/calls-dedup/', overwrite=TRUE)))
close(ffd)
close(ffd.dd)
