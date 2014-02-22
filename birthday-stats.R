library(data.table)
library(ffbase)
library(parallel)
library(lubridate)

rm(list=ls())

# useful function
`%+na%` <- function(x,y) {ifelse( is.na(x), y, ifelse( is.na(y), x, x+y) )}

setwd('/archive/gsb/vashevko/CT/')

ffdfns <- load.ffdf('./ffdb/calls-dedup')
ffd <- ffdfns$ffd.dd
open(ffd)

rd <- readRDS('./Rds/demos-raw-procd.Rds')
md <- readRDS('./Rds/demos-more-procd.Rds')
id2num <- readRDS('./Rds/id2number.Rds')

u.nums <- id2num[,ACC_NBR]
u.nums <- u.nums[order(u.nums)]

# random sample
# set.seed(1)
# u.nums <- u.nums[runif(length(u.nums)) < 0.01]

ec <- ecdf(u.nums)                     # poor man's progress bar 

# md[,bd := ymd(birthday)]

# how to proceed....
# parallelize by user?
# yeah, probably

# TODO: number of friends total per user
# TODO: number of friends per month? week? day?
# TODO: churn somehow?

# let's try to read the whole thing into memory!
print(system.time(all.calls <- as.data.table(as.data.frame(ffd))))
print(object.size(all.calls), units='auto')

# let's try reducing the top level table...
# print(system.time(pairs <- all.calls[1:5e4, list(n = .N,
#                                             mintime = min(START_TIME),
#                                             maxtime = max(START_TIME)),
#                                             by=list(CALLED_NBR, CALLING_NBR)]))

# ugh, this shit is slow
# let's try to process things by day
all.calls[, day := floor(julian(START_TIME))]
days <- all.calls[, unique(day)]
days <- days[order(days)]

res <- mclapply(days, mc.cores=64, mc.preschedule=FALSE,
    function (cday) {
        cat('S', cday - days[1] + 1, '\n')
        out <- all.calls[day == cday, list(n = .N,
                                            mintime = min(START_TIME),
                                            maxtime = max(START_TIME)),
                by=list(CALLED_NBR, CALLING_NBR)]

        cat('E', cday - days[1] + 1, '\n')
        out
    })

# first make sure everything is sorted
ptm <- proc.time()
res.acc <- mclapply(res, mc.cores=64, mc.preschedule=FALSE, function (x) {
        cat(dim(x), '\n')
        setkey(x, CALLED_NBR, CALLING_NBR)
        x
    })
print(proc.time() - ptm)

# res.acc <- res
ptm.tot <- proc.time()
itercount <- 1
while (length(res.acc) > 1) {
    cat('Iteration', itercount, '\n')
    cat('Length:', length(res.acc), '\n')

    ptm <- proc.time()
    N <- length(res.acc)
    res.acc <- mclapply(seq(1,N,2), mc.cores=64, mc.preschedule=FALSE,
        function (i) {
            if (i == N) {
                return(res.acc[[i]])
            } else {
                m.res <- merge(res.acc[[i]], res.acc[[i+1]], all=TRUE)

                m.res[, c('n', 'mintime', 'maxtime') :=
                      list(n.x %+na% n.y,
                           pmin(mintime.x, mintime.y, na.rm=TRUE),
                           pmax(maxtime.x, maxtime.y, na.rm=TRUE))]

                m.res[, c('n.x', 'n.y', 'mintime.x', 'mintime.y', 'maxtime.x', 'maxtime.y')
                      := NULL]

                setkey(m.res, CALLED_NBR, CALLING_NBR)

                return (m.res)
            }
        })
    
    print(proc.time() - ptm)
    itercount <- itercount + 1
}
print(proc.time() - ptm.tot)

# write out results
acc <- res.acc[[1]]
saveRDS(acc, 'Rds/pair-stats.Rds')

# separate into in/out networks
ct.ins <- acc[CALLED_NBR %in% u.nums]
ct.outs <- acc[CALLING_NBR %in% u.nums]

setnames(ct.ins, c('CALLED_NBR', 'CALLING_NBR'), c('ego', 'alter'))
setnames(ct.outs, c('CALLED_NBR', 'CALLING_NBR'), c('alter', 'ego'))

setkey(ct.ins,ego,alter)
setkey(ct.outs,ego,alter)

# merge into bi-directional ego-networks
ct.alls <- merge(ct.ins,ct.outs,all=TRUE)

ct.alls[, c('n', 'mintime', 'maxtime') :=
        list(n.x %+na% n.y,
             pmin(mintime.x, mintime.y, na.rm=TRUE),
             pmax(maxtime.x, maxtime.y, na.rm=TRUE))]

ct.alls[, c('n.x', 'n.y', 'mintime.x', 'mintime.y', 'maxtime.x', 'maxtime.y')
        := NULL]

# cut out weird phone numbers
ct.alls <- ct.alls[floor(alter / 1e8) != 0]

# merge in service ids
setnames(id2num, names(id2num), tolower(names(id2num)))
setnames(ct.alls, 'ego', 'acc_nbr')

setkey(ct.alls, acc_nbr)
setkey(id2num, acc_nbr)

ct.alls <- merge(ct.alls, id2num, all.x=TRUE)

# look at # friends
nfr <- ct.alls[,.N, by=serv_id]
