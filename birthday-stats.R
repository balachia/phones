library(data.table)
library(ffbase)
library(parallel)
library(lubridate)
library(zoo)

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

# make sure shit is ready to merge
setnames(rd,'member_id','serv_id')
setkey(rd,serv_id)
setkey(md,serv_id)

# TODO: number of friends total per user
# TODO: number of friends per month? week? day?
# TODO: churn somehow?

if (file.exists('Rds/pair-stats.Rds')) {
    acc <- readRDS('Rds/pair-stats.Rds')
} else {
    # let's try to read the whole thing into memory!
    print(system.time(all.calls <- as.data.table(as.data.frame(ffd))))
    print(object.size(all.calls), units='auto')

    # ugh, this shit is slow
    # let's try to process things by day
    all.calls[, day := floor(julian(START_TIME))]
    days <- all.calls[, unique(day)]
    days <- days[order(days)]

    ptm <- proc.time()
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
    print(proc.time() - ptm)

    # first make sure everything is sorted
    ptm <- proc.time()
    res.acc <- mclapply(res, mc.cores=64, mc.preschedule=FALSE, function (x) {
                        cat(dim(x), '\n')
                        setkey(x, CALLED_NBR, CALLING_NBR)
                        x
                    })
    print(proc.time() - ptm)

    # now we run a parallel reduce
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
}

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
nfr <- ct.alls[n > 10,
               list(nfr=.N,
                    nmob=sum(floor(alter / 1e8) %in% 130:189)), by=serv_id]
setkey(nfr,serv_id)

nfr <- merge(nfr, md, all.x=TRUE)

ggplot(nfr[age %in% 17:19], aes(yday(bd), nfr)) + geom_point(alpha=0.1) + scale_y_log10() + 
    stat_smooth() + labs(x='Birthday (Day of Year)', y='# Friends (log)')
ggsave('~/proj/ct/plots/nfr_day.png')
print(summary(gam(nfr ~ s(yday(bd)), data=nfr[age %in% 17:19])))
print(summary(gam(log(nfr) ~ s(yday(bd)), data=nfr[age %in% 17:19])))

ggplot(nfr[age %in% 17:19], aes(factor(month(bd), levels=1:12, labels=month.name), nfr)) + 
    geom_violin(aes(fill=factor(month(bd)))) + scale_y_log10() + 
    geom_jitter(alpha=0.1) + stat_smooth(aes(group=1)) + 
    labs(x='Birth Month', y='# Friends (log)') 
ggsave('~/proj/ct/plots/nfr_month.png')

ggplot(nfr[age %in% 17:19], aes(factor(month(bd), levels=1:12, labels=month.name), nfr)) + 
    geom_boxplot(aes(fill=factor(month(bd)))) + scale_y_log10() + 
    geom_jitter(alpha=0.1) + stat_smooth(aes(group=1)) + 
    labs(x='Birth Month', y='# Friends (log)') 
ggsave('~/proj/ct/plots/nfr_month_bp.png')
print(summary(gam(nfr ~ as.factor(month(bd)), data=nfr[age %in% 17:19])))
print(summary(gam(log(nfr) ~ as.factor(month(bd)), data=nfr[age %in% 17:19])))

# look at time to get friends
ct.alls <- ct.alls[order(serv_id,mintime)]
ct.alls[, c('nfr','cfr') := list(.N,
                                 .I - .I[1] + 1), by=serv_id]
ct.alls[,ffr := cfr / nfr]

# merge in birthday data
setkey(ct.alls, serv_id)
ct.alls <- merge(ct.alls, md, all.x=TRUE)

days <- ct.alls[,unique(day)]
users <- ct.alls[age %in% 17:19, unique(serv_id)]
ud.keys <- as.data.table(expand.grid(users,days))
setnames(ud.keys, c('Var1','Var2'), c('serv_id', 'day'))
setkey(ud.keys, serv_id, day)

ct.alls[,bmonth := month(bd)]
ct.alls[,day := floor(julian(mintime))]

ct.alls[,c('location','migrant','maxtime','lan_address','lan_address_area',
           'foxconn','status','where','nonmigrant') := NULL]

rct.alls <- ct.alls[age %in% 17:19,list(nfr = mean(nfr),
                                        cfr = max(cfr),
                                        age = mean(age),
                                        bmonth = mean(bmonth)), by=list(serv_id,day)]
setkey(rct.alls, serv_id, day)
ct.byday <- merge(ud.keys, rct.alls, all.x=TRUE)

# correct some info
ct.byday[,c('nfr','age','bmonth') := list(mean(nfr, na.rm=TRUE),
                                          mean(age, na.rm=TRUE),
                                          mean(bmonth, na.rm=TRUE)),
            by=serv_id]
ct.byday[,cfr := na.locf(cfr, na.rm=FALSE), by=serv_id]
ct.byday[is.na(cfr),cfr := 0]
ct.byday[, ffr := cfr / nfr]

# cheapo way: look at average fraction of friends achieved per day
rct.byday <- ct.byday[,list(n=.N,
                            nfr = sum(nfr),
                            cfr = sum(cfr),
                            ffr = mean(ffr)),
                        by=list(bmonth,day)]
rct.byday[,fw_ffr := cfr / nfr]

ggplot(rct.byday, aes(as.Date(day), ffr, color=factor(bmonth, levels=1:12, labels=month.name))) + 
    geom_line() + scale_x_date() + 
    labs(x='Day of Observation', y='Fraction of Friends Made (User-Weighted)') + 
    scale_color_discrete(name='Birth Month')
ggsave('~/proj/ct/plots/ffr_track.png')
ggplot(rct.byday, aes(as.Date(day), fw_ffr, color=factor(bmonth, levels=1:12, labels=month.name))) + 
    geom_line() + scale_x_date() + 
    labs(x='Day of Observation', y='Fraction of Friends Made (Friend-Weighted)') + 
    scale_color_discrete(name='Birth Month')
ggsave('~/proj/ct/plots/fwffr_track.png')

ggplot(rct.byday, aes(as.Date(day), cfr/n, color=factor(bmonth, levels=1:12, labels=month.name))) + 
    geom_line() + scale_x_date() + 
    labs(x='Day of Observation', y='Number of Friends Made/User') + 
    scale_color_discrete(name='Birth Month')
ggsave('~/proj/ct/plots/cfr_track.png')
