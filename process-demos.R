library(data.table)
library(lubridate)

rm(list=ls())

setwd('/archive/gsb/vashevko/CT/Rds')

rd <- readRDS('demos-raw.Rds')
md <- readRDS('demos-more.Rds')

rd.ints <- c('OFFER_ID', 'OFFER_SPEC_ID', 'OFFER_STATUS_CD',
             'MEMBER_ID', 'MEMBER_STATUS_CD', 'MONEY', 'ACCT_ID',
             'BILL_MODE_ID', 'STATE', 'ADDR_ID', 'TML_ID', 'OLD_D',
             'GENDER', 'KD_MEMBER_ID')
rd.dates <- c('OFFER_CREATE_DT', 'OFFER_START_DT', 'START_DT',
             'STATE_DATE')
md.ints <- c('SERV_ID','location','nonmigrant','Age','Gender','Birthday')

# make dates
lapply(rd.dates, function (x) {
    print(x)
#    rd[, paste0(x, '_parse') := parse_date_time(get(x), c('mdy H:M:S',
#                               'mdy'), tz='UTC'), with=FALSE]
    rd[, x := parse_date_time(get(x), c('mdy H:M:S', 'mdy'), tz='UTC'), with=FALSE]
    dim(rd[is.na(get(x))])
})

# make ints
lapply(rd.ints, function (x) {
    print(x)
    # rd[, paste0(x, '_parse') := as.numeric(get(x)), with=FALSE]
    rd[, x := as.numeric(get(x)), with=FALSE]
    dim(rd[is.na(get(x))])
})

# make ints for md
lapply(md.ints, function (x) {
    print(x)
    # md[, paste0(x, '_parse') := as.numeric(get(x)), with=FALSE]
    md[, x := as.numeric(get(x)), with=FALSE]
    dim(md[is.na(get(x))])
})

# dick around with names
setnames(md, c('Lan Address', 'Lan Address Area'),
         c('lan_address','lan_address_area'))

setnames(rd, names(rd), tolower(names(rd)))
setnames(md, names(md), tolower(names(md)))

# reprocess birthdays
md[, bd := ymd(birthday)]

saveRDS(rd, 'demos-raw-procd.Rds')
saveRDS(md, 'demos-more-procd.Rds')
