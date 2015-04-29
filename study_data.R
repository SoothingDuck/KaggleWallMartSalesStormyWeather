library(RSQLite)
library(ggplot2)

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

weather.df <- dbGetQuery(con, "
select
t1.station_nbr,
t1.date as date_j,
t2.date as date_j_1,
t1.tavg as tavg_j,
t2.tavg as tavg_j_1
from
weather t1,
weather t2
where
t1.station_nbr =  t2.station_nbr
and
t1.date = (t2.date - (24*3600))
and
t1.station_nbr = 13
                         ")

ggplot(weather.df) + geom_point(aes(x=date, y=preciptotal))

df <- dbGetQuery(con,"
select
s.date,
s.store_nbr,
sum(s.units) as total_item
from
sales s,
key k,
weather w
where
s.dataset = 'train' and
s.store_nbr = k.store_nbr and
k.station_nbr = w.station_nbr and
s.date = w.date and
w.station_nbr = 13
group by 1,2
having total_item < 3000
                 ")

ggplot(df) + geom_point(aes(x=date, y=total_item)) + facet_wrap(~ store_nbr)

dbDisconnect(con)