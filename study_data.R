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

df <- dbGetQuery(con,"
select
s.dataset,
count(*)
from
sales s
group by 1
                 ")

sales.df <- dbGetQuery(con,"
  select * from sales
")




for(item_number in 1:111) {
  df <- dbGetQuery(con,paste("
                          select
                             s.date,
                             s.year,
                             s.month,
                             s.store_nbr,
                             s.dataset,
                             coalesce(s.units, -1) as units
                             from
                             sales s left outer join sales_zero_test t on
                             (  
                                s.date = t.date and
                                s.item_nbr = t.item_nbr and
                                s.store_nbr = t.store_nbr
                             ) left outer join sales_zero_train u on
                             (
                                s.date = u.date and
                                s.item_nbr = u.item_nbr and
                                s.store_nbr = u.store_nbr
                             ),
                             key k,
                             weather w
                             where
                             t.date is null and
                             u.date is null and
                             s.store_nbr = k.store_nbr and
                             k.station_nbr = w.station_nbr and
                             s.date = w.date and
                             s.item_nbr = ", item_number," and
                             coalesce(s.units, -1) < 2000
                             ", sep = ""))

  sec.2013 <- 1325376000 + (365*24*3600)
  sec.2014 <- 1325376000 + (365*24*3600)*2
  
  g <- ggplot(df) + 
    geom_point(aes(x=date, y=units, colour=dataset)) + 
    facet_wrap( ~ store_nbr) + 
    theme_bw() +
    geom_vline(xintercept=sec.2013) + 
    geom_vline(xintercept=sec.2014)
  
  ggsave(filename = file.path("pdf", paste("store_item_", item_number, ".pdf", sep = "")), plot = g, scale = 1.5)

}

dbDisconnect(con)
