library(RSQLite)
library(ggplot2)



con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

# weather aggregation
df <- dbGetQuery(con,"
  select 
T1.station_nbr as station_nbr,
T1.date as date,
T2.date as date_plus_two,
T3.date as date_plus_seven,

T1.tmax as tmax_day,
T1.tmin as tmin_day,
T1.tavg as tavg_day,
T1.depart as depart_day,     
T1.dewpoint as dewpoint_day,
T1.wetbulb as wetbulb_day,
T1.heat as heat_day,
T1.cool as cool_day,
T1.sunrise as sunrise_day,
T1.sunset as sunset_day,
T1.snowfall as snowfall_day,
T1.preciptotal as preciptotal_day,
T1.stnpressure as stnpressure_day,
T1.sealevel as sealevel_day,
T1.resultspeed as resultspeed_day,
T1.resultdir as resultdir_day,
T1.avgspeed as avgspeed_day ,

T2.tmax as tmax_day_plus_two,
T2.tmin as tmin_day_plus_two,
T2.tavg as tavg_day_plus_two,
T2.depart as depart_day_plus_two,     
T2.dewpoint as dewpoint_day_plus_two,
T2.wetbulb as wetbulb_day_plus_two,
T2.heat as heat_day_plus_two,
T2.cool as cool_day_plus_two,
T2.sunrise as sunrise_day_plus_two,
T2.sunset as sunset_day_plus_two,
T2.snowfall as snowfall_day_plus_two,
T2.preciptotal as preciptotal_day_plus_two,
T2.stnpressure as stnpressure_day_plus_two,
T2.sealevel as sealevel_day_plus_two,
T2.resultspeed as resultspeed_day_plus_two,
T2.resultdir as resultdir_day_plus_two,
T2.avgspeed as avgspeed_day_plus_two ,

T3.tmax as tmax_day_plus_seven,
T3.tmin as tmin_day_plus_seven,
T3.tavg as tavg_day_plus_seven,
T3.depart as depart_day_plus_seven,     
T3.dewpoint as dewpoint_day_plus_seven,
T3.wetbulb as wetbulb_day_plus_seven,
T3.heat as heat_day_plus_seven,
T3.cool as cool_day_plus_seven,
T3.sunrise as sunrise_day_plus_seven,
T3.sunset as sunset_day_plus_seven,
T3.snowfall as snowfall_day_plus_seven,
T3.preciptotal as preciptotal_day_plus_seven,
T3.stnpressure as stnpressure_day_plus_seven,
T3.sealevel as sealevel_day_plus_seven,
T3.resultspeed as resultspeed_day_plus_seven,
T3.resultdir as resultdir_day_plus_seven,
T3.avgspeed as avgspeed_day_plus_seven ,
                 
T2.tmax - T1.tmax as tmax_day_plus_two_diff,
T2.tmin - T1.tmin as tmin_day_plus_two_diff,
T2.tavg - T1.tavg as tavg_day_plus_two_diff,
T2.depart - T1.depart as depart_day_plus_two_diff,     
T2.dewpoint - T1.dewpoint as dewpoint_day_plus_two_diff,
T2.wetbulb - T1.wetbulb as wetbulb_day_plus_two_diff,
T2.heat - T1.heat as heat_day_plus_two_diff,
T2.cool - T1.cool as cool_day_plus_two_diff,
T2.sunrise - T1.sunrise as sunrise_day_plus_two_diff,
T2.sunset - T1.sunset as sunset_day_plus_two_diff,
T2.snowfall - T1.snowfall as snowfall_day_plus_two_diff,
T2.preciptotal - T1.preciptotal as preciptotal_day_plus_two_diff,
T2.stnpressure - T1.stnpressure as stnpressure_day_plus_two_diff,
T2.sealevel - T1.sealevel as sealevel_day_plus_two_diff,
T2.resultspeed - T1.resultspeed as resultspeed_day_plus_two_diff,
T2.resultdir - T1.resultdir as resultdir_day_plus_two_diff,
T2.avgspeed - T1.avgspeed as avgspeed_day_plus_two_diff ,

T3.tmax - T1.tmax as tmax_day_plus_seven_diff,
T3.tmin - T1.tmin as tmin_day_plus_seven_diff,
T3.tavg - T1.tavg as tavg_day_plus_seven_diff,
T3.depart - T1.depart as depart_day_plus_seven_diff,     
T3.dewpoint - T1.dewpoint as dewpoint_day_plus_seven_diff,
T3.wetbulb - T1.wetbulb as wetbulb_day_plus_seven_diff,
T3.heat - T1.heat as heat_day_plus_seven_diff,
T3.cool - T1.cool as cool_day_plus_seven_diff,
T3.sunrise - T1.sunrise as sunrise_day_plus_seven_diff,
T3.sunset - T1.sunset as sunset_day_plus_seven_diff,
T3.snowfall - T1.snowfall as snowfall_day_plus_seven_diff,
T3.preciptotal - T1.preciptotal as preciptotal_day_plus_seven_diff,
T3.stnpressure - T1.stnpressure as stnpressure_day_plus_seven_diff,
T3.sealevel - T1.sealevel as sealevel_day_plus_seven_diff,
T3.resultspeed - T1.resultspeed as resultspeed_day_plus_seven_diff,
T3.resultdir - T1.resultdir as resultdir_day_plus_seven_diff,
T3.avgspeed - T1.avgspeed as avgspeed_day_plus_seven_diff ,

T1.year as year_T1,
T1.month as month_T1,
T1.day as day_T1,
T2.year as year_T2,
T2.month as month_T2,
T2.day as day_T2
from
weather T1 left outer join
weather T2 on (
T1.station_nbr = T2.station_nbr and
T1.date = T2.date - (24*3600*2) and T2.station_nbr not in (5)
) left outer join
weather T3 on (
T1.station_nbr = T3.station_nbr and
T1.date = T3.date - (24*3600*7) and T2.station_nbr not in (5)
)
")

# dbRemoveTable(con, "weather_agg")
dbWriteTable(con, "weather_agg", df)

# index weather_agg
dbGetQuery(con,"create unique index weather_agg_station_nbr_idx on weather_agg (station_nbr, date)")


# weather aggregation
df <- dbGetQuery(con,"
select 
T1.station_nbr as station_nbr,
T1.date as date,

min(T2.date) as min_next_date,
max(T2.date) as max_next_date,

min(T3.date) as min_previous_date,
max(T3.date) as max_previous_date,

min(T2.tmax) as tmax_min_next_seven_days,
max(T2.tmax) as tmax_max_next_seven_days,
avg(T2.tmax) as tmax_avg_next_seven_days,

min(T2.tmin) as tmin_min_next_seven_days,
max(T2.tmin) as tmin_max_next_seven_days,
avg(T2.tmin) as tmin_avg_next_seven_days,

min(T2.tavg) as tavg_min_next_seven_days,
max(T2.tavg) as tavg_max_next_seven_days,
avg(T2.tavg) as tavg_avg_next_seven_days,

min(T2.depart) as depart_min_next_seven_days,
max(T2.depart) as depart_max_next_seven_days,
avg(T2.depart) as depart_avg_next_seven_days,

min(T2.dewpoint) as dewpoint_min_next_seven_days,
max(T2.dewpoint) as dewpoint_max_next_seven_days,
avg(T2.dewpoint) as dewpoint_avg_next_seven_days,

min(T2.wetbulb) as wetbulb_min_next_seven_days,
max(T2.wetbulb) as wetbulb_max_next_seven_days,
avg(T2.wetbulb) as wetbulb_avg_next_seven_days,

min(T2.heat) as heat_min_next_seven_days,
max(T2.heat) as heat_max_next_seven_days,
avg(T2.heat) as heat_avg_next_seven_days,

min(T2.cool) as cool_min_next_seven_days,
max(T2.cool) as cool_max_next_seven_days,
avg(T2.cool) as cool_avg_next_seven_days,

min(T2.sunrise) as sunrise_min_next_seven_days,
max(T2.sunrise) as sunrise_max_next_seven_days,
avg(T2.sunrise) as sunrise_avg_next_seven_days,

min(T2.sunset) as sunset_min_next_seven_days,
max(T2.sunset) as sunset_max_next_seven_days,
avg(T2.sunset) as sunset_avg_next_seven_days,

min(T2.snowfall) as snowfall_min_next_seven_days,
max(T2.snowfall) as snowfall_max_next_seven_days,
avg(T2.snowfall) as snowfall_avg_next_seven_days,

min(T2.preciptotal) as preciptotal_min_next_seven_days,
max(T2.preciptotal) as preciptotal_max_next_seven_days,
avg(T2.preciptotal) as preciptotal_avg_next_seven_days,

min(T2.stnpressure) as stnpressure_min_next_seven_days,
max(T2.stnpressure) as stnpressure_max_next_seven_days,
avg(T2.stnpressure) as stnpressure_avg_next_seven_days,

min(T2.sealevel) as sealevel_min_next_seven_days,
max(T2.sealevel) as sealevel_max_next_seven_days,
avg(T2.sealevel) as sealevel_avg_next_seven_days,

min(T2.resultspeed) as resultspeed_min_next_seven_days,
max(T2.resultspeed) as resultspeed_max_next_seven_days,
avg(T2.resultspeed) as resultspeed_avg_next_seven_days,

min(T2.resultdir) as resultdir_min_next_seven_days,
max(T2.resultdir) as resultdir_max_next_seven_days,
avg(T2.resultdir) as resultdir_avg_next_seven_days,

min(T2.avgspeed) as avgspeed_min_next_seven_days,
max(T2.avgspeed) as avgspeed_max_next_seven_days,
avg(T2.avgspeed) as avgspeed_avg_next_seven_days,

min(T3.tmax) as tmax_min_previous_seven_days,
max(T3.tmax) as tmax_max_previous_seven_days,
avg(T3.tmax) as tmax_avg_previous_seven_days,

min(T3.tmin) as tmin_min_previous_seven_days,
max(T3.tmin) as tmin_max_previous_seven_days,
avg(T3.tmin) as tmin_avg_previous_seven_days,

min(T3.tavg) as tavg_min_previous_seven_days,
max(T3.tavg) as tavg_max_previous_seven_days,
avg(T3.tavg) as tavg_avg_previous_seven_days,

min(T3.depart) as depart_min_previous_seven_days,
max(T3.depart) as depart_max_previous_seven_days,
avg(T3.depart) as depart_avg_previous_seven_days,

min(T3.dewpoint) as dewpoint_min_previous_seven_days,
max(T3.dewpoint) as dewpoint_max_previous_seven_days,
avg(T3.dewpoint) as dewpoint_avg_previous_seven_days,

min(T3.wetbulb) as wetbulb_min_previous_seven_days,
max(T3.wetbulb) as wetbulb_max_previous_seven_days,
avg(T3.wetbulb) as wetbulb_avg_previous_seven_days,

min(T3.heat) as heat_min_previous_seven_days,
max(T3.heat) as heat_max_previous_seven_days,
avg(T3.heat) as heat_avg_previous_seven_days,

min(T3.cool) as cool_min_previous_seven_days,
max(T3.cool) as cool_max_previous_seven_days,
avg(T3.cool) as cool_avg_previous_seven_days,

min(T3.sunrise) as sunrise_min_previous_seven_days,
max(T3.sunrise) as sunrise_max_previous_seven_days,
avg(T3.sunrise) as sunrise_avg_previous_seven_days,

min(T3.sunset) as sunset_min_previous_seven_days,
max(T3.sunset) as sunset_max_previous_seven_days,
avg(T3.sunset) as sunset_avg_previous_seven_days,

min(T3.snowfall) as snowfall_min_previous_seven_days,
max(T3.snowfall) as snowfall_max_previous_seven_days,
avg(T3.snowfall) as snowfall_avg_previous_seven_days,

min(T3.preciptotal) as preciptotal_min_previous_seven_days,
max(T3.preciptotal) as preciptotal_max_previous_seven_days,
avg(T3.preciptotal) as preciptotal_avg_previous_seven_days,

min(T3.stnpressure) as stnpressure_min_previous_seven_days,
max(T3.stnpressure) as stnpressure_max_previous_seven_days,
avg(T3.stnpressure) as stnpressure_avg_previous_seven_days,

min(T3.sealevel) as sealevel_min_previous_seven_days,
max(T3.sealevel) as sealevel_max_previous_seven_days,
avg(T3.sealevel) as sealevel_avg_previous_seven_days,

min(T3.resultspeed) as resultspeed_min_previous_seven_days,
max(T3.resultspeed) as resultspeed_max_previous_seven_days,
avg(T3.resultspeed) as resultspeed_avg_previous_seven_days,

min(T3.resultdir) as resultdir_min_previous_seven_days,
max(T3.resultdir) as resultdir_max_previous_seven_days,
avg(T3.resultdir) as resultdir_avg_previous_seven_days,

min(T3.avgspeed) as avgspeed_min_previous_seven_days,
max(T3.avgspeed) as avgspeed_max_previous_seven_days,
avg(T3.avgspeed) as avgspeed_avg_previous_seven_days
                 

from
weather T1 left outer join
weather T2 on (
T1.station_nbr = T2.station_nbr and
T1.date between (T2.date - (24*3600*7)) and (T2.date - (24*3600*1)) and
T2.station_nbr not in (5)
) left outer join
weather T3 on (
T1.station_nbr = T3.station_nbr and
T1.date between (T3.date + (24*3600*1)) and (T3.date + (24*3600*7)) and
T2.station_nbr not in (5)
)
group by 1,2

")


# dbRemoveTable(con, "weather_next_seven_days")
dbWriteTable(con, "weather_next_seven_days", df)

# index weather_next_seven_days
dbGetQuery(con,"create index weather_next_seven_days_station_nbr_idx on weather_next_seven_days (station_nbr, date)")

# Recuperation de toutes les lignes de test
df <- dbGetQuery(con,"
  select 
  *
  from
  sales
  where
  dataset = 'test'
")

# dbRemoveTable(con, "sales_all_test")
dbWriteTable(con, "sales_all_test", df)

# Recuperation de toutes les lignes de train
df <- dbGetQuery(con,"
  select 
  *
  from
  sales
  where
  dataset = 'train'
")

# dbRemoveTable(con, "sales_all_train")
dbWriteTable(con, "sales_all_train", df)

# Agrégation à l'année
df <- dbGetQuery(con,"
  select 
  store_nbr,
  item_nbr,
  year,
  sum(units) as total_units_year,
  avg(units) as avg_units_year
  from
  sales
  group by 1,2,3
")

# dbRemoveTable(con, "store_item_year_avg")
dbWriteTable(con, "store_item_year_avg", df)

# index store_item_year_avg
dbGetQuery(con,"create index store_item_year_avg_idx on store_item_year_avg (store_nbr, item_nbr)")

# Lignes de test avec 0 item à l'année
df <- dbGetQuery(con,"
  select 
  T1.date as date,
  T1.store_nbr as store_nbr,
  T1.item_nbr as item_nbr,
  0 as units,
  'test' as dataset,
  T1.year as year,
  T1.month as month,
  T1.week as week,
  T1.day as day,
  T1.week_day as week_day
  from
  sales_all_test T1 inner join store_item_year_avg T2 on (
    T1.store_nbr = T2.store_nbr and
    T1.item_nbr = T2.item_nbr and
    T1.year = T2.year
  )
  where
  T1.dataset = 'test'
  and
  T2.total_units_year = 0
")

# dbRemoveTable(con, "sales_zero_test")
dbWriteTable(con, "sales_zero_test", df)

# Lignes de train avec 0 item à l'année
df <- dbGetQuery(con,"
                 select 
                 T1.date as date,
                 T1.store_nbr as store_nbr,
                 T1.item_nbr as item_nbr,
                 0 as units,
                 'train' as dataset,
                 T1.year as year,
                 T1.month as month,
                 T1.week as week,
                 T1.day as day,
                 T1.week_day as week_day
                 from
                 sales_all_train T1 inner join store_item_year_avg T2 on (
                 T1.store_nbr = T2.store_nbr and
                 T1.item_nbr = T2.item_nbr and
                 T1.year = T2.year
                 )
                 where
                 T1.dataset = 'train'
                 and
                 T2.total_units_year = 0
                 ")

# dbRemoveTable(con, "sales_zero_train")
dbWriteTable(con, "sales_zero_train", df)

# Delete 
df <- dbGetQuery(con, "
  select
  T1.date as date,
  T1.store_nbr as store_nbr,
  T1.item_nbr as item_nbr,
  T1.units as units,
  T1.dataset as dataset,
  T1.year as year,
  T1.month as month,
  T1.week as week,
  T1.day as day,
  T1.week_day as week_day
  from
  sales_all_train T1 left outer join
  sales_zero_train T2 on (
    T1.date = T2.date and
    T1.store_nbr = T2.store_nbr and
    T1.item_nbr = T2.item_nbr and
    T1.year = T2.year and
    T1.month = T2.month and
    T1.week = T2.week and
    T1.day = T2.day
  )
  where
  T2.date is null
")

dbRemoveTable(con, "sales_all_train")
dbWriteTable(con, "sales_all_train", df)

# Agrégation au mois
df <- dbGetQuery(con,"
  select 
  store_nbr,
  item_nbr,
  year,
  month,
  sum(units) as total_units_month,
  avg(units) as avg_units_month
  from
  sales
  group by 1,2,3,4
")

# dbRemoveTable(con, "store_item_month_avg")
dbWriteTable(con, "store_item_month_avg", df)

# index store_item_month_avg
dbGetQuery(con,"create index store_item_month_avg_idx on store_item_month_avg (store_nbr, item_nbr)")

# Lignes de test avec 0 item au mois
df <- dbGetQuery(con,"
  select 
  T1.date as date,
  T1.store_nbr as store_nbr,
  T1.item_nbr as item_nbr,
  0 as units,
  T1.year as year,
  T1.month as month,
  T1.week as week,
  T1.day as day,
  T1.week_day as week_day
  from
  sales_all_test T1 inner join store_item_month_avg T2 on (
    T1.store_nbr = T2.store_nbr and
    T1.item_nbr = T2.item_nbr and
    T1.year = T2.year and
    T1.month = T2.month
  )
  where
  T1.dataset = 'test'
  and
  T2.total_units_month = 0
  union
  select
  date,
  store_nbr,
  item_nbr,
  units,
  year,
  month,
  week,
  day,
  week_day
  from
  sales_zero_test
")

dbRemoveTable(con, "sales_zero_test")
dbWriteTable(con, "sales_zero_test", df)

# Delete 
df <- dbGetQuery(con, "
select
T1.date as date,
T1.store_nbr as store_nbr,
T1.item_nbr as item_nbr,
T1.units as units,
T1.dataset as dataset,
T1.year as year,
T1.month as month,
T1.week as week,
T1.day as day,
T1.week_day as week_day
from
sales_all_test T1 left outer join
sales_zero_test T2 on (
T1.date = T2.date and
T1.store_nbr = T2.store_nbr and
T1.item_nbr = T2.item_nbr
)
where
T2.date is null
")

dbRemoveTable(con, "sales_all_test")
dbWriteTable(con, "sales_all_test", df)

# Lignes de train avec 0 item au mois
df <- dbGetQuery(con,"
  select 
                 T1.date as date,
                 T1.store_nbr as store_nbr,
                 T1.item_nbr as item_nbr,
                 0 as units,
                 T1.year as year,
                 T1.month as month,
                 T1.week as week,
                 T1.day as day,
                 T1.week_day as week_day
                 from
                 sales_all_train T1 inner join store_item_month_avg T2 on (
                 T1.store_nbr = T2.store_nbr and
                 T1.item_nbr = T2.item_nbr and
                 T1.year = T2.year and
                 T1.month = T2.month
                 )
                 where
                 T1.dataset = 'train'
                 and
                 T2.total_units_month = 0
                 union
                 select
                 date,
                 store_nbr,
                 item_nbr,
                 units,
                 year,
                 month,
                 week,
                 day,
                 week_day
                 from
                 sales_zero_train
                 ")

dbRemoveTable(con, "sales_zero_train")
dbWriteTable(con, "sales_zero_train", df)

# Delete 
df <- dbGetQuery(con, "
                 select
                 T1.date as date,
                 T1.store_nbr as store_nbr,
                 T1.item_nbr as item_nbr,
                 T1.units as units,
                 T1.dataset as dataset,
                 T1.year as year,
                 T1.month as month,
                 T1.week as week,
                 T1.day as day,
                 T1.week_day as week_day
                 from
                 sales_all_train T1 left outer join
                 sales_zero_train T2 on (
                 T1.date = T2.date and
                 T1.store_nbr = T2.store_nbr and
                 T1.item_nbr = T2.item_nbr
                 )
                 where
                 T2.date is null
                 ")

dbRemoveTable(con, "sales_all_train")
dbWriteTable(con, "sales_all_train", df)

# All test 
df <- dbGetQuery(con, "
select
*
from
sales_all_test
")

# Creation d'index
dbGetQuery(con,"create unique index sales_all_train_store_nbr_idx on sales_all_train (store_nbr, item_nbr, date)")
dbGetQuery(con,"create unique index sales_zero_train_store_nbr_idx on sales_zero_train (store_nbr, item_nbr, date)")
dbGetQuery(con,"create unique index sales_all_test_store_nbr_idx on sales_all_test (store_nbr, item_nbr, date)")
dbGetQuery(con,"create unique index sales_zero_test_store_nbr_idx on sales_zero_test (store_nbr, item_nbr, date)")


dbDisconnect(con)
