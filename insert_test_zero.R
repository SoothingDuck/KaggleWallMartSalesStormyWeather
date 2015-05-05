library(RSQLite)
library(ggplot2)

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

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

# Lignes de test avec 0 item à l'année
df <- dbGetQuery(con,"
  select 
  T1.date as date,
  T1.store_nbr as store_nbr,
  T1.item_nbr as item_nbr,
  0 as units,
  T1.year as year,
  T1.month as month,
  T1.week as week
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
  T1.week as week
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

# Lignes de test avec 0 item au mois
df <- dbGetQuery(con,"
  select 
  T1.date as date,
  T1.store_nbr as store_nbr,
  T1.item_nbr as item_nbr,
  0 as units,
  T1.year as year,
  T1.month as month,
  T1.week as week
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
  week
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
T1.week as week
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

# All test 
df <- dbGetQuery(con, "
select
*
from
sales_all_test
")

dbDisconnect(con)
