library(RSQLite)
library(ggplot2)
library(parallel)
library(gbm)

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

df <- dbGetQuery(con, "
select
*
from
sales_all_test
")

dbDisconnect(con)

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

df.u <- unique(df[, c("store_nbr", "item_nbr")])

for(i in 1:nrow(df.u)) {
  store_nbr <- df.u[i, "store_nbr"]
  item_nbr <- df.u[i, "item_nbr"]
  
  cat("Generation model store", store_nbr, "item", item_nbr, "...\n")
  
  gbm.filename <- file.path("DATA", paste("gbm_store_nbr_", store_nbr, "_item_nbr_", item_nbr, ".RData", sep = ""))
  
  sql <- paste("
  
  select
  T1.units,
  T1.year,
  T1.month,
  T1.week,
  T3.tmax,
  T3.tmin,
  T3.tavg,
  T3.depart,
  T3.dewpoint,
  T3.wetbulb,
  T3.heat,
  T3.cool,
  T3.sunrise,
  T3.sunset,
  T3.snowfall,
  T3.preciptotal,
  T3.stnpressure,
  T3.sealevel,
  T3.resultspeed,
  T3.resultdir,
  T3.avgspeed
  from 
  sales T1 inner join
  key T2 on (T1.store_nbr = T2.store_nbr) inner join
  weather T3 on (T2.station_nbr = T3.station_nbr)
  where
  T1.dataset = 'train' and
  T1.date = T3.date and
  T1.store_nbr = ", store_nbr," and
  T1.item_nbr = ", item_nbr, "
  ", sep = "")
  
  train.df <- dbGetQuery(con, sql)

  indice.cols.to.keep <- as.integer(which(! apply(train.df, 2, function(x) { all(is.na(x)) })))
  
  train.df <- train.df[, indice.cols.to.keep]
  
  gbm.model <- gbm(
      units ~ .,
      data = train.df,
      distribution = "gaussian",
      n.trees = 10000,
      interaction.depth = 2,
      n.minobsinnode = 5,
      shrinkage = 0.001,
      bag.fraction = 0.5,
      train.fraction = 0.95,
      verbose = TRUE
    )
  
  prediction <- predict(gbm.model, newdata=train.df)
  
  save(gbm.model, file = gbm.filename)
}

dbDisconnect(con)
