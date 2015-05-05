library(RSQLite)
library(ggplot2)
library(parallel)
library(gbm)
library(lubridate)

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
  T1.day,
  T1.week_day,
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
  train.df$year <- factor(train.df$year)
  train.df$month <- factor(train.df$month)
  train.df$week_day <- factor(train.df$week_day)
  train.df$ecart <- with(train.df, tmax - tmin)
    
  gbm.model <- gbm(
      units ~ . - day,
      data = train.df,
      distribution = "gaussian",
      n.trees = 10000,
      interaction.depth = 5,
      n.minobsinnode = 5,
      shrinkage = 0.001,
      bag.fraction = 0.9,
      train.fraction = 0.95,
      verbose = TRUE
    )
  
  prediction <- predict(gbm.model, newdata=train.df)
  
  save(gbm.model, file = gbm.filename)
}

dbDisconnect(con)

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

test.df <- dbGetQuery(con, "
select
*
from
sales_all_test
")

result <- data.frame()
df.u <- unique(test.df[, c("store_nbr", "item_nbr")])

for(i in 1:nrow(df.u)) {
  store_nbr <- df.u[i, "store_nbr"]
  item_nbr <- df.u[i, "item_nbr"]

  cat("Evalutation model store", store_nbr, "item", item_nbr, "...\n")
  
  gbm.filename <- file.path("DATA", paste("gbm_store_nbr_", store_nbr, "_item_nbr_", item_nbr, ".RData", sep = ""))
  
  sql <- paste("
               
               select
               T1.date,
               T1.year,
               T1.month,
               T1.week,
               T1.day,
               T1.week_day,
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
               sales_all_test T1 inner join
               key T2 on (T1.store_nbr = T2.store_nbr) inner join
               weather T3 on (T2.station_nbr = T3.station_nbr)
               where
               T1.dataset = 'test' and
               T1.date = T3.date and
               T1.store_nbr = ", store_nbr," and
               T1.item_nbr = ", item_nbr, "
               ", sep = "")
  
  subset.test.df <- dbGetQuery(con, sql)
  
  subset.test.df$year <- factor(subset.test.df$year)
  subset.test.df$month <- factor(subset.test.df$month)
  subset.test.df$week_day <- factor(subset.test.df$week_day)
  subset.test.df$ecart <- with(subset.test.df, tmax - tmin)
  
  load(gbm.filename)
  
  prediction.units <- predict(gbm.model, newdata=subset.test.df)
  
  result <- rbind(
    result,
    data.frame(
      date=subset.test.df$date,
      store_nbr=store_nbr,
      item_nbr=item_nbr,
      units=prediction.units,
      year=subset.test.df$year,
      month=subset.test.df$month,
      week=subset.test.df$week,
      day=subset.test.df$day,
      week_day=subset.test.df$week_day
      )
    )
}

dbDisconnect(con)

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

other.test.df <- dbGetQuery(con,"
  select 
  *
  from
  sales_zero_test
")

dbDisconnect(con)

all.df <- rbind(result, other.test.df)
sample.submission <- read.csv(file.path("DATA", "sampleSubmission.csv"))

units <- all.df$units
dates_str <- strftime(origin + all.df$date, "%Y-%m-%d")
id <- paste(all.df$store_nbr, all.df$item_nbr, dates_str, sep = "_")

submission <- data.frame(
  id=id,
  units=ifelse(units < 0, 0, units)
  )

submission <- submission[match(sample.submission$id, submission$id),]

write.csv(submission, file=file.path("DATA","gbm_try_submission.csv"), 
          row.names = FALSE,
          quote = FALSE)

