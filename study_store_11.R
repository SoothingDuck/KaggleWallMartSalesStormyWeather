
library(RSQLite)
library(ggplot2)
library(parallel)
library(gbm)
library(lubridate)
library(plyr)
library(reshape2)

rmsle <- function(y, ypred) {
  return(sqrt(mean((log(y+1)-log(ypred+1))**2)))
}

##########################################################
########## Evaluation modele
##########################################################
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


store_nbr <- 11
# item_nbr <- df.u[i, "item_nbr"]

result <- data.frame()

for(item_nbr in as.integer(df.u$item_nbr[df.u$store_nbr == 11])) {
  
  cat("Generation model store", store_nbr, "item", item_nbr, "...\n")
  
  ###########################################
  ############# Train 2013
  ###########################################
  sql <- paste("
  select
  T1.units,
  
  T1.store_nbr,
  T1.item_nbr,
  
  T1.week_day,
  T1.year,
  T1.month,
  T1.day,
  T1.date,
  
  T3.tmax_day,
  T3.tmin_day,
  T3.tavg_day,
  T3.depart_day,
  T3.dewpoint_day,
  T3.wetbulb_day,
  T3.heat_day,
  T3.cool_day,
  T3.sunrise_day,
  T3.sunset_day,
  T3.snowfall_day,
  T3.preciptotal_day,
  T3.stnpressure_day,
  T3.sealevel_day,
  T3.resultspeed_day,
  T3.resultdir_day,
  T3.avgspeed_day,
  
  T3.tmax_day_plus_two,
  T3.tmin_day_plus_two,
  T3.tavg_day_plus_two,
  T3.depart_day_plus_two,
  T3.dewpoint_day_plus_two,
  T3.wetbulb_day_plus_two,
  T3.heat_day_plus_two,
  T3.cool_day_plus_two,
  T3.sunrise_day_plus_two,
  T3.sunset_day_plus_two,
  T3.snowfall_day_plus_two,
  T3.preciptotal_day_plus_two,
  T3.stnpressure_day_plus_two,
  T3.sealevel_day_plus_two,
  T3.resultspeed_day_plus_two,
  T3.resultdir_day_plus_two,
  T3.avgspeed_day_plus_two,
  
  T3.tmax_day_plus_two_diff,
  T3.tmin_day_plus_two_diff,
  T3.tavg_day_plus_two_diff,
  T3.depart_day_plus_two_diff,
  T3.dewpoint_day_plus_two_diff,
  T3.wetbulb_day_plus_two_diff,
  T3.heat_day_plus_two_diff,
  T3.cool_day_plus_two_diff,
  T3.sunrise_day_plus_two_diff,
  T3.sunset_day_plus_two_diff,
  T3.snowfall_day_plus_two_diff,
  T3.preciptotal_day_plus_two_diff,
  T3.stnpressure_day_plus_two_diff,
  T3.sealevel_day_plus_two_diff,
  T3.resultspeed_day_plus_two_diff,
  T3.resultdir_day_plus_two_diff,
  T3.avgspeed_day_plus_two_diff,
  
  T4.total_units_year,
  T4.avg_units_year,
  
  T5.total_units_month,
  T5.avg_units_month,
  
  T4.total_units_year - T5.total_units_month as total_units_year_month_diff,
  T4.avg_units_year - T5.avg_units_month as avg_units_year_month_diff
  
  from 
  sales T1 inner join
  key T2 on (T1.store_nbr = T2.store_nbr) inner join
  weather_agg T3 on (T2.station_nbr = T3.station_nbr) inner join
  store_item_year_avg T4 on (
  T1.store_nbr = T4.store_nbr and
  T1.item_nbr = T4.item_nbr and
  T1.year = T4.year
  ) inner join
  store_item_month_avg T5 on (
  T1.store_nbr = T5.store_nbr and
  T1.item_nbr = T5.item_nbr and
  T1.year = T5.year and
  T1.month = T5.month
  )
  where
  T1.dataset = 'train' and
  T1.date = T3.date and
  T1.store_nbr = ", store_nbr," and
  T1.item_nbr = ", item_nbr," and
  T1.year in (2014) and
  T1.units < 25
  ", sep = "")
  
  train.df <- dbGetQuery(con, sql)
  
  for(diff.col in names(train.df)[grepl("_diff", names(train.df))]) {
    train.df[, diff.col] <- as.numeric(train.df[, diff.col])
  }
  
  indice.cols.to.keep <- as.integer(which(! apply(train.df, 2, function(x) { all(is.na(x)) })))
  
  train.df <- train.df[, indice.cols.to.keep]
  train.df$week_day <- factor(train.df$week_day)
  train.df$year <- factor(train.df$year)
  train.df$month <- factor(train.df$month)
  train.df$day <- factor(train.df$day)
  
  ggplot(train.df) + geom_point(aes(x=date, y=units, colour=item_nbr))
  
  gbm.model <- gbm(
    I(log1p(units)) ~ . - date - day - store_nbr - item_nbr,
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

  result <- rbind(
    result,
    data.frame(
      store_nbr=store_nbr,
      item_nbr=item_nbr,
      n.tree=seq(1, gbm.model$n.trees),
      train.error=gbm.model$train.error,
      valid.error=gbm.model$valid.error,
      oobag.improve=cumsum(gbm.model$oobag.improve)
      
      )
    )
}

m <- melt(result, id.vars = c("store_nbr", "item_nbr", "n.tree"))
m$store_nbr <- factor(m$store_nbr)
m$item_nbr <- factor(m$item_nbr)
m$variable <- factor(m$variable)

ggplot(m) +  geom_point(aes(x=n.tree, y=value)) +  facet_grid(item_nbr ~ variable)

stop()
###########################################
############# Predict 2014
###########################################
sql <- paste("
             
             select
             T1.units,
             
             T1.store_nbr,
             T1.item_nbr,
             
             T1.week_day,
             T1.year,
             T1.month,
             T1.day,
             T1.date,
             
             T3.tmax_day,
             T3.tmin_day,
             T3.tavg_day,
             T3.depart_day,
             T3.dewpoint_day,
             T3.wetbulb_day,
             T3.heat_day,
             T3.cool_day,
             T3.sunrise_day,
             T3.sunset_day,
             T3.snowfall_day,
             T3.preciptotal_day,
             T3.stnpressure_day,
             T3.sealevel_day,
             T3.resultspeed_day,
             T3.resultdir_day,
             T3.avgspeed_day,
             
             T3.tmax_day_plus_two,
             T3.tmin_day_plus_two,
             T3.tavg_day_plus_two,
             T3.depart_day_plus_two,
             T3.dewpoint_day_plus_two,
             T3.wetbulb_day_plus_two,
             T3.heat_day_plus_two,
             T3.cool_day_plus_two,
             T3.sunrise_day_plus_two,
             T3.sunset_day_plus_two,
             T3.snowfall_day_plus_two,
             T3.preciptotal_day_plus_two,
             T3.stnpressure_day_plus_two,
             T3.sealevel_day_plus_two,
             T3.resultspeed_day_plus_two,
             T3.resultdir_day_plus_two,
             T3.avgspeed_day_plus_two,
             
             T3.tmax_day_plus_two_diff,
             T3.tmin_day_plus_two_diff,
             T3.tavg_day_plus_two_diff,
             T3.depart_day_plus_two_diff,
             T3.dewpoint_day_plus_two_diff,
             T3.wetbulb_day_plus_two_diff,
             T3.heat_day_plus_two_diff,
             T3.cool_day_plus_two_diff,
             T3.sunrise_day_plus_two_diff,
             T3.sunset_day_plus_two_diff,
             T3.snowfall_day_plus_two_diff,
             T3.preciptotal_day_plus_two_diff,
             T3.stnpressure_day_plus_two_diff,
             T3.sealevel_day_plus_two_diff,
             T3.resultspeed_day_plus_two_diff,
             T3.resultdir_day_plus_two_diff,
             T3.avgspeed_day_plus_two_diff,
             
             T3.tmax_day_plus_seven,
             T3.tmin_day_plus_seven,
             T3.tavg_day_plus_seven,
             T3.depart_day_plus_seven,
             T3.dewpoint_day_plus_seven,
             T3.wetbulb_day_plus_seven,
             T3.heat_day_plus_seven,
             T3.cool_day_plus_seven,
             T3.sunrise_day_plus_seven,
             T3.sunset_day_plus_seven,
             T3.snowfall_day_plus_seven,
             T3.preciptotal_day_plus_seven,
             T3.stnpressure_day_plus_seven,
             T3.sealevel_day_plus_seven,
             T3.resultspeed_day_plus_seven,
             T3.resultdir_day_plus_seven,
             T3.avgspeed_day_plus_seven,
             
             T3.tmax_day_plus_seven_diff,
             T3.tmin_day_plus_seven_diff,
             T3.tavg_day_plus_seven_diff,
             T3.depart_day_plus_seven_diff,
             T3.dewpoint_day_plus_seven_diff,
             T3.wetbulb_day_plus_seven_diff,
             T3.heat_day_plus_seven_diff,
             T3.cool_day_plus_seven_diff,
             T3.sunrise_day_plus_seven_diff,
             T3.sunset_day_plus_seven_diff,
             T3.snowfall_day_plus_seven_diff,
             T3.preciptotal_day_plus_seven_diff,
             T3.stnpressure_day_plus_seven_diff,
             T3.sealevel_day_plus_seven_diff,
             T3.resultspeed_day_plus_seven_diff,
             T3.resultdir_day_plus_seven_diff,
             T3.avgspeed_day_plus_seven_diff
             
             from 
             sales_all_train T1 inner join
             key T2 on (T1.store_nbr = T2.store_nbr) inner join
             weather_agg T3 on (T2.station_nbr = T3.station_nbr)
             where
             T1.dataset = 'train' and
             T1.date = T3.date and
             T1.store_nbr = ", store_nbr," and
             T1.item_nbr = 5 and
             T1.units < 1000 and
             T1.year = 2014
             ", sep = "")

train.df <- dbGetQuery(con, sql)

ggplot(train.df) + geom_point(aes(x=date,y=units, colour=item_nbr))

for(diff.col in names(train.df)[grepl("_diff", names(train.df))]) {
  train.df[, diff.col] <- as.numeric(train.df[, diff.col])
}

indice.cols.to.keep <- as.integer(which(! apply(train.df, 2, function(x) { all(is.na(x)) })))

train.df <- train.df[, indice.cols.to.keep]
train.df$week_day <- factor(train.df$week_day)
train.df$year <- factor(train.df$year)
train.df$month <- factor(train.df$month)
train.df$day <- factor(train.df$day)

prediction <- predict(gbm.model, newdata=train.df)

rmsle(train.df$units, expm1(prediction))

dbDisconnect(con)
