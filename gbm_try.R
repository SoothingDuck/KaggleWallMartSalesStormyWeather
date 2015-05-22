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

for(i in 1:nrow(df.u)) {
  store_nbr <- df.u[i, "store_nbr"]
  item_nbr <- df.u[i, "item_nbr"]
  
  cat("Generation model store", store_nbr, "item", item_nbr, "...\n")
  
  gbm.filename <- file.path("DATA", paste("gbm_store_nbr_", store_nbr, "_item_nbr_", item_nbr, ".RData", sep = ""))
  
  if(! file.exists(gbm.filename)) {

  sql <- paste("
  
  select
  T1.units,

  T1.week_day,
  T1.year,
  T1.month,
  T1.day,
  T1.date,

  T6.tmax - T3.tmax_min_previous_seven_days as tmax_diff_min_previous_seven_days,
  T6.tmax - T3.tmax_max_previous_seven_days as tmax_diff_max_previous_seven_days,
  T6.tmax - T3.tmax_avg_previous_seven_days as tmax_diff_avg_previous_seven_days,

  T3.tmax_min_previous_seven_days,
  T3.tmax_max_previous_seven_days,
  T3.tmax_avg_previous_seven_days,
  
  T6.tmin - T3.tmin_min_previous_seven_days as tmin_diff_min_previous_seven_days,
  T6.tmin - T3.tmin_max_previous_seven_days as tmin_diff_max_previous_seven_days,
  T6.tmin - T3.tmin_avg_previous_seven_days as tmin_diff_avg_previous_seven_days,

  T3.tmin_min_previous_seven_days,
  T3.tmin_max_previous_seven_days,
  T3.tmin_avg_previous_seven_days,
  
  T6.tavg - T3.tavg_min_previous_seven_days as tavg_diff_min_previous_seven_days,
  T6.tavg - T3.tavg_max_previous_seven_days as tavg_diff_max_previous_seven_days,
  T6.tavg - T3.tavg_avg_previous_seven_days as tavg_diff_avg_previous_seven_days,

  T3.tavg_min_previous_seven_days,
  T3.tavg_max_previous_seven_days,
  T3.tavg_avg_previous_seven_days,
  
  T6.depart - T3.depart_min_previous_seven_days as depart_diff_min_previous_seven_days,
  T6.depart - T3.depart_max_previous_seven_days as depart_diff_max_previous_seven_days,
  T6.depart - T3.depart_avg_previous_seven_days as depart_diff_avg_previous_seven_days,

  T3.depart_min_previous_seven_days,
  T3.depart_max_previous_seven_days,
  T3.depart_avg_previous_seven_days,

  T6.dewpoint - T3.dewpoint_min_previous_seven_days as dewpoint_diff_min_previous_seven_days,
  T6.dewpoint - T3.dewpoint_max_previous_seven_days as dewpoint_diff_max_previous_seven_days,
  T6.dewpoint - T3.dewpoint_avg_previous_seven_days as dewpoint_diff_avg_previous_seven_days,
  
  T3.dewpoint_min_previous_seven_days,
  T3.dewpoint_max_previous_seven_days,
  T3.dewpoint_avg_previous_seven_days,
  
  T6.wetbulb - T3.wetbulb_min_previous_seven_days as wetbulb_diff_min_previous_seven_days,
  T6.wetbulb - T3.wetbulb_max_previous_seven_days as wetbulb_diff_max_previous_seven_days,
  T6.wetbulb - T3.wetbulb_avg_previous_seven_days as wetbulb_diff_avg_previous_seven_days,
  
  T3.wetbulb_min_previous_seven_days,
  T3.wetbulb_max_previous_seven_days,
  T3.wetbulb_avg_previous_seven_days,

  T6.heat - T3.heat_min_previous_seven_days as heat_diff_min_previous_seven_days,
  T6.heat - T3.heat_max_previous_seven_days as heat_diff_max_previous_seven_days,
  T6.heat - T3.heat_avg_previous_seven_days as heat_diff_avg_previous_seven_days,
  
  T3.heat_min_previous_seven_days,
  T3.heat_max_previous_seven_days,
  T3.heat_avg_previous_seven_days,
  
  T6.cool - T3.cool_min_previous_seven_days as cool_diff_min_previous_seven_days,
  T6.cool - T3.cool_max_previous_seven_days as cool_diff_max_previous_seven_days,
  T6.cool - T3.cool_avg_previous_seven_days as cool_diff_avg_previous_seven_days,

  T3.cool_min_previous_seven_days,
  T3.cool_max_previous_seven_days,
  T3.cool_avg_previous_seven_days,
  
  T6.sunrise - T3.sunrise_min_previous_seven_days as sunrise_diff_min_previous_seven_days,
  T6.sunrise - T3.sunrise_max_previous_seven_days as sunrise_diff_max_previous_seven_days,
  T6.sunrise - T3.sunrise_avg_previous_seven_days as sunrise_diff_avg_previous_seven_days,

  T3.sunrise_min_previous_seven_days,
  T3.sunrise_max_previous_seven_days,
  T3.sunrise_avg_previous_seven_days,
  
  T6.sunset - T3.sunset_min_previous_seven_days as sunset_diff_min_previous_seven_days,
  T6.sunset - T3.sunset_max_previous_seven_days as sunset_diff_max_previous_seven_days,
  T6.sunset - T3.sunset_avg_previous_seven_days as sunset_diff_avg_previous_seven_days,

  T3.sunset_min_previous_seven_days,
  T3.sunset_max_previous_seven_days,
  T3.sunset_avg_previous_seven_days,
  
  T6.snowfall - T3.snowfall_min_previous_seven_days as snowfall_diff_min_previous_seven_days,
  T6.snowfall - T3.snowfall_max_previous_seven_days as snowfall_diff_max_previous_seven_days,
  T6.snowfall - T3.snowfall_avg_previous_seven_days as snowfall_diff_avg_previous_seven_days,

  T3.snowfall_min_previous_seven_days,
  T3.snowfall_max_previous_seven_days,
  T3.snowfall_avg_previous_seven_days,
  
  T6.preciptotal - T3.preciptotal_min_previous_seven_days as preciptotal_diff_min_previous_seven_days,
  T6.preciptotal - T3.preciptotal_max_previous_seven_days as preciptotal_diff_max_previous_seven_days,
  T6.preciptotal - T3.preciptotal_avg_previous_seven_days as preciptotal_diff_avg_previous_seven_days,

  T3.preciptotal_min_previous_seven_days,
  T3.preciptotal_max_previous_seven_days,
  T3.preciptotal_avg_previous_seven_days,
  
  T6.stnpressure - T3.stnpressure_min_previous_seven_days as stnpressure_diff_min_previous_seven_days,
  T6.stnpressure - T3.stnpressure_max_previous_seven_days as stnpressure_diff_max_previous_seven_days,
  T6.stnpressure - T3.stnpressure_avg_previous_seven_days as stnpressure_diff_avg_previous_seven_days,

  T3.stnpressure_min_previous_seven_days,
  T3.stnpressure_max_previous_seven_days,
  T3.stnpressure_avg_previous_seven_days,
  
  T6.sealevel - T3.sealevel_min_previous_seven_days as sealevel_diff_min_previous_seven_days,
  T6.sealevel - T3.sealevel_max_previous_seven_days as sealevel_diff_max_previous_seven_days,
  T6.sealevel - T3.sealevel_avg_previous_seven_days as sealevel_diff_avg_previous_seven_days,

  T3.sealevel_min_previous_seven_days,
  T3.sealevel_max_previous_seven_days,
  T3.sealevel_avg_previous_seven_days,
  
  T6.resultspeed - T3.resultspeed_min_previous_seven_days as resultspeed_diff_min_previous_seven_days,
  T6.resultspeed - T3.resultspeed_max_previous_seven_days as resultspeed_diff_max_previous_seven_days,
  T6.resultspeed - T3.resultspeed_avg_previous_seven_days as resultspeed_diff_avg_previous_seven_days,

  T3.resultspeed_min_previous_seven_days,
  T3.resultspeed_max_previous_seven_days,
  T3.resultspeed_avg_previous_seven_days,
  
  T6.resultdir - T3.resultdir_min_previous_seven_days as resultdir_diff_min_previous_seven_days,
  T6.resultdir - T3.resultdir_max_previous_seven_days as resultdir_diff_max_previous_seven_days,
  T6.resultdir - T3.resultdir_avg_previous_seven_days as resultdir_diff_avg_previous_seven_days,

  T3.resultdir_min_previous_seven_days,
  T3.resultdir_max_previous_seven_days,
  T3.resultdir_avg_previous_seven_days,
  
  T6.avgspeed - T3.avgspeed_min_previous_seven_days as avgspeed_diff_min_previous_seven_days,
  T6.avgspeed - T3.avgspeed_max_previous_seven_days as avgspeed_diff_max_previous_seven_days,
  T6.avgspeed - T3.avgspeed_avg_previous_seven_days as avgspeed_diff_avg_previous_seven_days,

  T3.avgspeed_min_previous_seven_days,
  T3.avgspeed_max_previous_seven_days,
  T3.avgspeed_avg_previous_seven_days,
               
  T6.tmax - T3.tmax_min_next_seven_days as tmax_diff_min_next_seven_days,
  T6.tmax - T3.tmax_max_next_seven_days as tmax_diff_max_next_seven_days,
  T6.tmax - T3.tmax_avg_next_seven_days as tmax_diff_avg_next_seven_days,

  T3.tmax_min_next_seven_days,
  T3.tmax_max_next_seven_days,
  T3.tmax_avg_next_seven_days,
               
  T6.tmin - T3.tmin_min_next_seven_days as tmin_diff_min_next_seven_days,
  T6.tmin - T3.tmin_max_next_seven_days as tmin_diff_max_next_seven_days,
  T6.tmin - T3.tmin_avg_next_seven_days as tmin_diff_avg_next_seven_days,

  T3.tmin_min_next_seven_days,
  T3.tmin_max_next_seven_days,
  T3.tmin_avg_next_seven_days,

  T6.tavg - T3.tavg_min_next_seven_days as tavg_diff_min_next_seven_days,
  T6.tavg - T3.tavg_max_next_seven_days as tavg_diff_max_next_seven_days,
  T6.tavg - T3.tavg_avg_next_seven_days as tavg_diff_avg_next_seven_days,

  T3.tavg_min_next_seven_days,
  T3.tavg_max_next_seven_days,
  T3.tavg_avg_next_seven_days,

  T6.depart - T3.depart_min_next_seven_days as depart_diff_min_next_seven_days,
  T6.depart - T3.depart_max_next_seven_days as depart_diff_max_next_seven_days,
  T6.depart - T3.depart_avg_next_seven_days as depart_diff_avg_next_seven_days,

  T3.depart_min_next_seven_days,
  T3.depart_max_next_seven_days,
  T3.depart_avg_next_seven_days,

  T6.dewpoint - T3.dewpoint_min_next_seven_days as dewpoint_diff_min_next_seven_days,
  T6.dewpoint - T3.dewpoint_max_next_seven_days as dewpoint_diff_max_next_seven_days,
  T6.dewpoint - T3.dewpoint_avg_next_seven_days as dewpoint_diff_avg_next_seven_days,

  T3.dewpoint_min_next_seven_days,
  T3.dewpoint_max_next_seven_days,
  T3.dewpoint_avg_next_seven_days,

  T6.wetbulb - T3.wetbulb_min_next_seven_days as wetbulb_diff_min_next_seven_days,
  T6.wetbulb - T3.wetbulb_max_next_seven_days as wetbulb_diff_max_next_seven_days,
  T6.wetbulb - T3.wetbulb_avg_next_seven_days as wetbulb_diff_avg_next_seven_days,

  T3.wetbulb_min_next_seven_days,
  T3.wetbulb_max_next_seven_days,
  T3.wetbulb_avg_next_seven_days,

  T6.heat - T3.heat_min_next_seven_days as heat_diff_min_next_seven_days,
  T6.heat - T3.heat_max_next_seven_days as heat_diff_max_next_seven_days,
  T6.heat - T3.heat_avg_next_seven_days as heat_diff_avg_next_seven_days,

  T3.heat_min_next_seven_days,
  T3.heat_max_next_seven_days,
  T3.heat_avg_next_seven_days,

  T6.cool - T3.cool_min_next_seven_days as cool_diff_min_next_seven_days,
  T6.cool - T3.cool_max_next_seven_days as cool_diff_max_next_seven_days,
  T6.cool - T3.cool_avg_next_seven_days as cool_diff_avg_next_seven_days,

  T3.cool_min_next_seven_days,
  T3.cool_max_next_seven_days,
  T3.cool_avg_next_seven_days,

  T6.sunrise - T3.sunrise_min_next_seven_days as sunrise_diff_min_next_seven_days,
  T6.sunrise - T3.sunrise_max_next_seven_days as sunrise_diff_max_next_seven_days,
  T6.sunrise - T3.sunrise_avg_next_seven_days as sunrise_diff_avg_next_seven_days,

  T3.sunrise_min_next_seven_days,
  T3.sunrise_max_next_seven_days,
  T3.sunrise_avg_next_seven_days,

  T6.sunset - T3.sunset_min_next_seven_days as sunset_diff_min_next_seven_days,
  T6.sunset - T3.sunset_max_next_seven_days as sunset_diff_max_next_seven_days,
  T6.sunset - T3.sunset_avg_next_seven_days as sunset_diff_avg_next_seven_days,

  T3.sunset_min_next_seven_days,
  T3.sunset_max_next_seven_days,
  T3.sunset_avg_next_seven_days,

  T6.snowfall - T3.snowfall_min_next_seven_days as snowfall_diff_min_next_seven_days,
  T6.snowfall - T3.snowfall_max_next_seven_days as snowfall_diff_max_next_seven_days,
  T6.snowfall - T3.snowfall_avg_next_seven_days as snowfall_diff_avg_next_seven_days,

  T3.snowfall_min_next_seven_days,
  T3.snowfall_max_next_seven_days,
  T3.snowfall_avg_next_seven_days,

  T6.preciptotal - T3.preciptotal_min_next_seven_days as preciptotal_diff_min_next_seven_days,
  T6.preciptotal - T3.preciptotal_max_next_seven_days as preciptotal_diff_max_next_seven_days,
  T6.preciptotal - T3.preciptotal_avg_next_seven_days as preciptotal_diff_avg_next_seven_days,

  T3.preciptotal_min_next_seven_days,
  T3.preciptotal_max_next_seven_days,
  T3.preciptotal_avg_next_seven_days,

  T6.stnpressure - T3.stnpressure_min_next_seven_days as stnpressure_diff_min_next_seven_days,
  T6.stnpressure - T3.stnpressure_max_next_seven_days as stnpressure_diff_max_next_seven_days,
  T6.stnpressure - T3.stnpressure_avg_next_seven_days as stnpressure_diff_avg_next_seven_days,

  T3.stnpressure_min_next_seven_days,
  T3.stnpressure_max_next_seven_days,
  T3.stnpressure_avg_next_seven_days,

  T6.sealevel - T3.sealevel_min_next_seven_days as sealevel_diff_min_next_seven_days,
  T6.sealevel - T3.sealevel_max_next_seven_days as sealevel_diff_max_next_seven_days,
  T6.sealevel - T3.sealevel_avg_next_seven_days as sealevel_diff_avg_next_seven_days,

  T3.sealevel_min_next_seven_days,
  T3.sealevel_max_next_seven_days,
  T3.sealevel_avg_next_seven_days,

  T6.resultspeed - T3.resultspeed_min_next_seven_days as resultspeed_diff_min_next_seven_days,
  T6.resultspeed - T3.resultspeed_max_next_seven_days as resultspeed_diff_max_next_seven_days,
  T6.resultspeed - T3.resultspeed_avg_next_seven_days as resultspeed_diff_avg_next_seven_days,

  T3.resultspeed_min_next_seven_days,
  T3.resultspeed_max_next_seven_days,
  T3.resultspeed_avg_next_seven_days,

  T6.resultdir - T3.resultdir_min_next_seven_days as resultdir_diff_min_next_seven_days,
  T6.resultdir - T3.resultdir_max_next_seven_days as resultdir_diff_max_next_seven_days,
  T6.resultdir - T3.resultdir_avg_next_seven_days as resultdir_diff_avg_next_seven_days,

  T3.resultdir_min_next_seven_days,
  T3.resultdir_max_next_seven_days,
  T3.resultdir_avg_next_seven_days,

  T6.avgspeed - T3.avgspeed_min_next_seven_days as avgspeed_diff_min_next_seven_days,
  T6.avgspeed - T3.avgspeed_max_next_seven_days as avgspeed_diff_max_next_seven_days,
  T6.avgspeed - T3.avgspeed_avg_next_seven_days as avgspeed_diff_avg_next_seven_days,

  T3.avgspeed_min_next_seven_days,
  T3.avgspeed_max_next_seven_days,
  T3.avgspeed_avg_next_seven_days,

  T4.avg_units_year,

  T5.avg_units_month,

  T4.avg_units_year - T5.avg_units_month as avg_units_year_month_diff

  from 
  sales T1 inner join
  key T2 on (T1.store_nbr = T2.store_nbr) inner join
  weather_next_seven_days T3 on (T2.station_nbr = T3.station_nbr) inner join
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
  ) inner join 
  weather T6 on (
    T2.station_nbr = T6.station_nbr and
    T1.date = T6.date
  )
  where
  T1.dataset = 'train' and
  T1.date = T3.date and
  T1.store_nbr = ", store_nbr," and
  T1.item_nbr = ", item_nbr, " and
  T1.units < 1000 and
  T1.year in (2013, 2014)
  ", sep = "")
  
  train.df <- dbGetQuery(con, sql)

  for(col in names(train.df)) {
    train.df[, col] <- as.numeric(train.df[, col])
  }
  
  indice.cols.to.keep <- as.integer(which(! apply(train.df, 2, function(x) { all(is.na(x)) })))
  
  train.df <- train.df[, indice.cols.to.keep]
  train.df$week_day <- factor(train.df$week_day)
  train.df$year <- factor(train.df$year)
  train.df$month <- factor(train.df$month)
  train.df$day <- factor(train.df$day)
  
  if(store_nbr == 31 & item_nbr == 67) {
    
    gbm.model <- gbm(
      I(log1p(units)) ~ . - date - day - year - month,
      data = train.df,
      distribution = "gaussian",
      n.trees = 5000,
      interaction.depth = 5,
      n.minobsinnode = 5,
      shrinkage = 0.001,
      bag.fraction = 0.9,
      train.fraction = 0.95,
      verbose = TRUE
    )
    
  } else {
    
    gbm.model <- gbm(
      I(log1p(units)) ~ . - date - day,
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
    
  }
  
  prediction <- predict(gbm.model, newdata=train.df)
  
  save(gbm.model, file = gbm.filename)

  }
}

dbDisconnect(con)

##########################################################
########## Scoring
##########################################################

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

test.df <- dbGetQuery(con, "
select
*
from
sales_all_test
")

dbDisconnect(con)

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

result <- data.frame()
df.u <- unique(test.df[, c("store_nbr", "item_nbr")])

for(i in 1:nrow(df.u)) {
  store_nbr <- df.u[i, "store_nbr"]
  item_nbr <- df.u[i, "item_nbr"]
  
  cat("Evalutation model store", store_nbr, "item", item_nbr, "...\n")
  
  gbm.filename <- file.path("DATA", paste("gbm_store_nbr_", store_nbr, "_item_nbr_", item_nbr, ".RData", sep = ""))
    
  load(gbm.filename)
    
  result <- rbind(
    result,
    data.frame(
      store_nbr=store_nbr,
      item_nbr=item_nbr,
      n.tree=seq(1, gbm.model$n.trees),
      train.error=gbm.model$train.error,
      valid.error=gbm.model$valid.error      
    )
  )
    
}

write.csv(result, file="gbm_try_scoring.csv")
result <- read.csv("gbm_try_scoring.csv", stringsAsFactors= FALSE)
result$store_nbr <- factor(result$store_nbr)
result$item_nbr <- factor(result$item_nbr)

m <- melt(result, id.vars=c("X", "store_nbr", "item_nbr", "n.tree"))

ggplot(m) + 
  geom_point(aes(x=n.tree, y=value, colour=item_nbr)) + 
  facet_grid(variable ~ store_nbr) +
  theme_bw()

# store 37
ggplot(subset(m, store_nbr == 37)) + 
  geom_point(aes(x=n.tree, y=value, colour=item_nbr)) + 
  facet_grid(variable ~ store_nbr) +
  theme_bw()

# store 31
ggplot(subset(m, store_nbr == 31 & item_nbr == 67)) + 
  geom_point(aes(x=n.tree, y=value, colour=item_nbr)) + 
  facet_grid(variable ~ store_nbr) +
  theme_bw()


dbDisconnect(con)
stop()

##########################################################
########## Evaluation sur jeu de test
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

# Evaluation test
result <- data.frame()
df.u <- unique(test.df[, c("store_nbr", "item_nbr")])

for(i in 1:nrow(df.u)) {
  store_nbr <- df.u[i, "store_nbr"]
  item_nbr <- df.u[i, "item_nbr"]

  cat("Evalutation model store", store_nbr, "item", item_nbr, "...\n")
  
  gbm.filename <- file.path("DATA", paste("gbm_store_nbr_", store_nbr, "_item_nbr_", item_nbr, ".RData", sep = ""))
  
  sql <- paste("
select

T1.week_day,
T1.year,
T1.month,
T1.day,
T1.date,

T3.tmax_min_previous_seven_days,
T3.tmax_max_previous_seven_days,
T3.tmax_avg_previous_seven_days,

T3.tmin_min_previous_seven_days,
T3.tmin_max_previous_seven_days,
T3.tmin_avg_previous_seven_days,

T3.tavg_min_previous_seven_days,
T3.tavg_max_previous_seven_days,
T3.tavg_avg_previous_seven_days,

T3.depart_min_previous_seven_days,
T3.depart_max_previous_seven_days,
T3.depart_avg_previous_seven_days,

T3.dewpoint_min_previous_seven_days,
T3.dewpoint_max_previous_seven_days,
T3.dewpoint_avg_previous_seven_days,

T3.wetbulb_min_previous_seven_days,
T3.wetbulb_max_previous_seven_days,
T3.wetbulb_avg_previous_seven_days,

T3.heat_min_previous_seven_days,
T3.heat_max_previous_seven_days,
T3.heat_avg_previous_seven_days,

T3.cool_min_previous_seven_days,
T3.cool_max_previous_seven_days,
T3.cool_avg_previous_seven_days,

T3.sunrise_min_previous_seven_days,
T3.sunrise_max_previous_seven_days,
T3.sunrise_avg_previous_seven_days,

T3.sunset_min_previous_seven_days,
T3.sunset_max_previous_seven_days,
T3.sunset_avg_previous_seven_days,

T3.snowfall_min_previous_seven_days,
T3.snowfall_max_previous_seven_days,
T3.snowfall_avg_previous_seven_days,

T3.preciptotal_min_previous_seven_days,
T3.preciptotal_max_previous_seven_days,
T3.preciptotal_avg_previous_seven_days,

T3.stnpressure_min_previous_seven_days,
T3.stnpressure_max_previous_seven_days,
T3.stnpressure_avg_previous_seven_days,

T3.sealevel_min_previous_seven_days,
T3.sealevel_max_previous_seven_days,
T3.sealevel_avg_previous_seven_days,

T3.resultspeed_min_previous_seven_days,
T3.resultspeed_max_previous_seven_days,
T3.resultspeed_avg_previous_seven_days,

T3.resultdir_min_previous_seven_days,
T3.resultdir_max_previous_seven_days,
T3.resultdir_avg_previous_seven_days,

T3.avgspeed_min_previous_seven_days,
T3.avgspeed_max_previous_seven_days,
T3.avgspeed_avg_previous_seven_days,

T3.tmax_min_next_seven_days,
T3.tmax_max_next_seven_days,
T3.tmax_avg_next_seven_days,

T3.tmin_min_next_seven_days,
T3.tmin_max_next_seven_days,
T3.tmin_avg_next_seven_days,

T3.tavg_min_next_seven_days,
T3.tavg_max_next_seven_days,
T3.tavg_avg_next_seven_days,

T3.depart_min_next_seven_days,
T3.depart_max_next_seven_days,
T3.depart_avg_next_seven_days,

T3.dewpoint_min_next_seven_days,
T3.dewpoint_max_next_seven_days,
T3.dewpoint_avg_next_seven_days,

T3.wetbulb_min_next_seven_days,
T3.wetbulb_max_next_seven_days,
T3.wetbulb_avg_next_seven_days,

T3.heat_min_next_seven_days,
T3.heat_max_next_seven_days,
T3.heat_avg_next_seven_days,

T3.cool_min_next_seven_days,
T3.cool_max_next_seven_days,
T3.cool_avg_next_seven_days,

T3.sunrise_min_next_seven_days,
T3.sunrise_max_next_seven_days,
T3.sunrise_avg_next_seven_days,

T3.sunset_min_next_seven_days,
T3.sunset_max_next_seven_days,
T3.sunset_avg_next_seven_days,

T3.snowfall_min_next_seven_days,
T3.snowfall_max_next_seven_days,
T3.snowfall_avg_next_seven_days,

T3.preciptotal_min_next_seven_days,
T3.preciptotal_max_next_seven_days,
T3.preciptotal_avg_next_seven_days,

T3.stnpressure_min_next_seven_days,
T3.stnpressure_max_next_seven_days,
T3.stnpressure_avg_next_seven_days,

T3.sealevel_min_next_seven_days,
T3.sealevel_max_next_seven_days,
T3.sealevel_avg_next_seven_days,

T3.resultspeed_min_next_seven_days,
T3.resultspeed_max_next_seven_days,
T3.resultspeed_avg_next_seven_days,

T3.resultdir_min_next_seven_days,
T3.resultdir_max_next_seven_days,
T3.resultdir_avg_next_seven_days,

T3.avgspeed_min_next_seven_days,
T3.avgspeed_max_next_seven_days,
T3.avgspeed_avg_next_seven_days,

T4.total_units_year,
T4.avg_units_year,

T5.total_units_month,
T5.avg_units_month,

T4.total_units_year - T5.total_units_month as total_units_year_month_diff,
T4.avg_units_year - T5.avg_units_month as avg_units_year_month_diff

from 
sales_all_test T1 inner join
key T2 on (T1.store_nbr = T2.store_nbr) inner join
weather_next_seven_days T3 on (T2.station_nbr = T3.station_nbr) inner join
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
T1.dataset = 'test' and
T1.date = T3.date and
T1.store_nbr = ", store_nbr," and
T1.item_nbr = ", item_nbr, "
               ", sep = "")
  
  subset.test.df <- dbGetQuery(con, sql)
  
  for(col in names(subset.test.df)) {
    subset.test.df[, col] <- as.numeric(subset.test.df[, col])
  }
  
  # indice.cols.to.keep <- as.integer(which(! apply(subset.test.df, 2, function(x) { all(is.na(x)) })))
  
  # subset.test.df <- subset.test.df[, indice.cols.to.keep]
  subset.test.df$week_day <- factor(subset.test.df$week_day)
  subset.test.df$year <- factor(subset.test.df$year)
  subset.test.df$month <- factor(subset.test.df$month)
  subset.test.df$day <- factor(subset.test.df$day)
  
  load(gbm.filename)
  
  prediction.units <- expm1(predict(gbm.model, newdata=subset.test.df))
  
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

result$year <- as.numeric(as.character(result$year))
result$month <- as.numeric(as.character(result$month))
result$week <- as.numeric(as.character(result$week))
result$day <- as.numeric(as.character(result$day))

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

write.csv(submission, file=file.path("DATA","gbm_try_submission_expm1.csv"), 
          row.names = FALSE,
          quote = FALSE)

