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
  
  sql <- paste("
  
  select
  T1.units,

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
  T1.item_nbr = ", item_nbr, "
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
  
  prediction <- predict(gbm.model, newdata=train.df)
  
  save(gbm.model, file = gbm.filename)
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

m <- melt(result, id.vars=c("store_nbr", "item_nbr", "n.tree"))

ggplot(m) + 
  geom_point(aes(x=n.tree, y=value, colour=item_nbr)) + 
  facet_grid(variable ~ store_nbr) +
  theme_bw()

# store 37
ggplot(subset(m, store_nbr == 37)) + 
  geom_point(aes(x=n.tree, y=value, colour=item_nbr)) + 
  facet_grid(variable ~ store_nbr) +
  theme_bw()


dbDisconnect(con)

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
sales_all_test T1 inner join
key T2 on (T1.store_nbr = T2.store_nbr) inner join
weather_agg T3 on (T2.station_nbr = T3.station_nbr)
where
T1.dataset = 'test' and
T1.date = T3.date and
T1.store_nbr = ", store_nbr," and
T1.item_nbr = ", item_nbr, "
", sep = "")
  
  subset.test.df <- dbGetQuery(con, sql)
  
  for(diff.col in names(subset.test.df)[grepl("_diff", names(subset.test.df))]) {
    subset.test.df[, diff.col] <- as.numeric(subset.test.df[, diff.col])
  }
      
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

