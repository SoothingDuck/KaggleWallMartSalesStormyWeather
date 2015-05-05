library(RSQLite)
library(lubridate)
library(plyr)
library(reshape2)

unlink("db.sqlite3")

con <- dbConnect(RSQLite::SQLite(), "db.sqlite3")

key.df <- read.csv(file.path("DATA","key.csv"))
key.df$store_nbr <- factor(key.df$store_nbr)
key.df$station_nbr <- factor(key.df$station_nbr)

dbWriteTable(con, "key", key.df)

weather.df <- read.csv(file.path("DATA", "weather.csv"), na.strings = "M")
weather.df$station_nbr <- factor(weather.df$station_nbr)
weather.df$date <- ymd(weather.df$date)
weather.df$sunrise <- ifelse(weather.df$sunrise == "-", NA, weather.df$sunrise)
weather.df$sunset <- ifelse(weather.df$sunset == "-", NA, weather.df$sunset)

# # Code Sum
# f <- function(x) {
#   return(data.frame(x=strsplit(as.character(x$codesum), " ")[[1]]))
# }
# 
# agg <- ddply(weather.df,
#              .(station_nbr, date),
#              f
# )
# 
# m <- melt(agg, id.vars = c("station_nbr", "date"))
# m$value <- ifelse(m$value == "", "NOTHING", m$value)
# m$value <- paste("Weather.Code", m$value, sep = ".")
# m$bin <- 1
# 
# d <- dcast(m, station_nbr + date ~ value, value.var = "bin", fill = 0)
# 
# # Merge
# w <- merge(weather.df, d)
# w <- w[, -which(names(w) == "codesum")]

weather.df$snowfall <- ifelse(weather.df$snowfall == "T", 0.0, weather.df$snowfall)
weather.df$preciptotal <- ifelse(weather.df$preciptotal == "T", 0.0, weather.df$preciptotal)
weather.df$year <- year(weather.df$date)
weather.df$month <- month(weather.df$date)
weather.df$week <- week(weather.df$date)
weather.df$day <- day(weather.df$date)
weather.df$week_day <- wday(weather.df$date)

dbWriteTable(con, "weather", weather.df)

# Train & test data
train.df <- read.csv(file.path("DATA", "train.csv"), stringsAsFactor = FALSE)
test.df <- read.csv(file.path("DATA", "test.csv"), stringsAsFactor = FALSE)

train.df$dataset <- "train"
test.df$dataset <- "test"

train.df$date <- ymd(train.df$date)
train.df$year <- year(train.df$date)
train.df$month <- month(train.df$date)
train.df$week <- week(train.df$date)
train.df$day <- day(train.df$date)
train.df$week_day <- wday(train.df$date)

test.df$date <- ymd(test.df$date)
test.df$year <- year(test.df$date)
test.df$month <- month(test.df$date)
test.df$week <- week(test.df$date)
test.df$day <- day(test.df$date)
test.df$week_day <- wday(test.df$date)

train.df$store_nbr <- factor(train.df$store_nbr)
train.df$item_nbr <- factor(train.df$item_nbr)

test.df$store_nbr <- factor(test.df$store_nbr)
test.df$item_nbr <- factor(test.df$item_nbr)

test.df$units <- NA

all.df <- rbind(train.df, test.df)

dbWriteTable(con, "sales", all.df)

# Creation des indexes
dbGetQuery(con,"create unique index weather_station_nbr_idx on weather (station_nbr, date)")
dbGetQuery(con,"create unique index sales_store_nbr_idx on sales (store_nbr, item_nbr, date)")
dbGetQuery(con,"create index key_station_nbr_idx on key (station_nbr)")
dbGetQuery(con,"create unique index key_store_nbr_idx on key (store_nbr)")


dbDisconnect(con)
