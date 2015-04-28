
library(lubridate)
library(plyr)
library(reshape2)

key.df <- read.csv(file.path("DATA","key.csv"))
key.df$store_nbr <- factor(key.df$store_nbr)
key.df$station_nbr <- factor(key.df$station_nbr)

weather.df <- read.csv(file.path("DATA", "weather.csv"), na.strings = "M")
weather.df$station_nbr <- factor(weather.df$station_nbr)
weather.df$date <- ymd(weather.df$date)
weather.df$sunrise <- ifelse(weather.df$sunrise == "-", NA, weather.df$sunrise)
weather.df$sunset <- ifelse(weather.df$sunset == "-", NA, weather.df$sunset)

tokenize(as.character(weather.df$codesum))

# Code Sum
f <- function(x) {
  return(data.frame(x=strsplit(as.character(x$codesum), " ")[[1]]))
}

agg <- ddply(weather.df,
             .(station_nbr, date),
             f
             )

m <- melt(agg, id.vars = c("station_nbr", "date"))
m$value <- ifelse(m$value == "", "NOTHING", m$value)
m$value <- paste("Weather.Code", m$value, sep = ".")
m$bin <- 1

d <- dcast(m, station_nbr + date ~ value, value.var = "bin", fill = 0)

# Merge
w <- merge(weather.df, d)
w <- w[, -which(names(w) == "codesum")]
w$snowfall <- ifelse(w$snowfall == "T", 0.0, w$snowfall)
w$preciptotal <- ifelse(w$preciptotal == "T", 0.0, w$preciptotal)

weather.df <- w

write.csv(weather.df, file.path("DATA", "alternate_weather.csv"), row.names = FALSE)

# Train & test data
train.df <- read.csv(file.path("DATA", "train.csv"), stringsAsFactor = FALSE)
test.df <- read.csv(file.path("DATA", "test.csv"), stringsAsFactor = FALSE)

train.df$date <- ymd(train.df$date)
test.df$date <- ymd(test.df$date)

train.df$store_nbr <- factor(train.df$store_nbr)
train.df$item_nbr <- factor(train.df$item_nbr)

test.df$store_nbr <- factor(test.df$store_nbr)
test.df$item_nbr <- factor(test.df$item_nbr)

train.df <- merge(train.df, key.df)
train.df <- merge(train.df, weather.df)
write.csv(train.df, file.path("DATA", "alternate_train.csv"), row.names = FALSE)

test.df <- merge(test.df, key.df)
test.df <- merge(test.df, weather.df)
write.csv(test.df, file.path("DATA", "alternate_test.csv"), row.names = FALSE)

train.df$type <- "train"
test.df$type <- "test"

test.df$units <- NA

all.df <- rbind(train.df, test.df)

library(ggplot2)
agg <- ddply(all.df,
             .(store_nbr,date, type),
             summarise,
             tavg=max(tavg)
             )

ggplot(agg) + geom_point(aes(x=date, y=tavg, colour=type)) + facet_wrap(~ store_nbr)
