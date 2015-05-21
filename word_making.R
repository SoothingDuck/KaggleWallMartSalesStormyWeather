library(ggplot2)
library(lubridate)
library(plyr)


##########################################
########### train.csv
##########################################
df <- read.csv("DATA/train.csv", stringsAsFactors = FALSE)
df$date <- ymd(df$date)
df$year <- year(df$date)
df$week_day <- wday(df$date)
df$store_nbr <- factor(df$store_nbr)
df$item_nbr  <- factor(df$item_nbr)

agg <- ddply(
  df,
  .(store_nbr, item_nbr),
  summarise,
  total_units=sum(units)
  )

agg$store_nbr <- factor(agg$store_nbr)
agg$item_nbr <- factor(agg$item_nbr)

ggplot(subset(agg, total_units < 1000)) + 
  geom_point(aes(x=item_nbr, y=total_units)) +
  facet_wrap(~ store_nbr) +
  theme_bw() +
  xlab("Item Number") +
  ylab("Total number of units sold") +
  ggtitle("Number of units sold for each item number per store") +
  theme(axis.text.x=element_blank(), axis.ticks.x=element_blank())

#  Tous les stores ne vendent pas tous les objets => présence  de valeurs nulles

ggplot(subset(agg, total_units > 0)) + geom_bar(aes(x=item_nbr)) + coord_flip()
                                                                                       



#  Pour un store par date
ggplot(subset(df, store_nbr == 1 & units > 0)) +
  geom_point(aes(x=date, y=units)) +
  facet_grid(week_day ~ item_nbr)

#  Pour un store par date
ggplot(subset(df, item_nbr == 93 & units > 0)) +
  geom_point(aes(x=date, y=units)) +
  facet_grid(week_day ~ store_nbr) + ylim(0,50)


# Store 31 item 67
tmp <- df[df$store_nbr == 31 & df$units > 0 & df$item_nbr == 67,]
tmp$item_nbr <- factor(tmp$item_nbr)

ggplot(tmp) + geom_point(aes(x=date, y=units)) + facet_grid(year ~ week_day)

##########################################
########### key.csv
##########################################
key <- read.csv("DATA/key.csv", stringsAsFactors = FALSE)

ggplot(key) + 
  geom_bar(aes(x=factor(station_nbr))) +
  theme_bw() +
  ggtitle("Number of stores covered by each weather station") +
  xlab("Weather station") +
  ylab("Number of stores")


ggplot(key) + 
  geom_bar(aes(x=factor(store_nbr))) +
  theme_bw() +
  ggtitle("Number of weather station covering each store") +
  xlab("Store") +
  ylab("Number of weather station")

##########################################
########### weather.csv
##########################################
df <- read.csv("DATA/weather.csv", stringsAsFactors = FALSE, na.strings = c("-", "M"))
df$date <- ymd(df$date)
df$station_nbr <- factor(df$station_nbr)
df$snowfall <- as.numeric(ifelse(as.character(df$snowfall) == "T", 0.01, df$snowfall))
df$preciptotal <- as.numeric(ifelse(as.character(df$preciptotal) == "T", 0.001, df$preciptotal))

df$year <- year(df$date)
df$month <- month(df$date)
df$week <- week(df$date)

ggplot(df) + 
  geom_line(aes(x=date, y=tmin, colour="tmin")) +
  geom_line(aes(x=date, y=tmax, colour="tmax")) +
  geom_line(aes(x=date, y=tavg, colour="tavg")) +
  facet_wrap(~ station_nbr) +  theme_bw()

# Not all temperature available for every date (station 5 and 8)


ggplot(df) + 
  geom_line(aes(x=date, y=sealevel)) +
  facet_wrap(~ station_nbr) +  theme_bw()

# Problem on station 5 and 8



ggplot(df) + 
  geom_line(aes(x=date, y=snowfall)) +
  facet_wrap(~ station_nbr) +  theme_bw()

# Problem on station 5 and 8

ggplot(df) + 
  geom_line(aes(x=date, y=preciptotal)) +
  facet_wrap(~ station_nbr) +  theme_bw()


ggplot(df) + 
  geom_line(aes(x=date, y=depart)) +
  facet_wrap(~ station_nbr) +  theme_bw()


ggplot(df) + 
  geom_line(aes(x=date, y=dewpoint)) +
  facet_wrap(~ station_nbr) +  theme_bw()


ggplot(df) + 
  geom_line(aes(x=date, y=wetbulb)) +
  facet_wrap(~ station_nbr) +  theme_bw()

ggplot(df) + 
  geom_line(aes(x=date, y=heat)) +
  facet_wrap(~ station_nbr) +  theme_bw()


ggplot(df) + 
  geom_line(aes(x=date, y=cool)) +
  facet_wrap(~ station_nbr) +  theme_bw()


ggplot(df) + 
  geom_line(aes(x=date, y=sunrise)) +
  facet_wrap(~ station_nbr) +  theme_bw()


ggplot(df) + 
  geom_line(aes(x=date, y=sunset)) +
  facet_wrap(~ station_nbr) +  theme_bw()



df <- df[order(df$station_nbr, df$date),]


df$sunset_predicted <- ifelse(
    is.na(df$sunset),
    filter(df$sunset, filter=rep(1, 100)/100),
    df$sunset
  )

df$sunset_predicted <- df$sunset
for(station_nbr in as.numeric(levels(df$station_nbr))) {

  if(! all(is.na(df$sunset[df$station_nbr == station_nbr]))) {
    l <- loess(
      sunset ~ month + week,
      data=df[df$station_nbr == station_nbr,])
    
    df$sunset_predicted[df$station_nbr == station_nbr] <-  ifelse(
      is.na(df$sunset[df$station_nbr == station_nbr]),
      predict(l, newdata=df[df$station_nbr == station_nbr,]),
      df$sunset[df$station_nbr == station_nbr]
    )
        
  }
  
}

ggplot(df) + 
  geom_line(aes(x=date, y=sunset, colour="original")) +
  geom_line(aes(x=date, y=sunset_predicted, colour="predicted")) +
  facet_wrap(~ station_nbr) +  theme_bw()

# Missing values


ggplot(df) + 
  geom_line(aes(x=date, y=sunrise))  +
  facet_wrap(~ station_nbr) +  theme_bw()



ggplot(df) + 
  geom_line(aes(x=date, y=stnpressure))  +
  facet_wrap(~ station_nbr) +  theme_bw()
