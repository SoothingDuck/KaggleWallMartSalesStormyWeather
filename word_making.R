library(ggplot2)
library(lubridate)
library(plyr)

df <- read.csv("DATA/train.csv", stringsAsFactors = FALSE)
df$date <- ymd(df$date)
df$year <- year(df$date)
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

