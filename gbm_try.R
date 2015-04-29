library(lubridate)
library(gbm)

set.seed(42)
train.df <- read.csv(file.path("DATA", "alternate_train.csv"), stringsAsFactor = FALSE)
test.df <- read.csv(file.path("DATA", "alternate_test.csv"), stringsAsFactor = FALSE)

train.df$month <- factor(month(train.df$date))
test.df$month <- factor(month(test.df$date))

train.df$station_nbr <- factor(train.df$station_nbr)
test.df$station_nbr <- factor(test.df$station_nbr)

train.df$store_nbr <- factor(train.df$store_nbr)
test.df$store_nbr <- factor(test.df$store_nbr)

train.df$item_nbr <- factor(train.df$item_nbr)
test.df$item_nbr <- factor(test.df$item_nbr)

train.df$date <- ymd(train.df$date)
test.df$date <- ymd(test.df$date)

m <- data.frame(model.matrix(~ item_nbr - 1, test.df))
test.df <- cbind(test.df, m)
test.df <- test.df[, -which(names(test.df) == "item_nbr")]

m <- data.frame(model.matrix(~ store_nbr - 1, test.df))
test.df <- cbind(test.df, m)
test.df <- test.df[, -which(names(test.df) == "store_nbr")]

m <- data.frame(model.matrix(~ station_nbr - 1, test.df))
test.df <- cbind(test.df, m)
test.df <- test.df[, -which(names(test.df) == "station_nbr")]


# subset
train.df <- train.df[train.df$store_nbr != 35,]

# subset
indices <- sample(1:nrow(train.df), nrow(train.df)*.05)
train.df <- train.df[indices,]

m <- data.frame(model.matrix(~ item_nbr - 1, train.df))
train.df <- cbind(train.df, m)
train.df <- train.df[, -which(names(train.df) == "item_nbr")]

m <- data.frame(model.matrix(~ store_nbr - 1, train.df))
train.df <- cbind(train.df, m)
train.df <- train.df[, -which(names(train.df) == "store_nbr")]
train.df <- train.df[, -which(names(train.df) == "store_nbr35")]

m <- data.frame(model.matrix(~ station_nbr - 1, train.df))
train.df <- cbind(train.df, m)
train.df <- train.df[, -which(names(train.df) == "station_nbr")]

# GBM
gbm.model <- gbm(
  units ~ 
    . - date,
  distribution = "gaussian",
  data = train.df,
  n.trees = 1000,
  interaction.depth = 10,
  shrinkag = 0.01,
  train.fraction = 0.9,
  verbose = TRUE
  )

prediction.train <- predict(gbm.model, newdata=train.df)
prediction.test <- predict(gbm.model, newdata=test.df)