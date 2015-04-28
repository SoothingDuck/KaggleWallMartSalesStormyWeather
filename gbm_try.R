library(gbm)

train.df <- read.csv(file.path("DATA", "alternate_train.csv"), stringsAsFactor = FALSE)
test.df <- read.csv(file.path("DATA", "alternate_test.csv"), stringsAsFactor = FALSE)

# subset
train.df <- train.df[train.df$tavg > 50,]
train.df <- train.df[train.df$store_nbr != 35,]

# GBM
gbm.model <- gbm(
  units ~ 
    .  + store_nbr*item_nbr,
  distribution = "poisson",
  data = train.df,
  n.trees = 1000,
  interaction.depth = 1,
  shrinkag = 0.01,
  train.fraction = 0.9,
  verbose = TRUE
  )
