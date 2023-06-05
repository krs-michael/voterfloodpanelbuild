# Load (Potentially) Necessary Packages ----------------------------------------

library(data.table)
library(tidyverse)
library(sf)
library(s2)
library(here)
library(vroom)
library(arrow)
library(FNN)
library(tictoc)
library(parquetize)
library(haven)

# Load Necessary Packages ------------------------------------------------------

library(tidyverse)
library(haven)
library(data.table)

## Setting the Working Environment ---------------------------------------------

getwd()
setwd("D:/Mike_Data/PolData/")

# Making Stata data files into r dataframes ------------------------------------

setwd("D:/Mike_Data/PolData/dta_files")
data_files <- list.files()
data_files

from <- 2005
for (i in 1:length(data_files)) {
  assign(data_files[i], read_dta(paste0(data_files[i])))
  saveRDS(get(data_files[i]), paste0(data_files[i], ".RDS"))
}

rm(data_files, from, i, nc_voters2005.dta, nc_voters2006.dta, nc_voters2007.dta, nc_voters2008.dta,
   nc_voters2009.dta, nc_voters2010.dta, nc_voters2011.dta, nc_voters2012.dta,nc_voters2013.dta,
   nc_voters2014.dta, nc_voters2015.dta, nc_voters2016.dta, nc_voters2017.dta, nc_voters2018.dta,
   nc_voters2019.dta, nc_voters2020.dta, nc_voters2021.dta)

# Remove duplicate ncid's from 2006, 2008 - 2021 -------------------------------

setwd("D:/Mike_Data/PolData/rda_files")

# 2006
nc_xxxx <- readRDS("nc_voters2006.RDS")
length(unique(nc_xxxx$ncid))
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2006.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2006.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2006.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2006.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2008
nc_xxxx <- readRDS("nc_voters2008.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2008.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2008.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2008.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2008.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2009
nc_xxxx <- readRDS("nc_voters2009.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2009.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2009.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2009.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2009.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2010
nc_xxxx <- readRDS("nc_voters2010.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2010.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2010.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2010.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2010.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2011
nc_xxxx <- readRDS("nc_voters2011.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2011.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2011.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2011.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2011.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2012
nc_xxxx <- readRDS("nc_voters2012.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2012.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2012.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2012.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2012.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2013
nc_xxxx <- readRDS("nc_voters2013.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2013.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2013.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2013.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2013.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2014
nc_xxxx <- readRDS("nc_voters2014.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2014.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2014.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2014.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2014.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)


# 2015
nc_xxxx <- readRDS("nc_voters2015.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2015.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2015.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2015.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2015.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2016
nc_xxxx <- readRDS("nc_voters2016.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2016.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2016.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2016.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2016.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2017
nc_xxxx <- readRDS("nc_voters2017.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2017.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2017.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2017.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2017.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)


# 2018
nc_xxxx <- readRDS("nc_voters2018.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2018.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2018.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2018.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2018.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2019
nc_xxxx <- readRDS("nc_voters2019.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2019.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2019.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2019.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2019.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2020
nc_xxxx <- readRDS("nc_voters2020.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2020.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2020.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2020.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2020.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2021
nc_xxxx <- readRDS("nc_voters2021.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2021.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2021.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2021.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2021.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# Merging in missing ncid's for 2005 and 2007 ----------------------------------

## Decided to do in stata

# Dropping duplicate ncid's for 2005 and 2007 ----------------------------------

nc_2005 <- read_dta("D:/Mike_Data/PolData/dta_files/nc_voters2005_ncid.dta")
saveRDS(nc_2005, "D:/Mike_Data/PolData/rda_files/nc_voters2005_ncid.RDS")

nc_2007 <- read_dta("D:/Mike_Data/PolData/dta_files/nc_voters2007_ncid.dta")
saveRDS(nc_2007, "D:/Mike_Data/PolData/rda_files/nc_voters2007_ncid.RDS")

setwd("D:/Mike_Data/PolData/rda_files")

# 2005
nc_xxxx <- readRDS("nc_voters2005_ncid.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2005.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2005.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2005.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2005.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)

# 2007
nc_xxxx <- readRDS("nc_voters2007_ncid.RDS")
nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))
nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_xxxx$ncid))
length(unique(nc_xxxx_unique$ncid))
saveRDS(nc_xxxx_unique, file = "D:/Mike_Data/PolData/rda_files/nc_unique_2007.RDS")
write.csv(nc_xxxx_unique, 'D:/Mike_Data/PolData/csv_files/nc_unique_2007.csv')
write.csv(nc_xxxx, 'D:/Mike_Data/PolData/csv_files/nc_voters2007.csv')
write_dta(nc_xxxx_unique, 'D:/Mike_Data/PolData/dta_files/nc_unique_2007.dta')
rm(nc_unique, nc_dup, nc_mostrecent, nc_mostrecent2, nc_xxxx, nc_xxxx_unique)


# Matching Property Data to Nearest Point for 2006 - 2021 ----------------------

library(data.table)
library(tidyverse)
library(sf)
library(s2)
library(here)
library(vroom)
library(arrow)
library(FNN)
library(tictoc)
library(parquetize)

# 2005

getwd()
setwd("D:/Mike_Data/PolData/")

options(max.print = 50)

parquetize::table_to_parquet(
  path_to_file = here("D:/Mike_Data/PolData/dta_files/nc_unique_2005.dta"),
  path_to_parquet = here("D:/Mike_Data/PolData/Test1/nc_voters2005/")
)
nc_2005 <- read_parquet(
  here("D:/Mike_Data/PolData/Test1/nc_voters2005")
)
flood_data <- read_parquet(
  here("D:/Mike_Data/PolData/Test1/FS_NC_Flood")
)
tictoc::tic("Calculating nearest index")
nearest_idx = FNN:::get.knnx(
  data = flood_data[, c("lat", "v2")],
  query = nc_2005[, c("y", "x")],
  k = 1, algorithm = "kd_tree"
)
tictoc::toc()
xwalk_2005 <- data.table(
  ncid = nc_2005$ncid,
  fsid2005 = flood_data$fsid[nearest_idx$nn.index]
)
saveRDS(xwalk_2005, file = "D:/Mike_Data/PolData/rda_files/xwalk_2005.RDS")
write.csv(xwalk_2005, 'D:/Mike_Data/PolData/csv_files/xwalk_2005.csv')
write_dta(xwalk_2005, 'D:/Mike_Data/PolData/dta_files/xwalk_2005.dta')

# 2006

parquetize::table_to_parquet(
  path_to_file = here("D:/Mike_Data/PolData/dta_files/nc_unique_2006.dta"),
  path_to_parquet = here("D:/Mike_Data/PolData/Test1/nc_voters2006/")
)
nc_2006 <- read_parquet(
  here("D:/Mike_Data/PolData/Test1/nc_voters2006")
)
tictoc::tic("Calculating nearest index")
nearest_idx = FNN:::get.knnx(
  data = flood_data[, c("lat", "v2")],
  query = nc_2006[, c("y", "x")],
  k = 1, algorithm = "kd_tree"
)
tictoc::toc()
xwalk_2006 <- data.table(
  ncid = nc_2006$ncid,
  fsid2006 = flood_data$fsid[nearest_idx$nn.index]
)

saveRDS(xwalk_2006, file = "D:/Mike_Data/PolData/rda_files/xwalk_2006.RDS")
write.csv(xwalk_2006, 'D:/Mike_Data/PolData/csv_files/xwalk_2006.csv')
write_dta(xwalk_2006, 'D:/Mike_Data/PolData/dta_files/xwalk_2006.dta')

# 2007

parquetize::table_to_parquet(
  path_to_file = here("D:/Mike_Data/PolData/dta_files/nc_unique_2007.dta"),
  path_to_parquet = here("D:/Mike_Data/PolData/Test1/nc_voters2007/")
)
nc_2007 <- read_parquet(
  here("D:/Mike_Data/PolData/Test1/nc_voters2007")
)
tictoc::tic("Calculating nearest index")
nearest_idx = FNN:::get.knnx(
  data = flood_data[, c("lat", "v2")],
  query = nc_2007[, c("y", "x")],
  k = 1, algorithm = "kd_tree"
)
tictoc::toc()
xwalk_2007 <- data.table(
  ncid = nc_2007$ncid,
  fsid2007 = flood_data$fsid[nearest_idx$nn.index]
)

saveRDS(xwalk_2007, file = "D:/Mike_Data/PolData/rda_files/xwalk_2007.RDS")
write.csv(xwalk_2007, 'D:/Mike_Data/PolData/csv_files/xwalk_2007.csv')
write_dta(xwalk_2007, 'D:/Mike_Data/PolData/dta_files/xwalk_2007.dta')


# 2008

parquetize::table_to_parquet(
  path_to_file = here("D:/Mike_Data/PolData/dta_files/nc_unique_2008.dta"),
  path_to_parquet = here("D:/Mike_Data/PolData/Test1/nc_voters2008/")
)
nc_2008 <- read_parquet(
  here("D:/Mike_Data/PolData/Test1/nc_voters2008")
)
tictoc::tic("Calculating nearest index")
nearest_idx = FNN:::get.knnx(
  data = flood_data[, c("lat", "v2")],
  query = nc_2008[, c("y", "x")],
  k = 1, algorithm = "kd_tree"
)
tictoc::toc()
xwalk_2008 <- data.table(
  ncid = nc_2008$ncid,
  fsid2008 = flood_data$fsid[nearest_idx$nn.index]
)

saveRDS(xwalk_2008, file = "D:/Mike_Data/PolData/rda_files/xwalk_2008.RDS")
write.csv(xwalk_2008, 'D:/Mike_Data/PolData/csv_files/xwalk_2008.csv')
write_dta(xwalk_2008, 'D:/Mike_Data/PolData/dta_files/xwalk_2008.dta')

# 2009

parquetize::table_to_parquet(
  path_to_file = here("D:/Mike_Data/PolData/dta_files/nc_unique_2009.dta"),
  path_to_parquet = here("D:/Mike_Data/PolData/Test1/nc_voters2009/")
)
nc_2009 <- read_parquet(
  here("D:/Mike_Data/PolData/Test1/nc_voters2009")
)
tictoc::tic("Calculating nearest index")
nearest_idx = FNN:::get.knnx(
  data = flood_data[, c("lat", "v2")],
  query = nc_2009[, c("y", "x")],
  k = 1, algorithm = "kd_tree"
)
tictoc::toc()
xwalk_2009 <- data.table(
  ncid = nc_2008$ncid,
  fsid2009 = flood_data$fsid[nearest_idx$nn.index]
)

saveRDS(xwalk_2009, file = "D:/Mike_Data/PolData/rda_files/xwalk_2009.RDS")
write.csv(xwalk_2009, 'D:/Mike_Data/PolData/csv_files/xwalk_2009.csv')
write_dta(xwalk_2009, 'D:/Mike_Data/PolData/dta_files/xwalk_2009.dta')



library(parquetize)
library(data.table)
library(tictoc)
library(FNN)

# Set the path to your files
base_path <- "D:/Mike_Data/PolData"

# Loop over the years
for (year in 2005:2009) {
  year_str <- as.character(year)

  # Define the file paths
  dta_file <- file.path(base_path, "dta_files", paste0("nc_unique_", year_str, ".dta"))
  parquet_path <- file.path(base_path, "Test1", paste0("nc_voters", year_str))
  rds_file <- file.path(base_path, "rda_files", paste0("xwalk_", year_str, ".RDS"))
  csv_file <- file.path(base_path, "csv_files", paste0("xwalk_", year_str, ".csv"))
  dta_output <- file.path(base_path, "dta_files", paste0("xwalk_", year_str, ".dta"))

  # Convert table to Parquet
  table_to_parquet(path_to_file = dta_file, path_to_parquet = parquet_path)

  # Read Parquet file
  nc_data <- read_parquet(parquet_path)

  # Calculate nearest index
  tictoc::tic("Calculating nearest index")
  nearest_idx <- FNN:::get.knnx(
    data = flood_data[, c("lat", "v2")],
    query = nc_data[, c("y", "x")],
    k = 1, algorithm = "kd_tree"
  )
  tictoc::toc()

  # Create data.table
  xwalk_data <- data.table(
    ncid = nc_data$ncid,
    fsid = flood_data$fsid[nearest_idx$nn.index]
  )

  # Save data as RDS
  saveRDS(xwalk_data, file = rds_file)

  # Save data as CSV
  write.csv(xwalk_data, csv_file)

  # Save data as DTA
  write_dta(xwalk_data, dta_output)
}
























# Explore differences across years ---------------------------------------------

# Similar years 2008, 2015, 2016, 2017, 2019, 2020, 2021
# Dissimilar years 2005, 2006, 2007, 2009, 2010, 2011, 2012, 2013, 2014, 2018

setwd("D:/Mike_Data/PolData/rda_files")
data_files <- list.files()
data_files

from <- 2005
for (i in 1:length(data_files)) {
  assign(data_files[i], readRDS(paste0(data_files[i])))
  nc_unique <- data_files[i] %>% group_by(ncid) %>% filter(n()==1)
  nc_dup <- data_files[i] %>% group_by(ncid) %>% filter(n()>1)
  nc_mostrecent <- nc_dup %>%
    group_by(ncid) %>%
    filter(registr_dt == max(registr_dt))
  nc_mostrecent2 <- nc_mostrecent %>%
    group_by(ncid) %>%
    filter(voter_reg_num == max(voter_reg_num))
  nc_unique_bind <- rbind(nc_unique, nc_mostrecent2)

}

# 2014

nc_2014 <- read_dta('nc_voters2014.dta')
length(unique(nc_2014$ncid))

nc_unique <- nc_2014 %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_2014 %>% group_by(ncid) %>% filter(n()>1)

nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))

nc_2014_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_2014_unique$ncid))

rm(nc_unique)
rm(nc_dup)
rm(nc_mostrecent)
rm(nc_mostrecent2)

# 2017

nc_2017 <- read_dta('nc_voters2017.dta')
length(unique(nc_2017$ncid))

nc_unique <- nc_2017 %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_2017 %>% group_by(ncid) %>% filter(n()>1)

nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))

nc_2017_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_2017_unique$ncid))

rm(nc_unique)
rm(nc_dup)
rm(nc_mostrecent)
rm(nc_mostrecent2)

# 2009

nc_2009 <- read_dta('nc_voters2009.dta')
length(unique(nc_2009$ncid))

nc_unique <- nc_2009 %>% group_by(ncid) %>% filter(n()==1)
nc_dup <- nc_2009 %>% group_by(ncid) %>% filter(n()>1)

nc_mostrecent <- nc_dup %>%
  group_by(ncid) %>%
  filter(registr_dt == max(registr_dt))
nc_mostrecent2 <- nc_mostrecent %>%
  group_by(ncid) %>%
  filter(voter_reg_num == max(voter_reg_num))

nc_2009_unique <- rbind(nc_unique, nc_mostrecent2)
length(unique(nc_2009_unique$ncid))

rm(nc_unique)
rm(nc_dup)
rm(nc_mostrecent)
rm(nc_mostrecent2)


# Test Code --------------------------------------------------------------------

nc_2014_mostrecent <- nc_2014_dup %>%
  group_by(ncid) %>%
  filter(voter_status_desc == "ACTIVE" & registr_dt==max(registr_dt))
length(unique(nc_2014_mostrecent$ncid))

nc_2014_mostrecent2 <- nc_2014_dup %>%
  group_by(ncid)  %>%
  filter(registr_dt==max(registr_dt))
length(unique(nc_2014_mostrecent2$ncid))

nc_2014_mostrecent2 <- nc_2014_mostrecent %>%
  group_by(ncid)  %>%
  filter(registr_dt==max(registr_dt))
length(unique(nc_2014_mostrecent2$ncid))

rm(nc_2014_mostrecent)
rm(nc_2014_mostrecent2)
rm(nc_2014_conden)
rm(nc_dup)

rm(nc_2009)
rm(nc_2014)
rm(nc_2017)
rm(nc_2009_unique)
rm(nc_2014_unique)
rm(nc_2017_unique)
rm(nc_2014_dup)

nc_2014_conden <- nc_2014_dup %>% select('voter_reg_num', 'ncid', 'voter_status_desc', 'registr_dt')




