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

for(i in 2005:length(data_files)) {
  assign(paste0("nc_voters", i),
  read_dta(paste0("D:/Mike_Data/PolData/dta_files/",
                  data_files[i])))
  save(get(data_names[i]),
       paste0("D:/Mike_Data/PolData/rda_files/",
              nc_voters[i],
              ".rda"),
       row.names = FALSE)
}

# Explore differences across years ---------------------------------------------

# Similar years 2008, 2015, 2016, 2017, 2019, 2020, 2021
# Dissimilar years 2005, 2006, 2007, 2009, 2010, 2011, 2012, 2013, 2014, 2018

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




