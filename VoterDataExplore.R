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
setwd("D:/Mike_Data/PolData")



# Explore differences across years ---------------------------------------------

# Similar years 2008, 2015, 2016, 2017, 2019, 2020, 2021
# Dissimilar years 2005, 2006, 2007, 2009, 2010, 2011, 2012, 2013, 2014, 2018

# 2014

nc_2014 <- read_dta('nc_voters2014.dta')
length(unique(nc_2014$ncid))

nc_2014_dup <- subset(nc_2014,duplicated(ncid))

nc_2014_dup2 <- nc_2014 %>% group_by(ncid) %>% filter(n()>1)

nc_2014_mostrecent3 <- nc_2014_dup %>%
  group_by(ncid) %>%
  filter(voter_status_desc != "DENIED")

nc_2014_mostrecent2 <- nc_2014_dup2 %>%
  group_by(ncid)  %>%
  filter(registr_dt==max(registr_dt))

if ()






