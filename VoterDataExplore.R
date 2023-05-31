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


# Explore differences across years ---------------------------------------------

# Similar years 2008, 2015, 2016, 2017, 2019, 2020, 2021
# Dissimilar years 2005, 2006, 2007, 2009, 2010, 2011, 2012, 2013, 2014, 2018

