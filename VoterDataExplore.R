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
library(data.table)

## Setting the Working Environment ---------------------------------------------

getwd()
setwd("D:/Mike_Data/PolData/")

# Making Stata data files into RDS files ---------------------------------------

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

# Remove duplicate ncid's from 2005 - 2021 -------------------------------------

# Set the path to your files
base_path <- "D:/Mike_Data/PolData"

# Loop over the years
for (year in 2005:2021) {
  year_str <- as.character(year)

  # Define the file paths
  rds_file_old <- file.path(base_path, "rda_files", paste0("nc_voters", year_str, ".RDS"))
  rda_file_new <- file.path(base_path, "rda_files", paste0("nc_unique_", year_str, ".RDS"))
  csv_new <- file.path(base_path, "csv_files", paste0("nc_voters", year_str, ".csv"))
  csv_unique_new <- file.path(base_path, "csv_files", paste0("nc_unique_", year_str, ".csv"))
  dta_new <-  file.path(base_path, "dta_files", paste0("nc_unique_", year_str, ".dta"))

  # Read RDS file with ncid duplicates
  nc_xxxx <- readRDS(rds_file_old)

  # Removing duplicate ncid's
  nc_unique <- nc_xxxx %>% group_by(ncid) %>% filter(n()==1)
  nc_dup <- nc_xxxx %>% group_by(ncid) %>% filter(n()>1)
  nc_mostrecent <- nc_dup %>%
    group_by(ncid) %>%
    filter(registr_dt == max(registr_dt))
  nc_mostrecent2 <- nc_mostrecent %>%
    group_by(ncid) %>%
    filter(voter_reg_num == max(voter_reg_num))
  nc_xxxx_unique <- rbind(nc_unique, nc_mostrecent2)

  # Check that it has same number of unique ncid's
    # length(unique(nc_xxxx$ncid))
    # length(unique(nc_xxxx_unique$ncid))

  # Save data as RDS
  saveRDS(nc_xxxx_unique, file = rds_file_new)

  # Save data as CSV
  write.csv(nc_xxxx, csv_new)
  write.csv(nc_xxxx_unique, csv_unique_new)

  # Save data as DTA
  write_dta(nc_xxxx_unique, dta_new)
}

# Merging in missing ncid's for 2005 and 2007 ----------------------------------

  ## Decided to do in stata

# Matching Property Data to Nearest Point for 2006 - 2021 ----------------------

flood_data <- read_parquet(
  here("D:/Mike_Data/PolData/Test1/FS_NC_Flood")
)

# Set the path to your files
base_path <- "D:/Mike_Data/PolData"

# Loop over the years
for (year in 2005:2021) {
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

# Loop to add in a year variable for each voters dataset ---------------------

# Set the path to your files
base_path <- "D:/Mike_Data/PolData"

# Loop over the years
for (year in 2005:2021) {
  year_str <- as.character(year)

  # Define the file paths
  rds_file_new <- file.path(base_path, "rda_files", paste0("nc_", year_str, ".RDS"))
  rds_file_old <- file.path(base_path, "rda_files", paste0("nc_unique_", year_str, ".RDS"))

  # Read in RDS data
  nc_data_prelim <- readRDS(rds_file_old)

  # Gen a year variable
  nc_data <- nc_data_prelim %>% mutate(year = year_str)

  # Save data as RDS
  saveRDS(nc_data, file = rds_file_new)
}

# Merge in voters data from each year and FirstStreet data ---------------------

xwalk <- read_dta("D:/Mike_Data/PolData/dta_files/xwalk_all_years_long.dta")
nc_flood <- read_dta("D:/Mike_Data/PolData/FS_NC.dta")

# Set the path to your files
base_path <- "D:/Mike_Data/PolData"

# Loop over the years
for (year in 2005:2021) {
  year_str <- as.character(year)

  # Define the file paths
  rds_file_old <- file.path(base_path, "rda_files", paste0("nc_", year_str, ".RDS"))
  xwalk_new <- file.path(base_path, "rda_files", paste0("nc_xwalk_", year_str, ".RDS"))

  # Read RDS file
  nc_all <- readRDS(rds_file_old)

  # Select and merge columns of interest
  xwalk_xxxx <- xwalk[xwalk$year == year_str,]
  nc_sel <- nc_all %>% select(c('ncid','voter_status_desc','voter_status_reason_desc',
                                  'race_code','race_desc','ethnic_code','ethnic_desc',
                                  'party_cd','party_desc','sex_code','sex','age','birth_place',
                                  'registr_dt','year'))
  finished_prelim <- merge(xwalk_xxxx, nc_sel, by.x=c("ncid","year"), by.y=c("ncid","year"), all.x=TRUE)
  finished_product <- merge(finished_prelim, nc_flood, by.x=c("fsid"), by.y=c("fsid"), all.x=TRUE)

  # Adding in a blank dummy for flood event, flood event name, and inundation
  finished_product2 <- finished_product %>% mutate(flood_event = 0)
  finished_product3 <- finished_product2 %>% mutate(flood_event_desc = "")
  finished_product4 <- finished_product3 %>% mutate(inundation = 0)

  # Saving merged xwalk file
  saveRDS(finished_product4, file = xwalk_new)

}

# Creating flooding event indicators -------------------------------------------

# Florence (2018) - 1004
  nc_2018 <- readRDS("D:/Mike_Data/PolData/rda_files/nc_xwalk_2018.RDS")

  nc_2018$hist1_depth[is.na(nc_2018$hist1_depth)] <- 0
  nc_2018$hist1_depth[nc_2018$hist1_depth == 'NA'] <- 0
  nc_2018$flood_event[nc_2018$hist1_id == 1004 & nc_2018$hist1_depth > 0] <- 1

  nc_2018$hist2_depth[is.na(nc_2018$hist2_depth)] <- 0
  nc_2018$hist2_depth[nc_2018$hist2_depth == 'NA'] <- 0
  nc_2018$flood_event[nc_2018$hist2_id == 1004 & nc_2018$hist2_depth > 0] <- 1

  nc_2018$inundation[nc_2018$flood_event == 1 & nc_2018$hist1_id == 1004] <- nc_2018$hist1_depth[nc_2018$flood_event == 1 & nc_2018$hist1_id == 1004]
  nc_2018$inundation[nc_2018$flood_event == 1 & nc_2018$hist2_id == 1004] <- nc_2018$hist2_depth[nc_2018$flood_event == 1 & nc_2018$hist2_id == 1004]

  nc_2018$flood_event_desc[nc_2018$flood_event == 1] <- "Florence"

  saveRDS(nc_2018, "D:/Mike_Data/PolData/rda_files/nc_xwalk_2018.RDS")

# Irene (2011) - 1009
  nc_xxxx <- readRDS("D:/Mike_Data/PolData/rda_files/nc_xwalk_2011.RDS")

  nc_xxxx$hist1_depth[is.na(nc_xxxx$hist1_depth)] <- 0
  nc_xxxx$hist1_depth[nc_xxxx$hist1_depth == 'NA'] <- 0
  nc_xxxx$flood_event[nc_xxxx$hist1_id == 1009 & nc_xxxx$hist1_depth > 0] <- 1

  nc_xxxx$hist2_depth[is.na(nc_xxxx$hist2_depth)] <- 0
  nc_xxxx$hist2_depth[nc_xxxx$hist2_depth == 'NA'] <- 0
  nc_xxxx$flood_event[nc_xxxx$hist2_id == 1009 & nc_xxxx$hist2_depth > 0] <- 1

  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1009] <- nc_xxxx$hist1_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1009]
  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1009] <- nc_xxxx$hist2_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1009]

  nc_xxxx$flood_event_desc[nc_xxxx$flood_event == 1] <- "Irene"

  saveRDS(nc_xxxx, "D:/Mike_Data/PolData/rda_files/nc_xwalk_2011.RDS")

# Irma (2017) - 1010
  nc_xxxx <- readRDS("D:/Mike_Data/PolData/rda_files/nc_xwalk_2017.RDS")

  nc_xxxx$hist1_depth[is.na(nc_xxxx$hist1_depth)] <- 0
  nc_xxxx$hist1_depth[nc_xxxx$hist1_depth == 'NA'] <- 0
  nc_xxxx$flood_event[nc_xxxx$hist1_id == 1010 & nc_xxxx$hist1_depth > 0] <- 1

  nc_xxxx$hist2_depth[is.na(nc_xxxx$hist2_depth)] <- 0
  nc_xxxx$hist2_depth[nc_xxxx$hist2_depth == 'NA'] <- 0
  nc_xxxx$flood_event[nc_xxxx$hist2_id == 1010 & nc_xxxx$hist2_depth > 0] <- 1

  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1010] <- nc_xxxx$hist1_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1010]
  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1010] <- nc_xxxx$hist2_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1010]

  nc_xxxx$flood_event_desc[nc_xxxx$flood_event == 1] <- "Irma"

  saveRDS(nc_xxxx, "D:/Mike_Data/PolData/rda_files/nc_xwalk_2017.RDS")

# Nor'easter storm surge (2009) - 1027
  nc_xxxx <- readRDS("D:/Mike_Data/PolData/rda_files/nc_xwalk_2009.RDS")

  nc_xxxx$hist1_depth[is.na(nc_xxxx$hist1_depth)] <- 0
  nc_xxxx$hist1_depth[nc_xxxx$hist1_depth == 'NA'] <- 0
  nc_xxxx$flood_event[nc_xxxx$hist1_id == 1027 & nc_xxxx$hist1_depth > 0] <- 1

  nc_xxxx$hist2_depth[is.na(nc_xxxx$hist2_depth)] <- 0
  nc_xxxx$hist2_depth[nc_xxxx$hist2_depth == 'NA'] <- 0
  nc_xxxx$flood_event[nc_xxxx$hist2_id == 1027 & nc_xxxx$hist2_depth > 0] <- 1

  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1027] <- nc_xxxx$hist1_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1027]
  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1027] <- nc_xxxx$hist2_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1027]

  nc_xxxx$flood_event_desc[nc_xxxx$flood_event == 1] <- "Noreaster"

  saveRDS(nc_xxxx, "D:/Mike_Data/PolData/rda_files/nc_xwalk_2009.RDS")


# Matthew (2016) - 1017 & Rocky Mount river flood (2016) - 50
  nc_xxxx <- readRDS("D:/Mike_Data/PolData/rda_files/nc_xwalk_2016.RDS")

  nc_xxxx$hist1_depth[is.na(nc_xxxx$hist1_depth)] <- 0
  nc_xxxx$hist1_depth[nc_xxxx$hist1_depth == 'NA'] <- 0
  nc_xxxx$flood_event[nc_xxxx$hist1_id == 1017 & nc_xxxx$hist1_depth > 0] <- 1

  nc_xxxx$hist2_depth[is.na(nc_xxxx$hist2_depth)] <- 0
  nc_xxxx$hist2_depth[nc_xxxx$hist2_depth == 'NA'] <- 0
  nc_xxxx$flood_event[nc_xxxx$hist2_id == 1017 & nc_xxxx$hist2_depth > 0] <- 1

  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1017] <- nc_xxxx$hist1_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1017]
  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1017] <- nc_xxxx$hist2_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1017]

  nc_xxxx$flood_event[nc_xxxx$hist1_id == 50 & nc_xxxx$hist1_depth > 0] <- 1

  nc_xxxx$flood_event[nc_xxxx$hist2_id == 50 & nc_xxxx$hist2_depth > 0] <- 1

  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 50] <- nc_xxxx$hist1_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 50]
  nc_xxxx$inundation[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 50] <- nc_xxxx$hist2_depth[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 50]

  nc_xxxx$flood_event_desc[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 1017] <- "Matthew"
  nc_xxxx$flood_event_desc[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 1017] <- "Matthew"
  nc_xxxx$flood_event_desc[nc_xxxx$flood_event == 1 & nc_xxxx$hist1_id == 50] <- "Rocky Mount"
  nc_xxxx$flood_event_desc[nc_xxxx$flood_event == 1 & nc_xxxx$hist2_id == 50] <- "Rocky Mount"

  saveRDS(nc_xxxx, "D:/Mike_Data/PolData/rda_files/nc_xwalk_2016.RDS")

# Dropping unneeded variables --------------------------------------------------

# Set the path to your files
base_path <- "D:/Mike_Data/PolData"

# Loop over the years
for (year in 2005:2021) {
  year_str <- as.character(year)

  # Define the file paths
  rds_file_new <- file.path(base_path, "rda_files", paste0("nc_xwalk_v1_", year_str, ".RDS"))
  rds_file_old <- file.path(base_path, "rda_files", paste0("nc_xwalk_", year_str, ".RDS"))

  # Read in RDS data
  nc_data_prelim <- readRDS(rds_file_old)

  # Dropping unneeded columns
  nc_data = select(nc_data_prelim, -hist1_id, -hist1_event, -hist1_year, -hist1_depth,
                   -hist2_id, -hist2_event, -hist2_year, -hist2_depth, -adapt_id, -adapt_type)

  # Save data as RDS
  saveRDS(nc_data, file = rds_file_new)
}

  nc_test = select(nc_2018, -hist1_id, -hist1_event, -hist1_year, -hist1_depth,
                   -hist2_id, -hist2_event, -hist2_year, -hist2_depth, -adapt_id, -adapt_type)


# Appending datasets into one

nc_2005_base <- readRDS("D:/Mike_Data/PolData/rda_files/nc_xwalk_v1_2005.RDS")

# Set the path to your files
  base_path <- "D:/Mike_Data/PolData"

# Loop over the years
  for (year in 2006:2021) {
    year_str <- as.character(year)

# Define the file paths
    rds_file_old <- file.path(base_path, "rda_files", paste0("nc_xwalk_v1_", year_str, ".RDS"))

# Read in RDS data
    nc_data_prelim <- readRDS(rds_file_old)

# Append to main dataset
    nc_2005_base <- rbind(nc_2005_base,nc_data_prelim)
  }

saveRDS(nc_2005_base, "D:/Mike_Data/PolData/rda_files/nc_xwalk_v1_all.RDS")


# Constructing a variable for if someone moved ---------------------------------

nc_2005_base <- nc_2005_base %>%
  arrange(ncid, year)

saveRDS(nc_2005_base, "D:/Mike_Data/PolData/rda_files/nc_xwalk_v2_all.RDS")

nc_2005_base <- nc_2005_base %>% group_by(ncid) %>% mutate(moved_indicator = ifelse(fsid != lag(fsid), 1, 0))

nc_2005_base$moved_indicator[is.na(nc_2005_base$moved_indicator)] <- 0

nc_2005_base$fsid[is.na(nc_2005_base$fsid)] <- 0
nc_2005_base <- nc_2005_base %>% group_by(ncid) %>% mutate(moved_indicator2 = ifelse(fsid != lag(fsid), 1, 0))
nc_2005_base$moved_indicator2[is.na(nc_2005_base$moved_indicator2)] <- 0

# Taking a random sample for easier use when experimenting with code -----------

nc_base <- readRDS("D:/Mike_Data/PolData/rda_files/nc_xwalk_v2_all.RDS")



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




