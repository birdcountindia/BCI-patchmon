### BCI-patchmon repo needs to be at the same level as ebird-datasets 
### for (relative) file paths to work!

library(tidyverse)
library(lubridate)
library(glue)
# library(runner) # for streak (install if not already)
library(writexl)
library(skimmr)


### parameters ###

# paths to latest versions of user & GA info, and sensitive species data
load(url("https://github.com/birdcountindia/ebird-datasets/raw/main/EBD/latest_non-EBD_paths.RData"))
userspath <- glue("../ebird-datasets/{userspath}")
groupaccspath <- glue("../ebird-datasets/{groupaccspath}")
senspath <- glue("../ebird-datasets/{senspath}")


get_param()


pmpstartdate <- as_date("2021-07-01") # 1st July = PMP start

# for calculating streak
currentdays <- 1 + as.numeric(date_real - pmpstartdate)
# for new joinees (last 6 months): day after which under consideration for new joinees
newjoin_date <- date_real - months(6) 


pmpdatapath <- glue("../ebird-datasets/EBD/pmp_rel{currel_month_lab}-{currel_year}.RData")
ldbpath <- glue("ldb_{real_year}-{str_pad(real_month_num, width = 2, pad = 0)}.xlsx")

###   ###


######### preparing data ####

which <- "leaderboard"
source("scripts/pmp_analysis_setup.R")


######### instance-level leaderboard ####

data_l1 <- data2 %>% 
  group_by(OBSERVER.ID, FULL.NAME) %>% 
  summarise(NO.LISTS = n_distinct(SAMPLING.EVENT.IDENTIFIER), 
            NO.P = n_distinct(LOCALITY.ID)) %>% 
  ungroup()


data3 <- data2 %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY) %>% 
  arrange(desc(DAY.PMP)) %>% 
  # to slice with distinct() (some cases of multiple lists in one day and/or week)
  distinct(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY, DAY.PMP, WEEK.PMP) %>% 
  arrange(desc(DAY.PMP), desc(WEEK.PMP)) %>% 
  summarise(DAY.PMP = DAY.PMP,
            WEEK.PMP = WEEK.PMP,
            GAP.D = DAY.PMP - lead(DAY.PMP, default = NA),
            GAP = floor(GAP.D/7)) %>% 
  filter(!is.na(GAP.D), GAP < 3) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY) %>% 
  summarise(FREQ.D = round(mean(GAP.D)),
            FREQ = case_when(FREQ.D >= 7 ~ round(FREQ.D/7),
                             TRUE ~ round(FREQ.D/7, 1))) %>% 
  ungroup()



# looking at patch-level information
data4 <- data_l1 %>% 
  right_join(data2) %>% 
  left_join(data3) %>% 
  distinct(OBSERVER.ID, FULL.NAME, FREQ, FREQ.D, NO.LISTS, NO.P,
           LOCALITY.ID, LOCALITY, OBSERVATION.DATE, DAY.PMP, WEEK.PMP)

data_l2 <- data4 %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID) %>% 
  slice(1) %>% 
  group_by(OBSERVER.ID, FULL.NAME) %>% 
  arrange(LOCALITY.ID) %>% 
  summarise(LOCALITY.ID = LOCALITY.ID, 
            LOCALITY = LOCALITY,
            PATCH.NO = seq(length(LOCALITY.ID))) %>%
  ungroup()

data5 <- data4 %>% 
  left_join(data_l2) %>% 
  group_by(OBSERVER.ID, FULL.NAME, NO.LISTS, NO.P, LOCALITY.ID, PATCH.NO, 
           FREQ, FREQ.D, LOCALITY, DAY.PMP) %>% 
  slice(1) %>% 
  group_by(OBSERVER.ID, FULL.NAME, NO.LISTS, NO.P, LOCALITY.ID, PATCH.NO, 
           FREQ, FREQ.D, LOCALITY) %>% 
  summarise(WEEK.PMP = WEEK.PMP,
            DAY.PMP = DAY.PMP,
            GAP.D = DAY.PMP - lag(DAY.PMP, default = NA),
            GAP = floor(GAP.D/7)) %>% 
  # is one observation part of the same monitoring instance as previous?
  # is there any missing observation between consecutive instances?
  mutate(SAME = case_when(DAY.PMP == DAY.PMP[1] ~ 0, # first observation
                          DAY.PMP != DAY.PMP[1] & FREQ < 1 & GAP.D < (FREQ.D-1) ~ 1,
                          DAY.PMP != DAY.PMP[1] & FREQ < 1 & GAP.D >= (FREQ.D-1) ~ 0,
                          DAY.PMP != DAY.PMP[1] & FREQ >= 1 & GAP < (FREQ-1) ~ 1,
                          DAY.PMP != DAY.PMP[1] & FREQ >= 1 & GAP >= (FREQ-1) ~ 0),
         CONT = case_when(DAY.PMP == DAY.PMP[1] ~ 0, # first observation
                          DAY.PMP != DAY.PMP[1] & FREQ < 1 & GAP.D <= (FREQ.D+1) ~ 1,
                          DAY.PMP != DAY.PMP[1] & FREQ < 1 & GAP.D > (FREQ.D+1) ~ 0,
                          DAY.PMP != DAY.PMP[1] & FREQ >= 1 & GAP < (FREQ+1) ~ 1,
                          DAY.PMP != DAY.PMP[1] & FREQ >= 1 & GAP >= (FREQ+1) ~ 0)) %>% 
  ungroup()


# calculating total monitoring instances based on distinct days of observation.
data_l3 <-  data5 %>% 
  group_by(OBSERVER.ID, FULL.NAME, NO.LISTS, NO.P, LOCALITY.ID, PATCH.NO, FREQ, FREQ.D, LOCALITY) %>%
  summarise(NO.INST = n_distinct(DAY.PMP), # or n()
            NO.INST2 = NO.INST) %>% 
  ungroup()

data_l3a <- data_l3 %>% filter(grepl("errest", LOCALITY)) %>% mutate(P.TYPE = "T.INST")
data_l3b <- data_l3 %>% filter(grepl("etland", LOCALITY)) %>% mutate(P.TYPE = "W.INST")
data_l3c <- full_join(data_l3a, data_l3b)

data_l3 <- data_l3 %>% left_join(data_l3c)


# observer-level leaderboard
ldb1 <- data_l3 %>% 
  pivot_wider(names_from = c(P.TYPE), values_from = NO.INST2, values_fill = 0) %>% 
  select(-"NA") %>% 
  group_by(OBSERVER.ID, FULL.NAME, NO.LISTS, NO.P) %>% 
  summarise(TOT.INST = sum(NO.INST), # total instances over different patches
            T.INST = sum(T.INST),
            W.INST = sum(W.INST)) %>% 
  ungroup() %>% 
  arrange(desc(TOT.INST), FULL.NAME) %>% 
  rownames_to_column("Rank")


######### streaks (based on each observer's frequency) ####

data_l4 <- data5 %>% 
  left_join(data_l3) %>% 
  select(-P.TYPE) %>% 
  # adding state and district
  left_join(patch_loc) %>% 
  group_by(OBSERVER.ID, FULL.NAME, NO.LISTS, NO.P, LOCALITY.ID, LOCALITY, 
           STATE, COUNTY, PATCH.NO, FREQ, FREQ.D, NO.INST) %>% 
  summarise(FI.WEEK.PMP = WEEK.PMP, # final instance week
            FI.DAY.PMP = DAY.PMP, # final instance day
            FI.GAP.D = GAP.D, # final instance gap
            FI.GAP = GAP, # final instance gap
            FI.SAME = SAME, # final instance part of same instance?
            FI.CONT = CONT, # final instance continuing streak or missed instance?
            STREAK = runner::streak_run(CONT),
            H.STREAK = max(STREAK)) %>% 
  arrange(desc(FI.DAY.PMP)) %>% 
  slice(1) %>% 
  mutate(C.STREAK = case_when(
    FREQ < 1 & (currentdays - FI.DAY.PMP) > (FREQ.D+1) ~ 0,
    FREQ >= 1 & (ceiling(currentdays/7) - FI.WEEK.PMP) >= (FREQ+1) ~ 0,
    FREQ < 1 & (currentdays - FI.DAY.PMP) <= (FREQ.D+1) ~ as.numeric(STREAK),
    FREQ >= 1 & (ceiling(currentdays/7) - FI.WEEK.PMP) < (FREQ+1) ~ as.numeric(STREAK)
    ),
    STREAK = NULL) %>% 
  ungroup()


ldb2 <- data_l4 %>% 
  select(-c(LOCALITY.ID, NO.LISTS, NO.P, FI.WEEK.PMP, FI.DAY.PMP, FI.GAP, FI.SAME, FI.CONT))

# patch-level leaderboard by number of instances
ldb2a <- ldb2 %>% 
  arrange(desc(NO.INST), FULL.NAME) %>% 
  rownames_to_column("Rank")

# patch-level leaderboard by current streak
ldb2b <- ldb2 %>% 
  arrange(desc(C.STREAK), FULL.NAME) %>% 
  rownames_to_column("Rank")



######### new joinees ####

ldb3 <- data2 %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID) %>% 
  arrange(DAY.PMP) %>% 
  slice(1) %>% ungroup() %>% 
  filter(OBSERVATION.DATE >= newjoin_date) %>% 
  left_join(data_l2) %>% 
  left_join(data_l4) %>% 
  # adding state and district
  left_join(patch_loc) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY, STATE, COUNTY) %>% 
  summarise(J.MONTH = MONTH,
            J.WEEK.PMP = WEEK.PMP,
            J.DAY.PMP = DAY.PMP,
            FI.WEEK.PMP = FI.WEEK.PMP, 
            FI.DAY.PMP = FI.DAY.PMP) %>% 
  ungroup() %>% 
  arrange(desc(J.DAY.PMP)) %>% 
  mutate(J.MONTH.LAB = J.MONTH %>% month(label = T, abbr = T)) %>% 
  rownames_to_column("Rank")



######### exporting leaderboards ####


write_xlsx(x = list("Monitors" = ldb1, 
                    "Instances" = ldb2a, 
                    "Current streak" = ldb2b, 
                    "New joinees" = ldb3),
           path = ldbpath)


