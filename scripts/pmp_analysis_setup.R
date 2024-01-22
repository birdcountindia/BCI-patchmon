# joining observer names to dataset

eBird_users <- read.delim(userspath, sep = "\t", header = T, quote = "", 
                          stringsAsFactors = F, na.strings = c(""," ",NA)) %>% 
  transmute(OBSERVER.ID = observer_id,
            FULL.NAME = paste(first_name, last_name, sep = " "))

load(pmpdatapath)
data_pmp <- left_join(data_pmp, eBird_users, "OBSERVER.ID")


########


data0 <- data_pmp %>% 
  ungroup() %>% 
  filter(OBSERVER.ID != "obsr2607928") %>% # PMP account
  group_by(OBSERVER.ID, LOCALITY.ID, SAMPLING.EVENT.IDENTIFIER) %>% 
  slice(1) %>% ungroup() %>% 
  # basic eligible list filter
  filter(ALL.SPECIES.REPORTED == 1, DURATION.MINUTES >= 14) %>% 
  group_by(SAMPLING.EVENT.IDENTIFIER) %>% 
  filter(!any(OBSERVATION.COUNT == "X")) %>% 
  ungroup() 



data_pmp <- data_pmp %>% 
  ungroup() %>% 
  filter(OBSERVER.ID != "obsr2607928") %>% # PMP account
  # removing spuhs, slashes, etc.
  mutate(CATEGORY = if_else(CATEGORY == "domestic" & COMMON.NAME == "Rock Pigeon", 
                            "species",
                            CATEGORY)) %>% 
  filter(CATEGORY %in% c("issf", "species")) %>% 
  # basic eligible list filter
  filter(ALL.SPECIES.REPORTED == 1, DURATION.MINUTES >= 14) %>% 
  group_by(SAMPLING.EVENT.IDENTIFIER) %>% 
  filter(!any(OBSERVATION.COUNT == "X")) %>% 
  mutate(OBSERVATION.COUNT = as.numeric(OBSERVATION.COUNT)) %>% 
  ungroup() 


########

# observer-patch-state-district info
patch_loc <- data0 %>% distinct(OBSERVER.ID, LOCALITY.ID, STATE, COUNTY)

met_week <- function(dates) {
  require(lubridate)
  normal_year <- c((0:363 %/% 7 + 1), 52)
  leap_year   <- c(normal_year[1:59], 9, normal_year[60:365])
  year_day    <- yday(dates)
  return(ifelse(leap_year(dates), leap_year[year_day], normal_year[year_day])) 
}

# calculating DAY and WEEK from start of PMP (WEEK.MY starts 4 weeks before WEEK.PMP)
data1 <- data0 %>%
  mutate(DAY.Y = yday(OBSERVATION.DATE),
         WEEK.Y = met_week(OBSERVATION.DATE),
         M.YEAR = if_else(DAY.Y <= 151, YEAR-1, YEAR), # from 1st June to 31st May
         WEEK.MY = if_else(WEEK.Y > 21, WEEK.Y-21, 52-(21-WEEK.Y))) %>% 
  mutate(DAY.PMP = 1 + as.numeric(as_date(OBSERVATION.DATE) - pmpstartdate),
         WEEK.PMP = ceiling(DAY.PMP/7)) %>% 
  ungroup() 


# excluding non-patch-monitors having lists shared with patch-monitors
temp1 <- data1 %>% 
  group_by(LOCALITY.ID, GROUP.ID) %>% 
  # no. of observers in instance
  summarise(PATCH.OBS = n_distinct(SAMPLING.EVENT.IDENTIFIER)) 

# selecting users with at least one solo PMP checklist to filter out non-monitors that 
# only have shared lists with monitors
temp2 <- data1 %>% 
  left_join(temp1) %>% 
  filter(PATCH.OBS == 1) %>% 
  # to remove same observer's second account
  group_by(FULL.NAME, OBSERVER.ID) %>% 
  # choosing account with most observations (assumed to be primary)
  summarise(N = n()) %>% 
  arrange(desc(N)) %>% slice(1) %>% ungroup() %>% 
  distinct(FULL.NAME, OBSERVER.ID)


data2 <- data1 %>% 
  filter(OBSERVER.ID %in% temp2$OBSERVER.ID) %>% 
  # filter(str_detect(LOCALITY, "PMP")) %>% # PMP in location name is not mandate
  # Lakshmikant/Loukika slash
  mutate(FULL.NAME = case_when(FULL.NAME == "Lakshmikant Neve" ~ 
                                 "Lakshmikant-Loukika Neve",
                               TRUE ~ FULL.NAME))