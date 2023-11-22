library(tidyverse)
library(lubridate)
library(glue)
library(magick)
library(grid)
library(patchwork)
library(ggtext) # to enable markdown in ggplot text (here needed for facet strip text)
library(skimmr)

source("pmp-functions.R")

load("maps.RData")

cbbPalette <- c("#000000", "#E69F00", "#56B4E9", "#009E73", 
                "#F0E442", "#0072B2", "#D55E00", "#CC79A7")


### parameters ###

# paths to latest versions of user & GA info, and sensitive species data
load(url("https://github.com/birdcountindia/ebird-datasets/raw/main/EBD/latest_non-EBD_paths.RData"))

logo1 <- image_convert(image_read("bcilogo.png"), matte = T)
logo2 <- image_convert(image_read("eBird India logo.png"), matte = T)

get_param(date_currel = "2023-10-01")

# date_real <- today() %>% floor_date(unit = "month") # date under consideration for current leaderboard
pmpstartdate <- as_date("2021-07-01") # 1st July = PMP start


data_annotation <- glue("Data from {date_to_string(pmpstartdate)} to {date_to_string(date_real - 1)}")


pmpdatapath <- glue("../ebird-datasets/EBD/pmp_rel{currel_month_lab}-{currel_year}.RData")

### ###

##### setup ####

load(pmpdatapath)

# joining observer names to dataset
eBird_users <- read.delim(userspath, sep = "\t", header = T, quote = "", 
                          stringsAsFactors = F, na.strings = c(""," ",NA)) %>% 
  transmute(OBSERVER.ID = observer_id,
            FULL.NAME = paste(first_name, last_name, sep = " "))
data_pmp <- left_join(data_pmp, eBird_users, "OBSERVER.ID")

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

# season information
data_pmp <- data_pmp %>% 
  mutate(SEASON = case_when(MONTH %in% 3:5 ~ "Spring",
                            MONTH %in% 6:8 ~ "Summer",
                            MONTH %in% 9:11 ~ "Autumn",
                            MONTH %in% c(12, 1, 2) ~ "Winter")) %>% 
  mutate(SEASON = factor(SEASON, 
                         # same as migratory year
                         levels = c("Summer", "Autumn", "Winter", "Spring")))

# excluding non-patch-monitors having lists shared with patch-monitors
temp1 <- data_pmp %>% 
  group_by(LOCALITY.ID, GROUP.ID) %>% 
  # no. of observers in instance
  summarise(PATCH.OBS = n_distinct(SAMPLING.EVENT.IDENTIFIER)) 

# selecting users with at least one solo PMP checklist to filter out non-monitors that 
# only have shared lists with monitors
temp2 <- data_pmp %>% 
  left_join(temp1) %>% 
  filter(PATCH.OBS == 1) %>% 
  # to remove same observer's second account
  group_by(FULL.NAME, OBSERVER.ID) %>% 
  # choosing account with most observations (assumed to be primary)
  summarise(N = n()) %>% 
  arrange(desc(N)) %>% slice(1) %>% ungroup() %>% 
  distinct(FULL.NAME, OBSERVER.ID)

data_pmp <- data_pmp %>% 
  filter(OBSERVER.ID %in% temp2$OBSERVER.ID) %>% 
  # filter(str_detect(LOCALITY, "PMP")) %>% # PMP in location name is not mandate
  # Lakshmikant/Loukika slash
  mutate(FULL.NAME = case_when(FULL.NAME == "Lakshmikant Neve" ~ 
                                 "Lakshmikant-Loukika Neve",
                               TRUE ~ FULL.NAME))


# getting sample size data for other metrics 
samplesizes <- data_pmp %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY, SEASON) %>% 
  summarise(TOT.LISTS = n_distinct(SAMPLING.EVENT.IDENTIFIER))


##### filtering species per patch per observer for analyses ####


# list of species commonly observed in patches to be removed
remove_common <- data_pmp %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, SEASON) %>% 
  mutate(TOT.LISTS = n_distinct(SAMPLING.EVENT.IDENTIFIER)) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, COMMON.NAME, SEASON) %>% 
  summarise(N.LISTS = n_distinct(SAMPLING.EVENT.IDENTIFIER),
            REP.FREQ = (N.LISTS/min(TOT.LISTS)) * 100,
            NINETY = if_else(REP.FREQ > 90, 1, 0)) %>% 
  summarise(TOT.SEASONS = n_distinct(SEASON),
            # if species observed in more than two seasons AND repfreq > 90 in all seasons
            REMOVE = if_else(TOT.SEASONS > 2 & sum(NINETY) == TOT.SEASONS, 1, 0)) %>% 
  ungroup() %>% 
  filter(REMOVE == 1)



# filtering 20 species per observer across all their patches (removing very common spp.)
filt_spec_RC <- data_pmp %>% 
  group_by(OBSERVER.ID, FULL.NAME, COMMON.NAME, LOCALITY.ID, SEASON) %>% 
  summarise(N.MONTHS = n_distinct(MONTH),
            N.OBS = n_distinct(SAMPLING.EVENT.IDENTIFIER)) %>% 
  # every month per season
  filter(N.MONTHS == 3) %>% 
  summarise(N.SEASONS = n_distinct(SEASON),
            TOT.OBS = sum(N.OBS)) %>% 
  # at least 2 seasons
  filter(N.SEASONS >= 2) %>% 
  # # removing species commonly observed in patches (>90% repfreq in all seasons)
  anti_join(remove_common) %>%
  # selecting 20 species with most observations across all patches
  arrange(OBSERVER.ID, COMMON.NAME, LOCALITY.ID, desc(TOT.OBS)) %>% 
  group_by(OBSERVER.ID, FULL.NAME, COMMON.NAME) %>% 
  slice(1) %>% 
  arrange(OBSERVER.ID, desc(TOT.OBS)) %>% 
  group_by(OBSERVER.ID, FULL.NAME) %>% 
  slice(1:20) %>% 
  distinct(OBSERVER.ID, FULL.NAME, COMMON.NAME, TOT.OBS) %>% 
  ungroup()

# species must be present in every month per season, in at least two seasons (removing very common spp.)
filt_specloc_RC <- data_pmp %>%
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, COMMON.NAME, SEASON) %>%
  summarise(N.MONTHS = n_distinct(MONTH),
            N.OBS = n_distinct(SAMPLING.EVENT.IDENTIFIER)) %>%
  # every month per season
  filter(N.MONTHS == 3) %>%
  summarise(N.SEASONS = n_distinct(SEASON),
            TOT.OBS = sum(N.OBS)) %>%
  # at least 2 seasons
  filter(N.SEASONS >= 2) %>%
  # removing species commonly observed in patches (>90% repfreq in all seasons)
  anti_join(remove_common) %>%
  distinct(OBSERVER.ID, FULL.NAME, LOCALITY.ID, COMMON.NAME) %>% 
  ungroup()



# filtering 20 species per observer across all their patches (w/o removing very common spp.)
filt_spec <- data_pmp %>% 
  group_by(OBSERVER.ID, FULL.NAME, COMMON.NAME, LOCALITY.ID, SEASON) %>% 
  summarise(N.MONTHS = n_distinct(MONTH),
            N.OBS = n_distinct(SAMPLING.EVENT.IDENTIFIER)) %>% 
  # every month per season
  filter(N.MONTHS == 3) %>% 
  summarise(N.SEASONS = n_distinct(SEASON),
            TOT.OBS = sum(N.OBS)) %>% 
  # at least 2 seasons
  filter(N.SEASONS >= 2) %>% 
  # selecting 20 species with most observations across all patches
  arrange(OBSERVER.ID, COMMON.NAME, LOCALITY.ID, desc(TOT.OBS)) %>% 
  group_by(OBSERVER.ID, FULL.NAME, COMMON.NAME) %>% 
  slice(1) %>% 
  arrange(OBSERVER.ID, desc(TOT.OBS)) %>% 
  group_by(OBSERVER.ID, FULL.NAME) %>% 
  slice(1:20) %>% 
  distinct(OBSERVER.ID, FULL.NAME, COMMON.NAME, TOT.OBS) %>% 
  ungroup()


# species must be present in every month per season, in at least two seasons 
# (w/o removing very common spp.)
filt_specloc <- data_pmp %>%
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, COMMON.NAME, SEASON) %>%
  summarise(N.MONTHS = n_distinct(MONTH),
            N.OBS = n_distinct(SAMPLING.EVENT.IDENTIFIER)) %>%
  # every month per season
  filter(N.MONTHS == 3) %>%
  summarise(N.SEASONS = n_distinct(SEASON),
            TOT.OBS = sum(N.OBS)) %>%
  # at least 2 seasons
  filter(N.SEASONS >= 2) %>%
  distinct(OBSERVER.ID, FULL.NAME, LOCALITY.ID, COMMON.NAME) %>% 
  ungroup()
  


# location (patch) needs data in every month per season, in at least two seasons
filt_loc <- data_pmp %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, SEASON) %>% 
  summarise(N.MONTHS = n_distinct(MONTH)) %>% 
  # every month per season
  filter(N.MONTHS == 3) %>% 
  summarise(N.SEASONS = n_distinct(SEASON)) %>% 
  # at least 2 seasons
  filter(N.SEASONS >= 2) %>% 
  distinct(OBSERVER.ID, FULL.NAME, LOCALITY.ID) %>% 
  ungroup()



# breeding code classification
breedingcodes <- data.frame(
  BREEDING.CODE = c("NA",
                    "S", 
                    "S7", "M", "T", "C", "N", "A", "B",
                    "PE", "CN", "NB", "DD", "UN", "ON", "NE", "NY", "FL", "CF", "FY", "FS"),
  BREEDING.TYPE = c("No evidence",
                    "Possible", 
                    rep("Probable", 7), 
                    rep("Confirmed", 12)),
  BREEDING.ORDER = c(1:21),
  LABEL = c("No evidence of breeding",
            "Singing\nbird",
            #
            "Singing\n7+ days", "Multiple\nsinging\nbirds", "Territorial\nbehaviour",
            "Courtship,\ndisplay or\ncopulation", "Visiting\nprobable\nnest site",
            "Agitated\nbehaviour", "Woodpecker/wren\nnest building", 
            #
            "Physiological\nevidence", "Carrying\nnesting\nmaterial", "Nest\nbuilding", 
            "Distraction\ndisplay", "Used nest", "Occupied\nnest", "Nest with\neggs",
            "Nest with\nyoung", "Recently\nfledged\nyoung", "Carrying\nfood", 
            "Feeding\nyoung", "Carrying\nfaecal sac")) %>% 
  mutate(
    BREEDING.CODE = factor(BREEDING.CODE, levels = c("NA",
                                                     "S", 
                                                     "S7", "M", "T", "C", "N", "A", "B",
                                                     "PE", "CN", "NB", "DD", "UN", "ON", "NE", "NY", "FL", "CF", "FY", "FS")),
    BREEDING.TYPE = factor(BREEDING.TYPE, levels = c("No evidence", "Possible", 
                                                     "Probable", "Confirmed"))
  )

# species for which breeding codes patterns should be explored
# (must be present in every month per season, in at least two seasons)
filt_breedspecloc <- data_pmp %>%
  # trimming whitespace in breeding code values
  mutate(BREEDING.CODE = str_trim(BREEDING.CODE)) %>% 
  # if no breeding code, changing it to "no breeding evidence"
  mutate(BREEDING.CODE = if_else(is.na(BREEDING.CODE), "NA", BREEDING.CODE)) %>% 
  left_join(breedingcodes) %>% 
  # keeping only observations with useful breeding codes
  filter(BREEDING.CODE %in% breedingcodes$BREEDING.CODE) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, COMMON.NAME) %>%
  summarise(N.SEASONS = n_distinct(SEASON),
            N.BCODES = n_distinct(BREEDING.CODE),
            N.BTYPES = n_distinct(BREEDING.TYPE)) %>%
  # at least 2 seasons and 3 unique breeding codes
  filter(N.SEASONS >= 2 & N.BCODES >= 3) %>%
  distinct(OBSERVER.ID, FULL.NAME, LOCALITY.ID, COMMON.NAME) %>% 
  ungroup()


##### map of PMP observations ####

map_pmp <- data_pmp %>% 
  group_by(OBSERVER.ID, LOCALITY.ID, LONGITUDE, LATITUDE) %>% 
  summarise(N.SEASONS = n_distinct(SEASON)) %>% 
  ggplot(aes(x = LONGITUDE, y = LATITUDE)) +
  geom_polygon(data = indiamap, 
               aes(x = long, y = lat, group = group), 
               colour = NA, fill = "#B7B7B8") +
  geom_point(aes(colour = factor(N.SEASONS)), 
             size = 4, alpha = 0.2, stroke = 0) +
  scale_colour_viridis_d(name = "Number of seasons") +
  # scale_colour_brewer(palette = 3, name = "Number of seasons", direction = -1) +
  labs(title = "Patch Monitoring Project \nacross the country",
       subtitle = " ") +
  theme(axis.line = element_blank(),
        axis.text = element_blank(),
        axis.ticks = element_blank(),
        axis.title = element_blank(),
        panel.grid = element_blank(),
        panel.border = element_blank(),
        plot.background = element_rect(fill = "#EAEAEB", colour = NA),
        panel.background = element_rect(fill = "#EAEAEB", colour = NA),
        plot.title = element_text(hjust = 0, vjust = -0.8, lineheight = 0.8,
                                  size = 18),
        plot.title.position = "panel",
        legend.direction = "horizontal",
        legend.position = c(0.6, 0.25),
        legend.background = element_blank(),
        legend.text = element_text(size = 7),
        legend.title = element_text(size = 10),
        # no fill around legend points
        legend.key = element_blank(),
        legend.spacing.x = unit(0, "mm"),
        legend.spacing.y = unit(0, "mm")) +
  # legend title above but keys horizontal
  guides(colour = guide_legend(title.position = "top",
                               # so points in legend are not translucent
                               override.aes = list(alpha = 1))) +
  coord_cartesian(clip = "off") +
  annotation_custom(textGrob(label = data_annotation, 
                             hjust = 0,
                             gp = gpar(col = "#ADADAD", 
                                       cex = 1.0,
                                       fontface = "italic",
                                       fontsize = 6)),
                    xmin = 66, xmax = 69,
                    ymin = 5.5, ymax = 6.5) +
  annotation_raster(logo1, 
                    ymin = 40, ymax = 42,
                    xmin = 88.5, xmax = 94.1) +
  annotation_raster(logo2, 
                    ymin = 40, ymax = 42,
                    xmin = 95.1, xmax = 98.6)

ggsave(glue("pmp-yearly-patterns/{currel_year}/pmp-map_{currel_year}.png"), map_pmp, 
       dpi = 300, width = 6, height = 7, units = "in")


##### change in frequency ####

temp1 <- data_pmp %>% 
  group_by(OBSERVER.ID, LOCALITY.ID, SEASON) %>% 
  summarise(TOT.LISTS = n_distinct(SAMPLING.EVENT.IDENTIFIER))

temp2 <- data_pmp %>% 
  distinct(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY, SEASON, COMMON.NAME) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY) %>% 
  complete(nesting(SEASON), COMMON.NAME) %>% 
  left_join(temp1)


data1 <- data_pmp %>% 
  right_join(temp2) %>% 
  right_join(filt_specloc_RC) %>%
  right_join(filt_spec_RC) %>% 
  arrange(OBSERVER.ID, LOCALITY.ID, SEASON, SAMPLING.EVENT.IDENTIFIER) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY, COMMON.NAME, SEASON) %>% 
  summarise(N.LISTS = ifelse(anyNA(SAMPLING.EVENT.IDENTIFIER),
                          0, 
                          n_distinct(SAMPLING.EVENT.IDENTIFIER)),
            TOT.LISTS = min(TOT.LISTS),
            REP.FREQ = (N.LISTS/TOT.LISTS) * 100) %>%
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY, COMMON.NAME) %>% 
  complete(SEASON = unique(data_pmp$SEASON), 
           fill = list(N.LISTS = NA_integer_,
                       REP.FREQ = NA_integer_,
                       TOT.LISTS = 0)) %>% 
  ungroup()


for (obs in 1:n_distinct(data1$OBSERVER.ID)) {

  obs_temp <- unique(data1$OBSERVER.ID)[obs]
  obsname_temp <- data1 %>% distinct(OBSERVER.ID, FULL.NAME) %>% 
    filter(OBSERVER.ID %in% obs_temp) %>% distinct(FULL.NAME) %>% as.character()
  data_temp1 <- filter(data1, OBSERVER.ID == obs_temp)
  
  
  for (spec in 1:n_distinct(data_temp1$COMMON.NAME)) {
    
    print(glue("Loop progress: observer {obs}, species {spec}"))
    
    spec_temp <- unique(data_temp1$COMMON.NAME)[spec]
    
    data_temp2 <- filter(data_temp1, COMMON.NAME == spec_temp) %>% 
      group_by(LOCALITY) %>% 
      # for sample sizes
      mutate(LOCALITY.N = glue("<b>{LOCALITY}</b><br>N<sub>Su</sub> = {TOT.LISTS[1]}, N<sub>Au</sub> = {TOT.LISTS[2]}, N<sub>Wi</sub> = {TOT.LISTS[3]}, N<sub>Sp</sub> = {TOT.LISTS[4]}")) %>% 
      ungroup()


    path_temp <- glue("pmp-yearly-patterns/{currel_year}/{obsname_temp}/Reporting frequency of species/")
    file_temp <- glue("{currel_year}_RF_{str_replace(spec_temp, ' ', '-')}.png")
    
    
    # setting up dimensions and header for the figure
    gen_fig_setup(data_temp2, metric = 1)

        
    plot_temp <- (header) / 
      (ggplot(data_temp2, 
              aes(as.numeric(SEASON), REP.FREQ, colour = COMMON.NAME)) +
      facet_wrap(~ LOCALITY.N, ncol = 1) +
      scale_y_continuous(limits = c(0,100)) +
      # x axis should have four seasons always so taking from previous data object
      scale_x_continuous(limits = c(1, 4),
                         labels = unique(data_temp2$SEASON)) +
      geom_point(size = 4, position = position_dodge(width = 0.2), colour = "black") +
      geom_line(size = 1, position = position_dodge(width = 0.2), colour = "black") +
      labs(x = "Season", y = "Reporting frequency (%)") +
      theme(axis.line = element_blank(),
            axis.ticks = element_blank(),
            axis.text = element_text(size = 8),
            axis.title = element_text(size = 10),
            panel.grid = element_blank(),
            panel.grid.major.y = element_line(linetype = 3, colour = "#C2C2C2"),
            strip.text = element_markdown(size = 7, lineheight = 1.2),
            strip.background = element_rect(fill = "#D6D6D6", colour = NA),
            plot.background = element_rect(fill = "#EAEAEB", colour = NA),
            panel.background = element_rect(fill = "#EAEAEB", colour = NA),
            legend.position = "none",
            # legend.background = element_blank(),
            # legend.text = element_text(size = 7),
            # # no fill around legend points
            # legend.key = element_blank(), 
            panel.spacing = unit(2, "lines"),
            plot.margin = unit(c(1, 0.5, 1, 0.5), "lines"))) +
      plot_layout(heights = c(patch_a, patch_b)) &
      theme(plot.background = element_rect(fill = "#EAEAEB", colour = NA),
            panel.background = element_rect(fill = "#EAEAEB", colour = NA))

    if (!dir.exists(path_temp)) (dir.create(path_temp, recursive = T))
      
    ggsave(filename = glue("{path_temp}{file_temp}"), 
           plot = plot_temp, 
           dpi = 300, width = 6, height = fig_inches, units = "in")
    
  }

}


##### change in species richness ####

data2 <- data_pmp %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY, LOCALITY.ID, 
           SEASON, SAMPLING.EVENT.IDENTIFIER) %>% 
  summarise(NO.SP = n_distinct(COMMON.NAME)) %>% 
  summarise(NO.SP = boot_conf(x = NO.SP)) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY, LOCALITY.ID, SEASON) %>% 
  summarise(SE = sd(NO.SP),
            NO.SP = mean(NO.SP),
            CI.L = NO.SP - 1.96*SE,
            CI.U = NO.SP + 1.96*SE) %>% 
  ungroup() %>% 
  right_join(filt_loc) %>% 
  left_join(samplesizes) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY) %>% 
  complete(SEASON = unique(data_pmp$SEASON), 
           fill = list(TOT.LISTS = 0)) %>% 
  ungroup() %>% 
  mutate(NO.SP = case_when(TOT.LISTS != 0 & is.na(NO.SP) ~ 0,
                           # TOT.LISTS == 0 & is.na(NO.SP) ~ NA_integer_, # already NA
                           TOT.LISTS != 0 & !is.na(NO.SP) ~ NO.SP),
         CI.L = case_when(TOT.LISTS != 0 & is.na(CI.L) ~ 0,
                          TOT.LISTS != 0 & !is.na(CI.L) ~ CI.L),
         CI.U = case_when(TOT.LISTS != 0 & is.na(CI.U) ~ 0,
                          TOT.LISTS != 0 & !is.na(CI.U) ~ CI.U))

for (obs in 1:n_distinct(data2$OBSERVER.ID)) {
  
  obs_temp <- unique(data2$OBSERVER.ID)[obs]
  obsname_temp <- data2 %>% distinct(OBSERVER.ID, FULL.NAME) %>% 
    filter(OBSERVER.ID %in% obs_temp) %>% distinct(FULL.NAME) %>% as.character()
  data_temp1 <- filter(data2, OBSERVER.ID == obs_temp) %>% 
    group_by(LOCALITY) %>% 
    # for sample sizes
    mutate(LOCALITY.N = glue("<b>{LOCALITY}</b><br>N<sub>Su</sub> = {TOT.LISTS[1]}, N<sub>Au</sub> = {TOT.LISTS[2]}, N<sub>Wi</sub> = {TOT.LISTS[3]}, N<sub>Sp</sub> = {TOT.LISTS[4]}")) %>% 
    ungroup()
  
  print(glue("Loop progress: observer {obs}"))
  
  
  path_temp <- glue("pmp-yearly-patterns/{currel_year}/{obsname_temp}/Species richness/")
  file_temp <- glue("{currel_year}_SR.png")
  
  # setting up dimensions and header for the figure
  gen_fig_setup(data_temp1, metric = 2)
  
  
  plot_temp <- (header) / 
    (ggplot(data_temp1, 
            aes(as.numeric(SEASON), NO.SP)) +
       facet_wrap(~ LOCALITY.N, ncol = 1, scales = "free") +
       geom_ribbon(aes(ymin = CI.L, ymax = CI.U),
                   colour = NA, fill = "#ADADAD", alpha = 0.2) +
       geom_point(size = 3, colour = "black") +
       geom_line(size = 1, colour = "black") +
       scale_x_continuous(labels = unique(data_temp1$SEASON), 
                          breaks = unique(as.numeric(data_temp1$SEASON)),
                          limits = c(1, 4)) +
       {conditional_ybreaks(0, 100, 4, data_temp1$CI.U, data_temp1$CI.L)} +
       labs(x = "Season", y = "Number of species reported") +
       theme(axis.line = element_blank(),
             axis.ticks = element_blank(),
             axis.text = element_text(size = 8),
             axis.title = element_text(size = 10),
             panel.grid = element_blank(),
             panel.grid.major.y = element_line(linetype = 3, colour = "#C2C2C2"),
             strip.text = element_markdown(size = 7, lineheight = 1.2),
             strip.background = element_rect(fill = "#D6D6D6", colour = NA),
             plot.background = element_rect(fill = "#EAEAEB", colour = NA),
             panel.background = element_rect(fill = "#EAEAEB", colour = NA),
             # legend.background = element_blank(),
             # legend.text = element_text(size = 7),
             # # no fill around legend points
             # legend.key = element_blank(), 
             panel.spacing = unit(2, "lines"),
             plot.margin = unit(c(1, 0.5, 1, 0.5), "lines"))) +
    plot_layout(heights = c(patch_a, patch_b)) &
    theme(plot.background = element_rect(fill = "#EAEAEB", colour = NA),
          panel.background = element_rect(fill = "#EAEAEB", colour = NA))

  if (!dir.exists(path_temp)) (dir.create(path_temp, recursive = T))

  ggsave(filename = glue("{path_temp}{file_temp}"), 
         plot = plot_temp, 
         dpi = 300, width = 6, height = fig_inches, units = "in")
  
}

##### change in total counts ####

agg_spec <- c("Common Myna", "Red-vented Bulbul", "Rose-ringed Parakeet", "Black Kite", 
                "Brahminy Kite", "Cattle Egret", "Indian White-eye", "House Crow",
                "Green Bee-Eater")

temp1 <- data_pmp %>% 
  group_by(OBSERVER.ID, LOCALITY.ID, SEASON) %>% 
  summarise(TOT.LISTS = n_distinct(SAMPLING.EVENT.IDENTIFIER)) 
  
temp2 <- data_pmp %>% 
  distinct(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY, SEASON, COMMON.NAME) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY) %>% 
  complete(nesting(SEASON), COMMON.NAME) %>% 
  left_join(temp1) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY.ID, LOCALITY, COMMON.NAME) %>% 
  complete(SEASON = unique(data_pmp$SEASON), 
           fill = list(TOT.LISTS = 0)) 

data3 <- data_pmp %>% 
  right_join(temp2) %>% 
  right_join(filt_specloc) %>% 
  filter(COMMON.NAME %in% agg_spec) %>% 
  arrange(OBSERVER.ID, LOCALITY.ID, SEASON, SAMPLING.EVENT.IDENTIFIER) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY, COMMON.NAME, SEASON, TOT.LISTS) %>% 
  summarise(COUNT = boot_conf(OBSERVATION.COUNT)) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY, COMMON.NAME, SEASON, TOT.LISTS) %>% 
  summarise(SE = sd(COUNT),
            COUNT = mean(COUNT),
            CI.L = COUNT - 1.96*SE,
            CI.U = COUNT + 1.96*SE) %>% 
  mutate(COUNT = case_when(TOT.LISTS != 0 & is.na(COUNT) ~ 0,
                           # TOT.LISTS == 0 & is.na(COUNT) ~ NA_integer_, # already NA
                           TOT.LISTS != 0 & !is.na(COUNT) ~ COUNT),
         CI.L = case_when(TOT.LISTS != 0 & is.na(CI.L) ~ 0,
                           TOT.LISTS != 0 & !is.na(CI.L) ~ CI.L),
         CI.U = case_when(TOT.LISTS != 0 & is.na(CI.U) ~ 0,
                           TOT.LISTS != 0 & !is.na(CI.U) ~ CI.U)) %>% 
  ungroup()
# 
#   group_by(OBSERVER.ID, FULL.NAME, LOCALITY, COMMON.NAME) %>% 
#   complete(SEASON = unique(data_pmp$SEASON), 
#            fill = list(COUNT = NA_integer_,
#                        CI.L = NA_integer_,
#                        CI.U = NA_integer_,
#                        SE = NA_integer_,
#                        TOT.LISTS = 0)) %>% 
#   ungroup()


for (obs in 1:n_distinct(data3$OBSERVER.ID)) {
  
  obs_temp <- unique(data3$OBSERVER.ID)[obs]
  obsname_temp <- data3 %>% distinct(OBSERVER.ID, FULL.NAME) %>% 
    filter(OBSERVER.ID %in% obs_temp) %>% distinct(FULL.NAME) %>% as.character()
  data_temp1 <- filter(data3, OBSERVER.ID == obs_temp)
  
  for (spec in 1:n_distinct(data_temp1$COMMON.NAME)) {
    
    print(glue("Loop progress: observer {obs}, species {spec}"))
    
    spec_temp <- unique(data_temp1$COMMON.NAME)[spec]
    data_temp2 <- filter(data_temp1, COMMON.NAME == spec_temp) %>% 
      group_by(LOCALITY) %>% 
      # for sample sizes
      mutate(LOCALITY.N = glue("<b>{LOCALITY}</b><br>N<sub>Su</sub> = {TOT.LISTS[1]}, N<sub>Au</sub> = {TOT.LISTS[2]}, N<sub>Wi</sub> = {TOT.LISTS[3]}, N<sub>Sp</sub> = {TOT.LISTS[4]}")) %>% 
      ungroup()
    
    
    path_temp <- glue("pmp-yearly-patterns/{currel_year}/{obsname_temp}/Species counts/")
    file_temp <- glue("{currel_year}_SC_{str_replace(spec_temp, ' ', '-')}.png")
    
    # setting up dimensions and header for the figure
    gen_fig_setup(data_temp2, metric = 3)
    
    
    plot_temp <- (header) / 
      (ggplot(data_temp2, 
              aes(as.numeric(SEASON), COUNT, 
                  colour = COMMON.NAME, group = COMMON.NAME)) +
         facet_wrap(~ LOCALITY.N, ncol = 1, scales = "free") +
         geom_ribbon(aes(ymin = CI.L, ymax = CI.U),
                     colour = NA, fill = "#ADADAD", alpha = 0.2) +
         geom_point(size = 3, colour = "black") +
         geom_line(size = 1, colour = "black") +
         scale_x_continuous(labels = unique(data_temp1$SEASON), 
                            breaks = unique(as.numeric(data_temp1$SEASON)),
                            limits = c(1, 4)) +
         # scale_y_continuous(expand = expansion(mult = c(0, 0.1))) +
         labs( x = "Season", y = "Total count") +
         theme(axis.line = element_blank(),
               axis.ticks = element_blank(),
               axis.text = element_text(size = 8),
               axis.title = element_text(size = 10),
               panel.grid = element_blank(),
               panel.grid.major.y = element_line(linetype = 3, colour = "#C2C2C2"),
               strip.text = element_markdown(size = 7, lineheight = 1.2),
               strip.background = element_rect(fill = "#D6D6D6", colour = NA),
               plot.background = element_rect(fill = "#EAEAEB", colour = NA),
               panel.background = element_rect(fill = "#EAEAEB", colour = NA),
               # legend.background = element_blank(),
               # legend.text = element_text(size = 7),
               # # no fill around legend points
               # legend.key = element_blank(), 
               panel.spacing = unit(2, "lines"),
               plot.margin = unit(c(1, 0.5, 1, 0.5), "lines"))) +
      plot_layout(heights = c(patch_a, patch_b)) &
      theme(plot.background = element_rect(fill = "#EAEAEB", colour = NA),
            panel.background = element_rect(fill = "#EAEAEB", colour = NA))

    if (!dir.exists(path_temp)) (dir.create(path_temp, recursive = T))
    
    ggsave(filename = glue("{path_temp}{file_temp}"), 
           plot = plot_temp, 
           dpi = 300, width = 6, height = fig_inches, units = "in")
  
  }
  
}



##### change in breeding codes ####

data4 <- data_pmp %>% 
  # trimming whitespace in breeding code values
  mutate(BREEDING.CODE = str_trim(BREEDING.CODE),
         OBSERVATION.DATE = as_date(OBSERVATION.DATE)) %>% 
  # if no breeding code, changing it to "No evidence"
  mutate(BREEDING.CODE = if_else(is.na(BREEDING.CODE), "NA", BREEDING.CODE)) %>% 
  right_join(filt_breedspecloc) %>% 
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY, COMMON.NAME, SEASON) %>% 
  distinct(OBSERVATION.DATE, DAY.M, MONTH, BREEDING.CODE) %>% 
  ungroup() %>% 
  # keeping only observations with useful breeding codes
  filter(BREEDING.CODE %in% breedingcodes$BREEDING.CODE) %>% 
  left_join(breedingcodes) %>% 
  mutate(
    BREEDING.CODE = factor(BREEDING.CODE, levels = c("NA",
                                                     "S", 
                                                     "S7", "M", "T", "C", "N", "A", "B",
                                                     "PE", "CN", "NB", "DD", "UN", "ON", "NE", "NY", "FL", "CF", "FY", "FS")),
    BREEDING.TYPE = factor(BREEDING.TYPE, levels = c("No evidence", "Possible", 
                                                     "Probable", "Confirmed"))
    ) %>% 
  # summarising by month (max breeding code in a month)
  group_by(OBSERVER.ID, FULL.NAME, LOCALITY, COMMON.NAME, SEASON, MONTH) %>% 
  arrange(desc(BREEDING.ORDER)) %>% 
  slice(1) %>% 
  ungroup()

timeline <- data.frame(OBSERVATION.DATE = seq(pmpstartdate, date_real, by = "month")) %>% 
  mutate(MONTH = month(OBSERVATION.DATE),
         MONTH.LABEL = month(OBSERVATION.DATE, label = T),
         YEAR = year(OBSERVATION.DATE),
         LABEL = glue("{MONTH.LABEL} '{str_trunc(YEAR, 2, 'left', ellipsis = '')}"))

breedinglabels <- breedingcodes %>% 
  filter(BREEDING.ORDER %in% c(2, 6, 11, 15, 18, 21))

breedingcolours <- breedingcodes %>% 
  # legend colours
  mutate(COLOURS = case_when(BREEDING.TYPE == "No evidence" ~ "#D2C1B7", 
                             BREEDING.TYPE == "Possible" ~ "#F8CDB5", 
                             BREEDING.TYPE == "Probable" ~ "#F39968", 
                             BREEDING.TYPE == "Confirmed" ~ "#EB661E"))

for (obs in 1:n_distinct(data4$OBSERVER.ID)) {
  
  obs_temp <- unique(data4$OBSERVER.ID)[obs]
  obsname_temp <- data4 %>% distinct(OBSERVER.ID, FULL.NAME) %>% 
    filter(OBSERVER.ID %in% obs_temp) %>% distinct(FULL.NAME) %>% as.character()
  data_temp1 <- filter(data4, OBSERVER.ID == obs_temp)
  
  
  for (spec in 1:n_distinct(data_temp1$COMMON.NAME)) {
    
    print(glue("Loop progress: observer {obs}, species {spec}"))
    
    spec_temp <- unique(data_temp1$COMMON.NAME)[spec]
    
    data_temp2 <- filter(data_temp1, COMMON.NAME == spec_temp) %>% 
      ungroup() %>% 
      # legend colours
      left_join(breedingcolours)
    
    
    path_temp <- glue("pmp-yearly-patterns/{currel_year}/{obsname_temp}/Breeding codes/")
    file_temp <- glue("{currel_year}_BC_{str_replace(spec_temp, ' ', '-')}.png")
    
    
    # setting up dimensions and header for the figure
    gen_fig_setup(data_temp2, metric = 4)
    
    # getting colours for legend in correct order (of the factor levels of BREEDING.TYPE)
    colours_in_scale <- data_temp2 %>% arrange(BREEDING.TYPE) %>% distinct(COLOURS)
    
    plot_temp <- (header) / 
      (ggplot(data_temp2, 
              aes(OBSERVATION.DATE, BREEDING.ORDER, colour = BREEDING.TYPE)) +
         facet_wrap(~ LOCALITY, ncol = 1) +
         scale_y_continuous(limits = c(0, 21),
                            breaks = breedinglabels$BREEDING.ORDER,
                            labels = breedinglabels$LABEL) +
         scale_x_continuous(limits = c(pmpstartdate, date_real),
                            breaks = timeline$OBSERVATION.DATE,
                            labels = timeline$LABEL) +
         geom_line(aes(colour = NA), linetype = 1, colour = "#C2C2C2") +
         geom_point(size = 3, position = position_dodge(width = 0.4)) +
         scale_colour_manual(values = colours_in_scale$COLOURS) +
         labs(x = "Months", y = "Breeding behaviour",
              colour = "Breeding behaviour type") +
         theme(axis.line = element_blank(),
               axis.ticks = element_blank(),
               axis.text = element_text(size = 6),
               axis.text.y = element_text(size = 5),
               axis.title = element_text(size = 10),
               panel.grid = element_blank(),
               panel.grid.major.y = element_line(linetype = 3, colour = "#C2C2C2"),
               strip.text = element_markdown(size = 7, lineheight = 1.2),
               strip.background = element_rect(fill = "#D6D6D6", colour = NA),
               plot.background = element_rect(fill = "#EAEAEB", colour = NA),
               panel.background = element_rect(fill = "#EAEAEB", colour = NA),
               legend.position = "bottom",
               legend.box.margin = margin(0, 0, 0, 0, "in"),
               legend.margin = margin(0, 0, 0, 0, "in"),
               legend.background = element_blank(),
               legend.text = element_text(size = 6),
               legend.title = element_text(size = 8),
               # no fill around legend points
               legend.key = element_blank(),
               panel.spacing = unit(2, "lines"),
               plot.margin = unit(c(1, 0.5, 1, 0.5), "lines"))) +
      plot_layout(heights = c(patch_a, patch_b)) &
      theme(plot.background = element_rect(fill = "#EAEAEB", colour = NA),
            panel.background = element_rect(fill = "#EAEAEB", colour = NA))
    
    if (!dir.exists(path_temp)) (dir.create(path_temp, recursive = T))
    
    ggsave(filename = glue("{path_temp}{file_temp}"), 
           plot = plot_temp, 
           dpi = 300, width = 6, height = fig_inches, units = "in")
    
  }
  
}
