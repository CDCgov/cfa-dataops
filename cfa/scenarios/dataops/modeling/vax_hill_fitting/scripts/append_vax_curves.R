# append last year's data to the end of this year's curve

library(readr)
library(tidyverse)

source("vax_at_state_level.R")
# last year by age group
covid_vax_2324 <- read_csv("COVID_RD18_Vaccination_curves.csv")

# rename age groups
covid_vax_2324[covid_vax_2324$Age == "6 months-17 years",]$Age <- "0-17"
covid_vax_2324$Age <- gsub(" years", "", covid_vax_2324$Age)
covid_vax_2324$state <- state.abb[match(covid_vax_2324$Geography, state.name)]
covid_vax_2324 <- covid_vax_2324 %>% 
  filter(Risk_group == "Overall" & state %in% state.abb) %>% 
  rename(agegroup = Age)
covid_vax_2324$agegroup <-  gsub(" – ", "-",covid_vax_2324$agegroup)



# vax by state final

vax_2425 <- read_csv("vax_by_state_all_agroups2425_20250211.csv")


# shift 23-24 back one day to match the current year data
covid_vax_2324$Date <- covid_vax_2324$Date - 1

covid_vax_2324 <- covid_vax_2324 %>% 
  rename(date = Date)
covid_vax_2324$date_shifted <- covid_vax_2324$date - 21

# most recent vax date
last_date <- max(vax_2425$date)
# filter last years data by most recent date
covid_vax_post <- covid_vax_2324 %>% filter(date >= as.Date(last_date ))

# get last data point by state and add that to all in last years data
vax_2425_max <- vax_2425 %>% 
#  filter(!(state == "AZ" & date > as.Date("2024-10-26"))) %>% 
  group_by(state, agegroup) %>% 
  filter(!is.na(prop_state)) %>% 
  filter(prop_state == max(prop_state)) %>% 
  filter(date == max(date))

# join together
covid_vax_post <- covid_vax_post %>% 
  left_join(vax_2425_max %>% 
              select(-date))

# want last date to match with current 

adjusted_data <- covid_vax_post %>%
  group_by(state, agegroup) %>%
  mutate(
    # Calculate the difference for the specific date within each group
    difference = if_else(date == last_date, prop_state - Cum_Coverage_Percent, NA_real_),
    # Carry forward the calculated difference within the group
    difference = max(difference, na.rm = TRUE),
    # Shift the data after the specific date by the difference
    prop_state = Cum_Coverage_Percent + difference) %>%
  ungroup() %>%
  select(-difference)


# high 

adjusted_high <- covid_vax_post %>%
  group_by(state, agegroup) %>%
  mutate(
    # Calculate the difference for the specific date within each group
    difference = if_else(date == last_date, prop_state - Cum_Coverage_Percent, NA_real_),
    # Carry forward the calculated difference within the group
    difference = max(difference, na.rm = TRUE),
    # Shift the data after the specific date by the difference
    prop_state = Cum_Coverage_Percent + 1.25* difference) %>%
  ungroup() %>%
  select(-difference)

adjusted_intercept <- vax_2425_max %>% 
#  filter(date == as.Date("2024-11-09")) %>% 
  rename(intercept = prop_state) 


adjusted_low <- covid_vax_post %>% 
  select(agegroup, date, state, Cum_Coverage_Percent) %>% 
  left_join(adjusted_intercept %>% select(-date)) %>% 
  filter(date > last_date) %>% 
  mutate()
    
# just get the increase in the curve after 11/09 from last year

vax_after <- covid_vax_post %>% 
group_by(state, agegroup) %>%
  mutate(
    # Get col1 value for the specific date within each group
    reference_value = Cum_Coverage_Percent[date == last_date],
    # Subtract the reference value from col1 for all rows in the group
    adjusted = Cum_Coverage_Percent- reference_value
  ) %>%
  ungroup() %>%
  select(-reference_value) %>% 
  select(state, agegroup, date, adjusted)


# join together
vax_post <- vax_after %>% 
  left_join(vax_2425_max %>% 
              select(-date))



# high 

adjusted_high <- vax_post %>%
  group_by(state, agegroup) %>%
  mutate(
    prop_state = prop_state + adjusted * 1.5) %>%
  mutate(prop_state = ifelse(prop_state > 1, 1, prop_state)) %>% 
  ungroup() 



# low
adjusted_low <- vax_post %>%
  group_by(state, agegroup) %>%
  mutate(
    prop_state = prop_state + adjusted * 0.5) %>%
  ungroup() 

# med 
adjusted_med <- vax_post %>%
  group_by(state, agegroup) %>%
  mutate(
    prop_state = prop_state + adjusted) %>%
  ungroup()


# combine
combined_low <- adjusted_low %>% filter(date >= last_date) %>%  bind_rows(vax_2425)
combined_high <- adjusted_high %>% filter(date >= last_date) %>%  bind_rows(vax_2425)
combined_med <- adjusted_med %>% filter(date >= last_date) %>%  bind_rows(vax_2425) %>% select(-adjusted) %>% distinct()

write_csv(combined_med, file = "vax_med50_scenario_20250421.csv")
write_csv(combined_high, file = "vax_high50_scenario_20250421.csv")
write_csv(combined_low, file = "vax_low50_scenario_20250421.csv")





