# data for Ed for vaccination projections
# get vaccinations by state in each AG using the vaccination rates nationally by AG
setwd("C:/Users/XNG3/Downloads/scenarios_covideda-master/vaxfitting")
source("download_vax_data.R")
source("populations_into_agegroups.R") #need to download US data first

# get proportion of people in each age group vaxx'd every day


# start with only 18+ 
vax_national <- vax %>% 
  filter(geographic_name == "National",
         demographic_level == "Age", 
         indicator_category_label == "Received a vaccination", 
         demographic_name %in% c("18-49 years",  "50-64 years", "65+ years"),
         covid_season == "2024-2025")

vax_national$agegroup <- gsub(" years", "", vax_national$demographic_name)

# for some reason the 50-64 age group has two entries per date 
# that are very similar to eachother... take the average?
vax_national <- vax_national %>% 
  group_by(date, geographic_name, demographic_level,
           indicator_category_label, agegroup, covid_season) %>% 
  reframe(estimate = mean(estimate))

# add population data to get number vax'd

vax_national <- vax_national %>% 
  left_join(pop_agegroups_national) %>% 
  mutate(vax_prop = estimate/100) 

# find proportion of new vaccinations at each day in each
# age group


# adult data by state
vax_state <- vax %>% 
  filter(geographic_level == "State", 
         demographic_level == "Overall",
         indicator_category_label == "Received a vaccination",
         covid_season == "2024-2025")
# get number of people vaccinated in each state at each time
pop_state <- pop_agegroups %>% 
  group_by(state) %>% reframe(pop = sum(pop))
vax_state$state <- state.abb[match(vax_state$geographic_name, state.name)]

vax_state_pop <- vax_state %>% left_join(pop_state) # need total without 17 and under

# get the number vax'd in each state at each time point
vax_state_pop <- vax_state_pop %>% 
  mutate(vax_prop_state = estimate/100)

# assume the relative proportion of people vaccinated at each time point 
# found at the national level is the same in every state

vax_national_new <- vax_national %>% select(date, agegroup, vax_prop )
vax_national_total <- vax %>% 
  filter(geographic_name == "National",
         demographic_level == "Overall", 
         indicator_category_label == "Received a vaccination", 
         covid_season == "2024-2025")

vax_national_total$vax_prop_total <- vax_national_total$estimate/100

# join national with state
vax_both <- vax_national_new %>% left_join(vax_state_pop, by ="date") %>% left_join(vax_national_total %>% select(date, vax_prop_total))

# multiply national proportion by age group by number in each state
vax_both <- vax_both %>% 
  mutate(prop_state = vax_prop_state/vax_prop_total * vax_prop) %>% 
  select(date, state, agegroup, vax_prop_state, vax_prop_total, vax_prop, prop_state,  )


# now get the population for each state by age group
vax_both %>% left_join(pop_agegroups) # %>% 
  mutate(prop = total_ag/pop) %>% View()


# save child vax at state level

child_vax_state <- child_vax %>% filter(geographic_level == "State", demographic_level == "Overall", 
                                        indicator_category_label == "Vaccinated",
                                        date >= as.Date("2024-09-01"))

child_vax_region <- child_vax %>% filter(geographic_level == "Region", demographic_level == "Overall", 
                                         indicator_category_label == "Vaccinated",
                                         date >= as.Date("2024-09-01"))
child_vax_region$agegroup <- "0-17"
child_vax_region$region <- child_vax_region$geographic_name
child_vax_region_final <- child_vax_region %>% 
  select(date, agegroup, region, prop_region = estimate) %>% 
  filter(!is.na(region))
child_vax_state$state <- state.abb[match(child_vax_state$geographic_name, state.name)]
child_vax_state$agegroup <- "0-17"
child_vax_state_final <- child_vax_state %>% 
  select(date, agegroup, state, prop_state = estimate) %>% 
  filter(!is.na(state))

child_vax_state_final %>% bind_rows(vax_both %>% select(date, state, agegroup, prop_state)) %>% 
  write_csv("vax_by_state_all_agroups2425.csv")

