source("scripts/load_pop_data_byage.R")

# group into age groups
pop$agegroup <- NA
pop[pop$age <= 17,]$agegroup <- "0-17"
pop[pop$age >= 18 & pop$age <= 49,]$agegroup <- "18-49"
pop[pop$age >= 50 & pop$age <= 64,]$agegroup <- "50-64"
pop[pop$age >= 65,]$agegroup <- "65+"

pop_agegroups <- pop %>% 
  group_by(state, agegroup) %>% 
  reframe(pop = sum(pop))

pop_agegroups_national <- pop_agegroups %>% 
  group_by(agegroup) %>% 
  reframe(pop = sum(pop))

pop_agegroups_national %>% write_csv("data/pop_agegroups_national.csv")