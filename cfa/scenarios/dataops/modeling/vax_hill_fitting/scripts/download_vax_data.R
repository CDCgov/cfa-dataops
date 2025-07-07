
# Load libraries
library(httr)
library(jsonlite)
library(tidyverse)

# API URL
url <- "https://data.cdc.gov/resource/ksfb-ug5d.json?$limit=100000&$offset=0"
# Make the GET request to the API
response <- GET(url)

# Check if the request was successful
if (status_code(response) == 200) {
  # Parse the JSON content into an R list
  data <- content(response, as = "text")
  json_data <- fromJSON(data, flatten = TRUE)

  # Convert the list to a data frame
  vax <- as.data.frame(json_data)

  # Display the data
  head(vax)
} else {
  print(paste("Error:", status_code(response)))
}

# format data
vax$date <- as.Date(vax$current_season_week_ending)
# alighn 23-24 with 24-25 season, started 3 weeks earlier this year
vax$date1 <- as.Date(ifelse(vax$covid_season == "2023-2024", vax$date-21, vax$date) )

vax$estimate <- as.numeric(vax$estimate)

# download under 17 vax data
url <- "https://data.cdc.gov/resource/ker6-gs6z.json?$limit=100000&$offset=0"
# Make the GET request to the API
response <- GET(url)

# Check if the request was successful
if (status_code(response) == 200) {
  # Parse the JSON content into an R list
  data <- content(response, as = "text")
  json_data <- fromJSON(data, flatten = TRUE)

  # Convert the list to a data frame
  child_vax <- as.data.frame(json_data)

  # Display the data
  head(child_vax)
} else {
  print(paste("Error:", status_code(response)))
}

# format data
child_vax$date <- as.Date(child_vax$week_ending)
child_vax$estimate <- as.numeric(child_vax$estimate)



# national level child vax data
child_vax_usa <- child_vax %>%
  filter(geographic_name == "National",
         demographic_level == "Overall",
         indicator_category_label == "Vaccinated") %>%
  group_by(date,  demographic_level, indicator_category_label, demographic_name) %>%
  reframe(estimate = sum(estimate, na.rm = T))


child_vax_usa$covid_season <- ifelse(child_vax_usa$date <= as.Date("2024-08-01"), "2023-2024", "2024-2025")
child_vax_usa$date <- as.Date(ifelse(child_vax_usa$date <= as.Date("2024-08-01"), child_vax_usa$date +365, child_vax_usa$date))

child_vax_usa$date1 <- as.Date(ifelse(child_vax_usa$date <= as.Date("2024-08-01"), child_vax_usa$date +7, child_vax_usa$date))
child_vax_usa$estimate <- child_vax_usa$estimate * 100
child_vax_usa$demographic_name <- "0-17"


print(child_vax_usa,
  width = Inf, n=60)

#save vax data
