library(tidyverse)
library(data.table)

load_population_data <- function(pop_folder_path) {
  # List all files in the folder matching the specific pattern
  files <- list.files(
    path = pop_folder_path,
    pattern = "United_States_subnational_.*_age_distribution_85\\.csv$",
    full.names = TRUE
  )
  
  # Check if any files match the pattern
  if (length(files) == 0) {
    stop("No matching files found. Please check the folder path or file names.")
  }
  
  # Initialize an empty list to store the data
  data_list <- list()
  
  # Loop through each file
  for (file in files) {
    data <- read.csv(file, stringsAsFactors = FALSE, header = FALSE)
    
    # extract the state name from the file name
    file_name <- basename(file)
    state_name <- sub("United_States_subnational_", "", file_name)
    state_name <- sub("_age_distribution_85\\.csv$", "", state_name)

    # Add the state name as a new column
    data$state <- state_name

    # Standardize column names
    colnames(data) <- tolower(gsub("\\s+",
      "_", colnames(data)))  # Convert to lowercase and replace spaces with underscores

    # Append the data frame to the list
    data_list[[file_name]] <- data
  }

  # Bind all data frames together
  combined_data <- bind_rows(data_list)
  #combined_data <- do.call(rbind, lapply(data_list, function(x) {
  #x[ , intersect(names(x), c("age", "census", "state"))] # Keep only relevant columns
  #  }))

  return(combined_data)
}


pop_folder_path <- "data/pop"
pop <- load_population_data(pop_folder_path)

names(pop) <- c("age", "pop", "state")

# get state abbreviations to join with hhs lookup
pop$state <- gsub("_", " ", pop$state)
pop$state <- state.abb[match(pop$state, state.name)]


# regions lookup
hhs <- c(
  4, 10, 9, 6, 9, 8, 1, 3, 4, 4, 9, 10, 5, 5, 7,
  7, 4, 6, 1, 3, 1, 5, 5, 4, 7, 8, 7, 9, 1, 2,
  6, 2, 4, 8, 5, 6, 10, 3, 1, 4, 8, 4, 6, 8, 1,
  4, 10, 3, 5, 8
)
hhs_lookup <- data.frame(state = state.abb, hhs = hhs)
# join with regions
pop <- pop %>% left_join(hhs_lookup)
