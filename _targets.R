# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(DBI)
library(doltr)
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c("tibble")
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# source("other_functions.R") # Source other scripts as needed.

# Replace the target list below with your own:
list(
  tar_target(
    name = db_path, 
    command = "~/dolthub/testdb"
  ),
  tar_target(
    name = data_target,
    command = make_data(n = 100, db_path = db_path)
  ),
  tar_target(
    name = model,
    command = make_model(data_hash = data_target, db_path = db_path)
  )
)
