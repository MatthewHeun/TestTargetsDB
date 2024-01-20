# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(DBI)
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c("tibble")
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# source("other_functions.R") # Source other scripts as needed.

conn_args <- list(dbname = "playground",
                  host = "153.106.113.125",
                  port = 5432,
                  user = "mkh2")

# dbDisconnect(conn)

# Replace the target list below with your own:
list(
  # tar_target(
  #   name = N, 
  #   command = 10,
  # ),
  # tar_target(
  #   name = DataTarget,
  #   command = make_data(n = N)
  # ),
  # tar_target(
  #   name = Model,
  #   command = make_model(data_hash = DataTarget, conn)
  # ), 
  
  tar_target(
    name = Countries, 
    command = c("A", "B", "C")
  ),
  
  tar_target(
    name = DF, 
    command = make_df(conn_args)
  ), 

  tar_target(
    name = Processed, 
    command = process(DF, conn_args), 
    pattern = map(DF), 
    iteration = "group"
  )
)
