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
                  host = "eviz.cs.calvin.edu",
                  port = 5432,
                  user = "mkh2")

# dbDisconnect(conn)

# Replace the target list below with your own:
list(
  tar_target(
    name = KeyCols, 
    command = c("Country", "Last.stage")
  ),
  tar_target(
    name = DF, 
    command = make_df(conn_args, key_cols = KeyCols)
  ), 
  tar_target(
    name = Processed, 
    command = process(DF, conn_args, key_cols = KeyCols), 
    pattern = map(DF), 
    iteration = "group"
  )
)
