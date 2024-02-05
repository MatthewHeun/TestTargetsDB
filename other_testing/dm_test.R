
# Read the sheets in the CL-PFU data model tables.xlsx spreadsheet
data_model_path <- file.path("design", "CL-PFU data model.xlsx")
table_names <- readxl::excel_sheets(data_model_path) |> 
  setdiff("ForeignKeys")
data_model <- table_names |> 
  setNames(table_names) |> 
  lapply(FUN = function(this_sheet) {
    readxl::read_excel(path = data_model_path, sheet = this_sheet)
  }) |> 
  dm::new_dm()

# Add primary keys
pk_suffix <- "ID"
for (this_table in table_names) {
  this_pk_col <- paste0(this_table, pk_suffix)
  data_model <- data_model |> 
    dm::dm_add_pk(table = {{this_table}}, columns = {{this_pk_col}}, autoincrement = TRUE)
}
# Add foreign keys
fk_table <- readxl::read_excel(path = data_model_path, sheet = "ForeignKeys")
for (this_row_num in 1:nrow(fk_table)) {
  this_row <- dplyr::slice(fk_table, this_row_num)
  child_table <- this_row$child_table
  child_fk_col <- this_row$child_fk_col
  parent_table <- this_row$parent_table
  parent_table_pk_col <- this_row$parent_pk_col
  data_model <- data_model |> 
    dm::dm_add_fk(table = {{child_table}},
                  columns = {{child_fk_col}}, 
                  ref_table = {{parent_table}},
                  ref_columns = {{parent_table_pk_col}})
}


# Because we used dm::new_dm(), we should do a validation step
dm::dm_validate(data_model)

# View the data model
data_model <- data_model |> 
  dm::dm_set_colors(blue = IEAMW, red = Energy.type, 
                    darkgreen = ECC.stage, lightyellow = Ledger.side, 
                    pink = Country, black = PSUT, 
                    purple = Year)
dm::dm_draw(data_model, view_type = "all")


# Get foreign key columns for the PSUT table.
# This will be quite handy, because 
# we can nest all other columns 
# besides the foreign key cols and
# generate a hash for the targets pipeline.
data_model |> 
  dm::dm_get_all_fks() |> 
  dplyr::filter(child_table == "PSUT") |> 
  dplyr::select(child_fk_cols) |> 
  unlist() |> 
  unname()

# Look at the PSUT data frame. 
# Does it still have strings in foreign key columns?
# Or have the strings been converted to integers?
# It has strings.
data_model |> 
  dm::pull_tbl(PSUT)

# Try to add a new row to PSUT. 
# Don't include PSUTID
new_psut_data <- dm::dm(PSUT = tibble::tribble(~Country, ~Year, ~Energy.type, ~Last.stage, ~IEAMW, ~Value, 
                                               "USA", 1960, "Energy", "Final", "IEA", 43, 
                                               "USA", 1961, "Exergy", "Useful", "MW", 44))
# Fails, because PSUTID is not present.
data_model <- data_model |> 
  dm::dm_rows_upsert(new_psut_data, in_place = FALSE)


# Does it require integers or strings in foreign key cols? 
# Why is the PSUTID value required? Seems like a lot of janitorial work to keep that straight.
new_psut_data <- dm::dm(PSUT = tibble::tribble(~PSUTID, ~Country, ~Year, ~Energy.type, ~Last.stage, ~IEAMW, ~Value, 
                                               1, "USA", 1960, "Energy", "Final", "IEA", 43, 
                                               11, "USA", 1961, "Exergy", "Useful", "MW", 44))

data_model <- data_model |> 
  dm::dm_rows_upsert(new_psut_data, in_place = FALSE)

# Look at it
data_model |> 
  dm::pull_tbl(PSUT)


# Add a new row (single) and compare sizes

orig_size <- object.size(data_model)

# Try to add some data that has a non-existent foreign key value
new_psut_data_size <- object.size(list(12, "USA", 1960, "Energy", "Final", "Bogus", 45))
expected_new_size <- orig_size + new_psut_data_size

new_psut_data_2 <- dm::dm(PSUT = tibble::tribble(~PSUTID, ~Country, ~Year, ~Energy.type, ~Last.stage, ~IEAMW, ~Value, 
                                                 12,      1,         1960, 1,            2,           42,     45))

data_model <- data_model |> 
  dm::dm_rows_upsert(new_psut_data_2)

new_size <- object.size(data_model)
# Calculate the compression provided by the data model.
# There is significant savings.
expected_new_size - new_size
# This is the growth:
new_size - orig_size
# Compared to the size of the data that was inserted:
new_psut_data_size
# So it must be storing only the integer keys internally.

# Make sure that the mode is still valid
dm::dm_validate(data_model)


# Look at it. Weirdly, "Bogus" was accepted in the IEAMW column.
data_model |> 
  dm::pull_tbl(PSUT)
# Bogus was not added to the IEAMW column of the IEAMW table.
data_model |> 
  dm::pull_tbl(IEAMW)


# Try this all in a database

library(DBI)
conn <- dbConnect(drv = RPostgres::Postgres(),
                  dbname = "playground",
                  host = "eviz.cs.calvin.edu",
                  port = 5432,
                  user = "mkh2")
table_names <- dbListTables(conn)
# Remove all tables
for (this_table_name in table_names) {
  dbRemoveTable(conn, this_table_name)
}

# Upload the data model to database
dm::copy_dm_to(dest = conn, dm = data_model, temporary = FALSE, )
dbListTables(conn)

dbReadTable(conn, "ECC.stage")

# Add a row to PSUT

# Pull some tables




dbDisconnect(conn)
