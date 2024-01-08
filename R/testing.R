# Examples from https://www.datacareer.de/blog/connect-to-postgresql-with-r-a-step-by-step-example/

# Remember to connect via the VPN if off-campus

library(DBI)

conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "mkh2", 
                  host = "153.106.113.125", 
                  port = 5432, 
                  user = "mkh2")

my_df <- tibble::tribble(~key, ~val, 
                         "A", 1, 
                         "B", 2, 
                         "C", 3)
dbWriteTable(conn, name = "test_table", my_df)
dbReadTable(conn, "test_table")
# Update a row
new_data <- tibble::tribble(~key, ~val, 
                            "A", 4)
dbExecute(conn, "update test_table set val = 4 where key = 'A'")
dbReadTable(conn, "test_table")

dbRemoveTable(conn, "test_table")

UKEnergy2000 <- Recca::UKEnergy2000mats |> 
  dplyr::rename(matnames = "matrix.name", matvals = "matrix") 
# Fails
dbWriteTable(conn, name = "UKEnergy2000", UKEnergy2000)

UKEnergy2000_tidy <- UKEnergy2000 |> 
  matsindf::expand_to_tidy(drop = 0) 
# Works
dbWriteTable(conn, name = "UKEnergy2000_tidy", UKEnergy2000_tidy)
dbRemoveTable(conn, "UKEnergy2000_tidy")

# Try to write a nested data frame.
UKEnergy2000_nested <- UKEnergy2000_tidy |> 
  tidyr::nest(matdfs = c(rownames, colnames, matvals, rowtypes, coltypes))
# Fails
dbWriteTable(conn, name = "UKEnergy2000_nested", UKEnergy2000_nested)

# Try with a table already created with a nested column
df_nested <- tibble::tribble(~Country, ~Year, ~val, 
                             "USA",    1990,   1, 
                             "USA",    1991,   2, 
                             "USA",    1992,   3, 
                             "GBR",    1990,   4, 
                             "GBR",    1991,   5,
                             "GBR",    1992,   6) |> 
  tidyr::nest(vals = c(Year, val))

# Now create a nested table
dbExecute(conn, "CREATE TABLE df_nested (
                 id serial PRIMARY KEY,
                 Country text,
                 val integer[]);")


dbListTables(conn)

dbWriteTable(conn, name = "df_nested", df_nested, append = TRUE)

dbDisconnect(conn)


