# Examples from https://www.datacareer.de/blog/connect-to-postgresql-with-r-a-step-by-step-example/

# Remember to connect via the VPN if off-campus

library(DBI)

conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "playground", 
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
# Fails
dbWriteTable(conn, name = "df_nested", value = df_nested, append = TRUE)
dbRemoveTable(conn, "df_nested")


# Try to read and upload psut data frame
psut_usa <- readRDS("~/Dropbox/Fellowship 1960-2015 PFU database/OutputData/PipelineReleases/psut/20231207T124854Z-73744/psut.rds") |> 
  dplyr::filter(Country == "USA") |> 
  tidyr::pivot_longer(names_to = "matnames", values_to = "matvals", 
                      cols = c("R", "U", "U_feed", "U_EIOU", "r_EIOU", "V", 
                               "Y", "S_units"))

# Expanding takes about 35 sec on my Mac Studio
# Expanding takes about 37 sec on my Macbook Pro
expanded_usa <- psut_usa |> 
  matsindf::expand_to_tidy(drop = 0)
# Writing the data into the database takes 2 sec locally
# Writing the data into the database takes 6 sec on the VPN from home 
dbWriteTable(conn, name = "expanded_usa", value = expanded_usa, overwrite = TRUE)

# Reading the data out of the database takes less than 1 second locally
# Reading the data out of the database takes 2.8 sec on the VPN from home
expanded_usa_2 <- dbReadTable(conn, name = "expanded_usa")
# Re-creating the matrix structure takes 36 sec on my Mac Studio
# Re-creating the matrix structure takes 41 sec on my Mac Book Pro
psut_usa_2 <- expanded_usa_2 |> 
  dplyr::group_by(Country, Method, Energy.type, Last.stage, Year, IEAMW, matnames) |> 
  matsindf::collapse_to_matrices(matrix_class = "Matrix")

# Try nesting
psut_usa_2 <- expanded_usa_2 |> 
  tidyr::nest(rcv = c(rownames, colnames, matvals, rowtypes, coltypes))


dbListTables(conn)


dbRemoveTable(conn, "expanded_usa")


dbDisconnect(conn)


