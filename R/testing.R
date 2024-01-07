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



|> 
  tidyr::nest(matdfs = c(rownames, colnames, matvals, rowtypes, coltypes))

dbWriteTable(conn, name = "UKEnergy2000", UKEnergy2000)

dbReadTable(conn, name = "UKEnergy2000")

dbRemoveTable(conn, "UKEnergy2000")

dbDisconnect(conn)


