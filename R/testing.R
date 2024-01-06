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

dbWriteTable(conn, name = "UKEnergy2000mats", Recca::UKEnergy2000mats)

dbRemoveTable(conn, "UKEnergy2000mats")

dbDisconnect(conn)


