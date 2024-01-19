library(DBI)

conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "playground", 
                  host = "153.106.113.125",
                  # host = "localhost",
                  port = 5432, 
                  user = "mkh2")

dbListTables(conn)
dbRemoveTable(conn, "data")

dbDisconnect(conn)


conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "postgres", 
                  host = "eviz.cs.calvin.edu",
                  port = 5432, 
                  user = "postgres")
dbDisconnect(conn)
