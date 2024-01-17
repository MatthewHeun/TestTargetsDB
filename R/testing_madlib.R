library(DBI)

conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "postgres", 
                  host = "localhost",
                  # host = "localhost",
                  port = 5432, 
                  user = "postgres")


dbDisconnect(conn)
