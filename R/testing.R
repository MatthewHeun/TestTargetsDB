# Examples from https://www.datacareer.de/blog/connect-to-postgresql-with-r-a-step-by-step-example/

library(DBI)

conn <- dbConnect(RPostgres::Postgres(), 
                  dbname = "postgres", 
                  host = "localhost", 
                  port = 5432, 
                  user = "postgres")
dbListTables(conn) 
dbDisconnect(conn)



conn <- dbConnect(RPostgres::Postgres(), 
                  dbname = "mkh2", 
                  host = "localhost", 
                  port = 5432, 
                  user = "mkh2")
dbListTables(conn) 
dbDisconnect(conn)




conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "postgres", 
                  host = "153.106.113.125", 
                  port = 5432, 
                  user = "postgres")
dbListTables(conn) 
dbDisconnect(conn)

conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "mkh2", 
                  host = "153.106.113.125", 
                  port = 5432, 
                  user = "mkh2")
dbListTables(conn) 
dbDisconnect(conn)


