conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "testing", 
                  # host = "153.106.113.125",
                  host = "localhost",
                  port = 5432, 
                  user = "postgres")


dbDisconnect(conn)
