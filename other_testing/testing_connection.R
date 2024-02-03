library(DBI)



conn <- connections::connection_open(drv = RPostgres::Postgres(), 
                                     dbname = "playground", 
                                     host = "eviz.cs.calvin.edu",
                                     port = 5432, 
                                     user = "mkh2")


# conn <- dbConnect(drv = RPostgres::Postgres(), 
#                   dbname = "playground", 
#                   host = "eviz.cs.calvin.edu",
#                   port = 5432, 
#                   user = "mkh2")

dbListTables(conn)

my_table <- tibble::tribble(~Country, ~Last.stage, ~val, 
                            "A", "Final", 1, 
                            "A", "Useful", 2, 
                            "B", "Final", 3, 
                            "B", "Useful", 4, 
                            "C", "Final", 5) |> 
  dplyr::group_by(dplyr::across(dplyr::all_of(c("Country", "Last.stage")))) |> 
  targets::tar_group()
  

dbWriteTable(conn, name = "df", my_table)
dbReadTable(conn, "df")
dbRemoveTable(conn, "df")

dbDisconnect(conn)






conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "playground", 
                  host = "153.106.113.125",
                  # host = "localhost",
                  port = 5432, 
                  user = "mkh2")

dbListTables(conn)
dbReadTable(conn, "df")
dbRemoveTable(conn, "df")

dbDisconnect(conn)


conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "postgres", 
                  host = "eviz.cs.calvin.edu",
                  port = 5432, 
                  user = "postgres")
dbDisconnect(conn)




conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "playground", 
                  host = "eviz.cs.calvin.edu",
                  port = 5432, 
                  user = "postgres")

dbListTables(conn)
dbReadTable(conn, "df")
dbRemoveTable(conn, "df")

dbDisconnect(conn)
