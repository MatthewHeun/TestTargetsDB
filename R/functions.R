store_and_return_hash <- function(x, conn_args, table_name) {
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(), 
                         dbname = "playground", 
                         host = "153.106.113.125",
                         port = 5432, 
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))
  
  # Create a hash of x
  DBI::dbWriteTable(conn = conn, name = table_name, value = x, overwrite = TRUE)
  print("wrote table")
  # Save, table, keys, and hash
  list(table = table_name, hash = rlang::hash(x))
}


load_table_from_hash <- function(a_hash, conn_args) {
  conn <- dbConnect(drv = RPostgres::Postgres(), 
                    dbname = "playground", 
                    host = "153.106.113.125",
                    port = 5432, 
                    user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))
  
  table_name <- a_hash$table
  # Get hash value from the DB
  # If unchanged, simply return a_hash
  # If changed, read the table
  DBI::dbReadTable(conn = conn, name = table_name)
}



make_data <- function(n, conn_args) {

  tibble(x = rnorm(n), y = rnorm(n)) |> 
    store_and_return_hash(conn_args, table_name = "data")
}


make_model <- function(data_hash, conn_args) {
  the_table <- load_table_from_hash(a_hash = data_hash, conn_args = conn_args)
  coefficients(lm(y ~ x, data = the_table))
}




make_df <- function(conn_args) {
  tibble::tribble(~Country, ~val, 
                  "A", 1, 
                  "A", 2, 
                  "B", 3, 
                  "B", 4, 
                  "C", 5) |>
    store_and_return_hash(conn_args = conn_args, table_name = "df")
}


process <- function(DF, conn_args, countries) {
  DF |> 
    load_table_from_hash(conn_args)
}
