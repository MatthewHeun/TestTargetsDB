make_data <- function(db_path, n) {
  to_store <- tibble(x = rnorm(n), y = rnorm(n))
  
  store_and_return_hash(to_store, db_path = db_path, table_name = "data")
}


store_and_return_hash <- function(x, db_path, table_name) {
  
  conn <- dbConnect(doltr::dolt_local(), dir = db_path)
  on.exit(doltr::dbDisconnect(conn))
  DBI::dbWriteTable(conn = conn, name = table_name, value = x, overwrite = TRUE)
  rlang::hash(x)
}
