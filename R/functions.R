make_data <- function(db_path, n) {
  to_store <- tibble(x = rnorm(n), y = rnorm(n))
  
  store_and_return_hash(to_store, db_path = db_path, table_name = "data")
}

make_model <- function(data_hash, db_path) {
  the_table <- load_table_from_hash(a_hash = data_hash, db_path = db_path)
  coefficients(lm(y ~ x, data = the_table))
}

store_and_return_hash <- function(x, db_path, table_name) {
  conn <- dbConnect(doltr::dolt_local(), dir = db_path)
  on.exit(doltr::dbDisconnect(conn))
  DBI::dbWriteTable(conn = conn, name = table_name, value = x, overwrite = TRUE)
  paste0(table_name, "-", rlang::hash(x))
}

load_table_from_hash <- function(a_hash, db_path, split = "-") {
  print(a_hash)
  table_name <- strsplit(a_hash, split = split)[[1]][[1]]
  print(table_name)
  conn <- dbConnect(doltr::dolt_local(), dir = db_path)
  on.exit(doltr::dbDisconnect(conn))
  DBI::dbReadTable(conn = conn, name = table_name)
}
