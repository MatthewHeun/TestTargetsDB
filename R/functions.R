

#' Store a grouped data frame in a database for use in a targets pipeline
#' 
#' The grouping variables of `x` are retained.
#'
#' @param x 
#' @param conn_args 
#' @param table_name 
#' @param hash_colname 
#' @param algo 
#' @param .ungrouped_cols 
#'
#' @return
#' @export
#'
#' @examples
store_and_return_hash <- function(x, conn_args, table_name, 
                                  algo = "md5", 
                                  .hash_colname = "group_hash",
                                  .table_colname = ".table_name",
                                  .ungrouped_cols = ".ungrouped_cols") {
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(), 
                         dbname = "playground", 
                         host = "153.106.113.125",
                         port = 5432, 
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))
  
  DBI::dbWriteTable(conn = conn, name = table_name, value = x, overwrite = TRUE)
  # Save, table, keys, and hash
  # Create a hash of the nested data frame
  nested_df <-  x |> 
    tidyr::nest(.key = .ungrouped_cols)
  nested_df |> 
    dplyr::mutate(
      "{.table_colname}" :=  table_name, 
      "{.hash_colname}" := digest::digest(.data[[.ungrouped_cols]], algo = algo), 
      "{.ungrouped_cols}" := NULL
    )
}


load_table_from_hash <- function(a_hash, conn_args, 
                                 .hash_colname = "group_hash",
                                 .table_colname = ".table_name",
                                 .ungrouped_cols = ".ungrouped_cols") {
  conn <- dbConnect(drv = RPostgres::Postgres(), 
                    dbname = "playground", 
                    host = "153.106.113.125",
                    port = 5432, 
                    user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  table_name <- a_hash[[.table_colname]] |> 
    unique()
  assertthat::assert_that(length(table_name) == 1)
  grp_vars <- a_hash |> 
    colnames() |> 
    setdiff(c(.hash_colname, .table_colname, .ungrouped_cols, "tar_group"))
  out <- DBI::dbReadTable(conn = conn, name = table_name)
print(out)  
  
  out |> 
    dplyr::group_by(.data[[grp_vars]])
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
    dplyr::group_by(Country) |> 
    targets::tar_group() |> 
    # tar_group removes R's groups!
    dplyr::group_by(Country, tar_group) |> 
    store_and_return_hash(conn_args = conn_args, table_name = "df")
}


process <- function(DF, conn_args) {
  DF |> 
    load_table_from_hash(conn_args) |> 
    dplyr::mutate(
      valplus1 = val + 1
    ) |> 
    dplyr::summarise(val = sum(val), valplus1 = sum(valplus1))
}
