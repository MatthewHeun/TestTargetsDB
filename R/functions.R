

#' Store a grouped data frame in a database for use in a targets pipeline
#' 
#' The grouping variables of `x` are retained.
#'
#' @param x 
#' @param conn_args 
#' @param table_name 
#' @param key_cols
#' @param key_cols
#' @param .table_colname 
#' @param .nested_data_col 
#' @param .nested_data_hash_col 
#' @param .algo 
#'
#' @return
#' @export
#'
#' @examples
store_and_return_hash <- function(x, conn_args, table_name, key_cols,
                                  tar_group_colname = "tar_group",
                                  .table_colname = ".table_name",
                                  .nested_data_hash_colname = ".nested_data_hash_col", 
                                  .algo = "md5") {
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(), 
                         dbname = conn_args$dbname, 
                         host = conn_args$host,
                         port = conn_args$port, 
                         user = conn_args$user)
  on.exit(DBI::dbDisconnect(conn))

  if (!(table_name %in% DBI::dbListTables(conn))) {
    # Maybe write a function that both destroys the targets cache and deletes all tables in the DB.
    DBI::dbWriteTable(conn, name = table_name, value = x)
  } else {
    # Check that key_cols are in table_name.
    # If not, throw a scary warning and maybe tell the user how to start over.
    the_table <- dplyr::tbl(conn, table_name)
    cnames <- the_table |> 
      colnames()
    if (!(all(key_cols %in% cnames))) {
      stop(paste("key_cols", paste(key_cols, collapse = ", "), "are not in the table named", table_name))
    }
    the_table |>
      dplyr::rows_upsert(x, by = dplyr::all_of(c(key_cols, tar_group_colname)), copy = TRUE) |>
      dplyr::compute()
  }
  
  # Create and return a hash of the nested data frame
  x |> 
    dplyr::group_by(!!as.name(tar_group_colname)) |>
    # dplyr::group_by(!!as.name(key_cols), !!as.name(tar_group_colname)) |> 
    tidyr::nest(.key = .nested_data_hash_colname) |> 
    dplyr::mutate(
      "{.table_colname}" :=  table_name, 
      "{.nested_data_hash_colname}" := digest::digest(.data[[.nested_data_hash_colname]], algo = .algo)
    ) |> 
    # Relocate .table_colname to be the the left (first) column in the table.
    dplyr::relocate(dplyr::any_of(.table_colname))
    # dplyr::relocate(!!as.name(.table_colname))
}


load_table_from_hash <- function(a_hash, conn_args, key_cols,
                                 tar_group_colname = "tar_group",
                                 .table_colname = ".table_name",
                                 .nested_data_colname = ".nested_data_col", 
                                 .nested_data_hash_colname = ".nested_data_hash_col") {
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(), 
                         dbname = conn_args$dbname, 
                         host = conn_args$host,
                         port = conn_args$port, 
                         user = conn_args$user)
  on.exit(DBI::dbDisconnect(conn))
print(a_hash)
  table_name <- a_hash[[.table_colname]] |> 
    unique()
  assertthat::assert_that(length(table_name) == 1)

  this_tar_group <- a_hash[[tar_group_colname]] |> 
    unique()
  assertthat::assert_that(length(this_tar_group) == 1)
  # Here, we should read only the rows we need to read.
  out <- dplyr::tbl(conn, table_name) |> 
    dplyr::filter(tar_group == this_tar_group) |> 
    dplyr::collect()
}



make_df <- function(conn_args, key_cols) {
  tibble::tribble(~Country, ~Last.stage, ~val, 
                  "A", "Final", 1, 
                  "A", "Useful", 2, 
                  "B", "Final", 3, 
                  "B", "Useful", 4, 
                  "C", "Final", 5) |> 
    dplyr::group_by(Country) |>
    targets::tar_group() |> 
    store_and_return_hash(conn_args = conn_args, table_name = "df", key_cols = key_cols)
}


process <- function(DF, conn_args, key_cols) {
  DF |> 
    load_table_from_hash(conn_args = conn_args, key_cols = key_cols) |> 
    dplyr::mutate(
      valplus1 = val + 1
    ) |> 
    store_and_return_hash(conn_args = conn_args, table_name = "Processed", key_cols = key_cols)
}
