

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
                                  .nested_data_colname = ".nested_data_col", 
                                  .nested_data_hash_colname = ".nested_data_hash_col", 
                                  .algo = "md5") {
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(), 
                         dbname = conn_args$dbname, 
                         host = conn_args$host,
                         port = conn_args$port, 
                         user = conn_args$user)
  on.exit(DBI::dbDisconnect(conn))

  if (!(table_name %in% DBI::dbListTables(conn))) {
    # Maybe also check that key_cols are in table_name.
    # If not, throw a scary warning and maybe tell the user how to start over.
    # Maybe write a function that both destroys the targets cache and deletes all tables in the DB.
    DBI::dbWriteTable(conn, name = table_name, value = x)
  } else {
    # DBI::dbWriteTable(conn = conn, name = table_name, value = x, overwrite = TRUE)
    # Eventually, do a dplyr::rows_upsert() here
    dplyr::tbl(conn, table_name) |>
      dplyr::rows_upsert(x, by = key_cols) |> 
      dplyr::collect()
  }
print(x)
  # Create a hash of the nested data frame
  nested_df <-  x |> 
    dplyr::group_by(!!as.name(key_cols), tar_group_colname) |> 
    tidyr::nest(.key = .nested_data_colname)
print(nested_df)
  nested_df |> 
    dplyr::mutate(
      "{.table_colname}" :=  table_name, 
      "{.nested_data_hash_colname}" := digest::digest(.data[[.nested_data_colname]], algo = .algo), 
      "{.nested_data_colname}" := NULL
    )
}


load_table_from_hash <- function(a_hash, conn_args, 
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
    dplyr::group_by(dplyr::across(dplyr::all_of(key_cols))) |> 
    targets::tar_group() |> 
    store_and_return_hash(conn_args = conn_args, table_name = "df", key_cols = key_cols)
}


process <- function(DF, conn_args, key_cols) {
  DF |> 
    load_table_from_hash(conn_args, key_cols) |> 
    dplyr::mutate(
      valplus1 = val + 1
    )
}
