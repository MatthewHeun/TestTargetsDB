library(DBI)

conn <- dbConnect(RSQLite::SQLite(), ":memory:")
# dbExecute(conn, "CREATE TABLE df (
#    id INTEGER PRIMARY KEY AUTOINCREMENT,
#    Country TEXT,
#    'Last.stage' TEXT, 
#    val INTEGER)")


df <- tibble::tribble(~Country, ~Last.stage, ~val, 
                      "A", "Final", 1, 
                      "A", "Useful", 2)
dbWriteTable(conn, "df", df, append = TRUE)
dbReadTable(conn, "df")

the_table <- dplyr::tbl(conn, "df")
df2 <- tibble::tribble(~Country, ~Last.stage, ~val,
                       "A", "Final", 0)

the_table |> 
  dplyr::anti_join(df2, by = c("Country", "Last.stage"), copy = TRUE) |> 
  dplyr::rows_update(df2, unmatched = "ignore", copy = TRUE, in_place = FALSE) |> 
  dplyr::compute()
  
dbReadTable(conn, "df")


# df2 <- tibble::tribble(~Country, ~Last.stage, ~val, 
#                        "A", "Final", 0)
# 
# the_table <- dplyr::tbl(conn, "df")
# the_table |> 
#   dplyr::rows_update(df2, by = c("Country", "Last.stage"), 
#                      copy = TRUE, 
#                      in_place = TRUE, 
#                      unmatched = "ignore")
# dbReadTable(conn, "df")
# 
# df3 <- tibble::tribble(~Country, ~Last.stage, ~val,
#                        "A", "Final", 42,
#                        "B", "Final", 3,
#                        "B", "Useful", 4)
# the_table |> 
#   dplyr::rows_insert(df3, by = c("Country", "Last.stage"), 
#                      copy = TRUE, 
#                      in_place = TRUE, 
#                      conflict = "ignore")
# dbReadTable(conn, "df")


dbDisconnect(conn)
