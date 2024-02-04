conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
df <- tibble::tribble(~Country, ~val, 
                      "A", 1) 
DBI::dbWriteTable(conn, "df", df, primary_key = "Country")
DBI::dbReadTable(conn, "df")
df2 <- tibble::tribble(~Country, ~val, 
                       "A", 11, 
                       "B", 2)
df_table <- dplyr::tbl(conn, "df")
# df_table |> 
#   dplyr::rows_upsert(df2, by = "Country", copy = TRUE, in_place = TRUE)
# Use dbExecute with parameterized SQL for upsert
for (i in seq_len(nrow(df2))) {
  existing <- DBI::dbGetQuery(conn, sprintf("SELECT * FROM df WHERE Country = '%s';", df2$Country[i]))
  
  if (nrow(existing) == 0) {
    # Row doesn't exist, insert
    DBI::dbExecute(conn, sprintf("INSERT INTO df (Country, val) VALUES ('%s', %s);", 
                                 df2$Country[i], df2$val[i]))
  } else {
    # Row exists, update
    DBI::dbExecute(conn, sprintf("UPDATE df SET val = %s WHERE Country = '%s';", 
                                 df2$val[i], df2$Country[i]))
  }
}
DBI::dbReadTable(conn, "df")

DBI::dbDisconnect(conn)
