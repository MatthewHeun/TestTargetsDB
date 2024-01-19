library(DBI)

conn <- dbConnect(drv = RPostgres::Postgres(), 
                  dbname = "playground", 
                  host = "153.106.113.125", 
                  port = 5432, 
                  user = "mkh2")


# psut <- PFUPipelineTools::read_pin_version("psut", "v1.3b1") |> 
#   dplyr::filter(Country == "USA")  # 17.6 secs


psut <- PFUPipelineTools::read_pin_version("psut", "v1.3b1") |> 
  # Put in a form that we would use for the DB
  tidyr::pivot_longer(cols = c("R", "U", "U_feed", "U_EIOU",
                               "r_EIOU", "V", "Y", "S_units"), 
                      names_to = "matname",
                      values_to = "matvals")
psut_usa <- psut |> 
  dplyr::filter(Country == "USA")

# Convert all matrices in psut_usa to NOT have dimnames
psut_nodns <- psut |> 
  dplyr::mutate(
    matvals = purrr::map(matvals, ~ {
      .x <- .x  # Ensure .x is not modified in place
      rownames(.x) <- NULL
      colnames(.x) <- NULL
      return(.x)
    }))
psut_usa_nodns <- psut_usa |> 
  dplyr::mutate(
    matvals = purrr::map(matvals, ~ {
      .x <- .x  # Ensure .x is not modified in place
      rownames(.x) <- NULL
      colnames(.x) <- NULL
      return(.x)
    }))

# Check some sizes.  Memory footprint is reduced by 50%
object.size(psut) |> format(units = "MB") # 5075 MB
object.size(psut_nodns) |> format(units = "MB") # 2517 MB
object.size(psut_usa) |> format(units = "MB") # 67.2 MB
object.size(psut_usa_nodns) |> format(units = "MB") # 32.3 MB

# Check disk footprint. 27% savings in disk space, 42% savings in write speed.
temp_dir <- tempdir()
psut_path <- file.path(temp_dir, "psut.rds")
psut_nodns_path <- file.path(temp_dir, "psut_nodns.rds")
bench::mark(save_psut = saveRDS(psut, psut_path)) # 29 sec
bench::mark(save_psut_nodns = saveRDS(psut_nodns, psut_nodns_path)) # 16.8 s  42% savings
file.info(psut_path)$size / 1e6 # 219 MB
file.info(psut_nodns_path)$size / 1e6 # 160 MB  27% savings
file.remove(psut_path)
file.remove(psut_nodns_path)

# Compare database write/read times
dbListTables(conn)
# dbExecute(conn, "CREATE DATABASE playground")

# Check data model for MADlib
# Expand and nest psut_usa_nodns to MADlib format
psut_usa_nodns_expanded <- psut_usa_nodns |> 
  dplyr::mutate(
    matvals = lapply(FUN = Matrix::mat2triplet, matvals), 
    matvals = lapply(FUN = tibble::as_tibble, matvals)
  ) |> 
  tidyr::unnest(cols = matvals)

# write the table to the playground
dbWriteTable(conn, "psut_usa", psut_usa_nodns_expanded)



# run timings on all of the above

# Read the table back out of the DB
bench::mark(psut_usa_read <- dbReadTable(conn, "psut_usa"))  # 1.14 secs, including network travel time

# Read table back out of the playground and recreate matrices  2.13 sec
# Compared to reading the entire psut data frame from disk 
# and filtering to USA (17.6 secs), this is an 8.3x speedup.
bench::mark(psut_usa_read <- dbReadTable(conn, "psut_usa") |> 
  dplyr::group_by(Country, Method, Energy.type, Last.stage, Year, IEAMW, matname) |> 
  dplyr::reframe(
    matvals = list(Matrix::sparseMatrix(i = i, j = j, x = x))
  ))

dbRemoveTable(conn, "psut_usa")

# Unnest and collapse
# run timings on all of the above


# Compare timings between 
#   - reading entire psut data frame from disk
#   - reading only USA from the playground

dbDisconnect(conn)
