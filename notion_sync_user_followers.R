#!/usr/bin/env Rscript

# ─────────────────────────────────────────────────────────────────────────────
# Supabase (Postgres) → Notion (paged, lazy preindex, SQL de-dupe, resumable)
# Source table: user_followers
# Unique Notion Title: "{user_id} • {snapshot_time in UTC ISO}"
# Optional full overwrite: set OVERWRITE_NOTION=true (archives all pages first)
# ─────────────────────────────────────────────────────────────────────────────

# --- packages ----------------------------------------------------------------
need <- c("httr2","jsonlite","DBI","RPostgres")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

# --- config ------------------------------------------------------------------
NOTION_TOKEN       <- Sys.getenv("NOTION_TOKEN")
DB_ID              <- Sys.getenv("NOTION_DATABASE_ID")
LOOKBACK_HOURS     <- as.integer(Sys.getenv("LOOKBACK_HOURS", "17532")) # ~2 years
NOTION_VERSION     <- "2022-06-28"

NUM_NA_AS_ZERO     <- tolower(Sys.getenv("NUM_NA_AS_ZERO","false")) %in% c("1","true","yes")
INSPECT_FIRST_ROW  <- tolower(Sys.getenv("INSPECT_FIRST_ROW","false")) %in% c("1","true","yes")
DUMP_SCHEMA        <- tolower(Sys.getenv("DUMP_SCHEMA","false"))       %in% c("1","true","yes")
RATE_DELAY_SEC     <- as.numeric(Sys.getenv("RATE_DELAY_SEC","0.20"))
RUN_SMOKE_TEST     <- tolower(Sys.getenv("RUN_SMOKE_TEST","false"))    %in% c("1","true","yes")
OVERWRITE_NOTION   <- tolower(Sys.getenv("OVERWRITE_NOTION","false"))  %in% c("1","true","yes")

CHUNK_SIZE         <- as.integer(Sys.getenv("CHUNK_SIZE","800"))
CHUNK_OFFSET       <- as.integer(Sys.getenv("CHUNK_OFFSET","0"))
USERNAME_FILTER    <- Sys.getenv("USERNAME_FILTER","")                 # optional filter
ORDER_DIR          <- toupper(Sys.getenv("ORDER_DIR","ASC"))           # time-series = ASC by default
if (!(ORDER_DIR %in% c("ASC","DESC"))) ORDER_DIR <- "ASC"

# Resumability budgets
MAX_ROWS_PER_RUN   <- as.integer(Sys.getenv("MAX_ROWS_PER_RUN","1200"))
MAX_MINUTES        <- as.numeric(Sys.getenv("MAX_MINUTES","110"))
t0 <- Sys.time()

if (!nzchar(NOTION_TOKEN) || !nzchar(DB_ID)) stop("Set NOTION_TOKEN and NOTION_DATABASE_ID.")

# --- helpers -----------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x

rtxt <- function(x) {
  s <- as.character(x %||% "")
  if (identical(s, "")) list()
  else list(list(type="text", text=list(content=substr(s, 1, 1800))))
}

parse_dt <- function(x) {
  if (inherits(x, "POSIXt")) return(as.POSIXct(x, tz="UTC"))
  if (inherits(x, "Date"))   return(as.POSIXct(x, tz="UTC"))
  if (inherits(x, "integer64")) x <- as.character(x)
  if (is.numeric(x))         return(as.POSIXct(x, origin="1970-01-01", tz="UTC"))
  if (!is.character(x))      return(as.POSIXct(NA, origin="1970-01-01", tz="UTC"))
  xx <- trimws(x)
  fmts <- c("%Y-%m-%dT%H:%M:%OSZ","%Y-%m-%d %H:%M:%S%z","%Y-%m-%d %H:%M:%S %z",
            "%Y-%m-%d %H:%M:%S","%Y-%m-%d")
  for (f in fmts) {
    d <- suppressWarnings(as.POSIXct(xx, format=f, tz="UTC"))
    if (!is.na(d)) return(d)
  }
  as.POSIXct(NA, origin="1970-01-01", tz="UTC")
}

perform <- function(req, tag = "", max_tries = 6, base_sleep = 0.5) {
  last <- NULL
  for (i in seq_len(max_tries)) {
    resp <- tryCatch(req_perform(req), error = function(e) e)
    last <- resp
    if (inherits(resp, "httr2_response")) {
      sc <- resp$status_code
      if (sc != 429 && sc < 500) return(resp)
      ra <- resp_headers(resp)[["retry-after"]]
      wait <- if (!is.null(ra)) suppressWarnings(as.numeric(ra)) else base_sleep * 2^(i - 1)
      Sys.sleep(min(wait, 10))
    } else {
      Sys.sleep(base_sleep * 2^(i - 1))
    }
  }
  err <- list(
    tag = tag,
    status = if (inherits(last, "httr2_response")) last$status_code else NA_integer_,
    body = if (inherits(last, "httr2_response")) tryCatch(resp_body_string(last), error = function(...) "<no body>")
           else paste("R error:", conditionMessage(last))
  )
  structure(list(.err = TRUE, err = err), class = "notion_err")
}
is_err   <- function(x) inherits(x, "notion_err") && isTRUE(x$.err %||% TRUE)
show_err <- function(x, row_i = NA, key = NA) {
  if (!is_err(x)) return(invisible())
  er <- x$err
  cat(sprintf("⚠️ Notion error%s%s [%s] Status: %s\nBody: %s\n",
              if (!is.na(row_i)) paste0(" on row ", row_i) else "",
              if (!is.na(key)) paste0(" (key=", key, ")") else "",
              er$tag %||% "request",
              as.character(er$status %||% "n/a"),
              er$body %||% "<empty>"))
}

notion_req <- function(url) {
  request(url) |>
    req_headers(
      Authorization    = paste("Bearer", NOTION_TOKEN),
      "Notion-Version" = NOTION_VERSION,
      "Content-Type"   = "application/json"
    )
}

# --- Notion schema -----------------------------------------------------------
get_db_schema <- function() {
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID)) |> perform(tag="GET /databases")
  if (is_err(resp)) { show_err(resp); stop("Could not read Notion database schema.") }
  resp_body_json(resp, simplifyVector = FALSE)
}
.DB <- get_db_schema()
PROPS <- .DB$properties
TITLE_PROP <- names(Filter(function(p) identical(p$type, "title"), PROPS))[1]
if (is.null(TITLE_PROP)) stop("This Notion database has no Title (Name) property.")
if (DUMP_SCHEMA) {
  cat("\n--- Notion schema ---\n")
  cat(paste(
    vapply(names(PROPS), function(n) sprintf("%s : %s", n, PROPS[[n]]$type), character(1)),
    collapse = "\n"
  ), "\n----------------------\n")
}

to_num <- function(x) {
  if (inherits(x, "integer64")) x <- as.character(x)
  v <- suppressWarnings(as.numeric(x))
  if (is.na(v) && NUM_NA_AS_ZERO) v <- 0
  v
}

set_prop <- function(name, value) {
  p <- PROPS[[name]]; if (is.null(p)) return(NULL)
  tp <- p$type
  if (tp == "title") {
    list(title = list(list(type="text", text=list(content=as.character(value %||% "untitled")))))
  } else if (tp == "rich_text") {
    list(rich_text = rtxt(value))
  } else if (tp == "number") {
    v <- to_num(value); if (!is.finite(v)) return(NULL)
    list(number = v)
  } else if (tp == "date") {
    d <- parse_dt(value); if (is.na(d)) return(NULL)
    list(date = list(start = format(d, "%Y-%m-%dT%H:%M:%SZ")))
  } else NULL
}

# --- Overwrite helpers (archive all pages) -----------------------------------
archive_page <- function(page_id) {
  resp <- notion_req(paste0("https://api.notion.com/v1/pages/", page_id)) |>
    req_method("PATCH") |>
    req_body_json(list(archived = TRUE), auto_unbox = TRUE) |>
    perform(tag = "ARCHIVE /pages/:id")
  !is_err(resp)
}

archive_all_pages_in_db <- function(db_id, delay = RATE_DELAY_SEC) {
  message("🧹 Overwrite requested — archiving all existing pages in Notion DB before sync …")
  has_more <- TRUE
  start_cursor <- NULL
  total_archived <- 0L

  while (has_more) {
    body <- list(page_size = 100L)
    if (!is.null(start_cursor)) body$start_cursor <- start_cursor

    resp <- notion_req(paste0("https://api.notion.com/v1/databases/", db_id, "/query")) |>
      req_body_json(body, auto_unbox = TRUE) |>
      perform(tag="POST /databases/query (overwrite pass)")
    if (is_err(resp)) { show_err(resp); break }

    out <- resp_body_json(resp, simplifyVector = FALSE)
    res <- out$results %||% list()
    if (!length(res)) { has_more <- FALSE; break }

    for (pg in res) {
      pid <- pg$id
      ok <- archive_page(pid)
      if (ok) total_archived <- total_archived + 1L
      Sys.sleep(delay)
    }

    has_more <- isTRUE(out$has_more %||% FALSE)
    start_cursor <- out$next_cursor %||% NULL
  }

  message(sprintf("🧹 Archived %d pages.", total_archived))
}

# Build Title and properties from a DB row
title_from_row <- function(r) {
  # Title = "{user_id} • {snapshot_time UTC ISO}"
  sid <- as.character(r$user_id %||% "")
  ts  <- parse_dt(r$snapshot_time)
  if (is.na(ts)) ts <- as.POSIXct("1970-01-01 00:00:00", tz="UTC")
  paste0(sid, " • ", format(ts, "%Y-%m-%dT%H:%M:%SZ"))
}

props_from_row <- function(r) {
  pr <- list()
  ttl <- title_from_row(r)
  pr[[TITLE_PROP]] <- set_prop(TITLE_PROP, ttl)

  wanted <- c("user_id","username","followers_count","snapshot_time")
  for (nm in wanted) {
    if (!is.null(PROPS[[nm]]) && !is.null(r[[nm]])) pr[[nm]] <- set_prop(nm, r[[nm]])
  }
  pr
}

find_page_by_title_eq <- function(val) {
  body <- list(filter = list(property = TITLE_PROP, title = list(equals = as.character(val %||% ""))),
               page_size = 1)
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag="POST /databases/query")
  if (is_err(resp)) return(structure(NA_character_, class="notion_err", .err=TRUE, err=resp$err))
  out <- resp_body_json(resp, simplifyVector = TRUE)
  if (length(out$results)) out$results$id[1] else NA_character_
}

# --- LAZY index for this chunk (by Title) ------------------------------------
build_index_for_rows <- function(rows) {
  by_ttl <- new.env(parent=emptyenv())
  titles <- vapply(seq_len(nrow(rows)), function(i) title_from_row(rows[i, , drop=FALSE]), character(1L))
  titles <- unique(titles[nzchar(titles)])

  i <- 1L
  while (i <= length(titles)) {
    slice <- titles[i:min(i+49, length(titles))]
    ors <- lapply(slice, function(tl) list(property = TITLE_PROP, title = list(equals = tl)))
    body <- list(filter = list(or = ors), page_size = 100)
    resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
      req_body_json(body, auto_unbox = TRUE) |>
      perform(tag="POST /databases/query (lazy)")
    if (!is_err(resp)) {
      out <- resp_body_json(resp, simplifyVector = FALSE)
      for (pg in out$results) {
        pid <- pg$id
        tnodes <- pg$properties[[TITLE_PROP]]$title
        ttl <- if (length(tnodes)) paste0(vapply(tnodes, \(x) x$plain_text %||% "", character(1L)), collapse = "") else ""
        if (nzchar(ttl)) by_ttl[[ttl]] <- pid
      }
    } else show_err(resp)
    i <- i + 50L
    Sys.sleep(RATE_DELAY_SEC/2)
  }

  list(by_title = by_ttl)
}

# --- CRUD --------------------------------------------------------------------
create_page <- function(pr) {
  body <- list(parent = list(database_id = DB_ID), properties = pr)
  resp <- notion_req("https://api.notion.com/v1/pages") |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag="POST /pages")
  if (is_err(resp)) return(structure(NA_character_, class="notion_err", .err=TRUE, err=resp$err))
  resp_body_json(resp, simplifyVector = TRUE)$id
}
update_page <- function(page_id, pr) {
  resp <- notion_req(paste0("https://api.notion.com/v1/pages/", page_id)) |>
    req_method("PATCH") |>
    req_body_json(list(properties = pr), auto_unbox = TRUE) |>
    perform(tag="PATCH /pages/:id")
  if (is_err(resp)) return(structure(FALSE, class="notion_err", .err=TRUE, err=resp$err))
  TRUE
}

# --- Upsert by Title ---------------------------------------------------------
upsert_row <- function(r, idx = NULL) {
  title_val <- title_from_row(r)
  pr_full   <- props_from_row(r)

  pid <- NA_character_
  if (!is.null(idx)) {
    pid <- idx$by_title[[title_val]]; if (is.null(pid)) pid <- NA_character_
  } else {
    pid <- find_page_by_title_eq(title_val)
  }

  if (!is.na(pid[1])) {
    ok <- update_page(pid, pr_full)
    return(is.logical(ok) && ok)
  }

  pid2 <- create_page(pr_full)
  if (!is.na(pid2[1])) return(TRUE)

  pr_min <- list(); pr_min[[TITLE_PROP]] <- set_prop(TITLE_PROP, title_val)
  pid3 <- create_page(pr_min)
  if (is.na(pid3[1])) return(FALSE)
  ok2 <- update_page(pid3, pr_full)
  is.logical(ok2) && ok2
}

# --- Supabase connection -----------------------------------------------------
supa_host <- Sys.getenv("SUPABASE_HOST")
supa_user <- Sys.getenv("SUPABASE_USER")
supa_pwd  <- Sys.getenv("SUPABASE_PWD")
if (!nzchar(supa_host) || !nzchar(supa_user) || !nzchar(supa_pwd)) stop("Set SUPABASE_HOST, SUPABASE_USER, SUPABASE_PWD.")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = supa_host,
  port = as.integer(Sys.getenv("SUPABASE_PORT", "5432")),
  dbname = as.character(Sys.getenv("SUPABASE_DB", "postgres")),
  user = supa_user,
  password = supa_pwd,
  sslmode = "require"
)

# Optional full overwrite (only on first chunk)
if (OVERWRITE_NOTION && CHUNK_OFFSET == 0) {
  archive_all_pages_in_db(DB_ID, delay = RATE_DELAY_SEC)
}

# Lookback window applies to snapshot_time
since <- as.POSIXct(Sys.time(), tz = "UTC") - LOOKBACK_HOURS * 3600
since_str <- format(since, "%Y-%m-%d %H:%M:%S%z")

base_where  <- paste0("snapshot_time >= TIMESTAMPTZ ", DBI::dbQuoteString(con, since_str))
user_clause <- if (nzchar(USERNAME_FILTER)) paste0(" AND username = ", DBI::dbQuoteString(con, USERNAME_FILTER)) else ""

# Expected distinct (user_id, snapshot_time)
exp_sql <- sprintf("
  SELECT COUNT(*) AS n FROM (
    SELECT DISTINCT user_id, snapshot_time
    FROM user_followers
    WHERE %s%s
  ) t
", base_where, user_clause)
expected_raw <- DBI::dbGetQuery(con, exp_sql)$n[1]
expected_num <- suppressWarnings(as.numeric(expected_raw))
if (!is.finite(expected_num)) expected_num <- 0
expected_i <- as.integer(round(expected_num))
message(sprintf("Expected distinct user_id/snapshot_time under this filter: %d", expected_i))

# Optional smoke test
if (RUN_SMOKE_TEST) {
  smoke_title <- paste0("ping ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
  smoke_pid <- create_page(setNames(list(set_prop(TITLE_PROP, smoke_title)), TITLE_PROP))
  if (is_err(smoke_pid) || is.na(smoke_pid[1])) {
    cat("\n🔥 Smoke test FAILED — cannot create even a minimal page in this database.\n",
        "Confirm the integration has access to THIS database.\n", sep = "")
    if (is_err(smoke_pid)) show_err(smoke_pid)
    DBI::dbDisconnect(con); quit(status = 1L, save = "no")
  } else {
    notion_req(paste0("https://api.notion.com/v1/pages/", smoke_pid)) |>
      req_method("PATCH") |>
      req_body_json(list(archived = TRUE), auto_unbox = TRUE) |>
      perform(tag="ARCHIVE ping")
  }
}

# Optional one-row inspector
if (INSPECT_FIRST_ROW) {
  test_q <- sprintf("
    WITH ranked AS (
      SELECT f.*,
             ROW_NUMBER() OVER (
               PARTITION BY user_id, snapshot_time
               ORDER BY snapshot_time DESC
             ) AS rn
      FROM user_followers f
      WHERE %s%s
    )
    SELECT user_id, username, followers_count, snapshot_time
    FROM ranked
    WHERE rn = 1
    ORDER BY snapshot_time %s
    LIMIT 1 OFFSET %d
  ", base_where, user_clause, ORDER_DIR, CHUNK_OFFSET)
  one <- DBI::dbGetQuery(con, test_q)
  if (nrow(one)) {
    one$snapshot_time <- parse_dt(one$snapshot_time)
    # print(one)
  }
}

# --- Main loop: page through Supabase ----------------------------------------
offset <- CHUNK_OFFSET
total_success <- 0L
total_seen    <- 0L

repeat {
  qry <- sprintf("
    WITH ranked AS (
      SELECT f.*,
             ROW_NUMBER() OVER (
               PARTITION BY user_id, snapshot_time
               ORDER BY snapshot_time DESC
             ) AS rn
      FROM user_followers f
      WHERE %s%s
    )
    SELECT user_id, username, followers_count, snapshot_time
    FROM ranked
    WHERE rn = 1
    ORDER BY snapshot_time %s
    LIMIT %d OFFSET %d
  ", base_where, user_clause, ORDER_DIR, CHUNK_SIZE, offset)

  rows <- DBI::dbGetQuery(con, qry)
  n <- nrow(rows)
  if (!n) break

  message(sprintf("Fetched %d rows (offset=%d). USERNAME_FILTER=%s CHUNK_SIZE=%d",
                  n, offset, ifelse(nzchar(USERNAME_FILTER), USERNAME_FILTER, "<none>"), CHUNK_SIZE))

  idx <- build_index_for_rows(rows)

  success <- 0L
  for (i in seq_len(n)) {
    r <- rows[i, , drop = FALSE]
    if (!is.null(r$snapshot_time)) r$snapshot_time <- parse_dt(r$snapshot_time)
    key <- title_from_row(r)
    message(sprintf("Upserting %s", key))
    ok <- upsert_row(r, idx = idx)
    if (ok) success <- success + 1L else message(sprintf("Row %d failed (key=%s)", i, key))
    if (i %% 50 == 0) message(sprintf("Processed %d/%d in this page (ok %d)", i, n, success))
    Sys.sleep(RATE_DELAY_SEC)
  }

  total_success <- total_success + success
  total_seen    <- total_seen + n
  offset        <- offset + n

  message(sprintf("Page done. %d/%d upserts ok (cumulative ok %d, seen %d of ~%d).",
                  success, n, total_success, total_seen, expected_i))

  elapsed_min <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
  if (total_seen >= MAX_ROWS_PER_RUN || elapsed_min >= MAX_MINUTES) {
    message(sprintf("Stopping early (seen=%d, elapsed=%.1f min). Next offset = %d",
                    total_seen, elapsed_min, offset))
    go <- Sys.getenv("GITHUB_OUTPUT")
    if (nzchar(go)) write(paste0("next_offset=", offset), file = go, append = TRUE)
    DBI::dbDisconnect(con)
    quit(status = 0, save = "no")
  }
}

DBI::dbDisconnect(con)
message(sprintf("All pages done. Upserts ok: %d. Expected distinct under filter: %d", total_success, expected_i))

# Tell the workflow we’re finished (for auto-continue step)
go <- Sys.getenv("GITHUB_OUTPUT")
if (nzchar(go)) write("next_offset=done", file = go, append = TRUE)
