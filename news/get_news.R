library(tidyverse)

boeWebConnectr::boe_web_config()

firm_df <- read_csv("../tweets/twitter_firms.csv", col_types = cols(
  Division = col_character(),
  Department = col_character(),
  Parent_Firm = col_character(),
  Firm_Aggregation = col_character(),
  Firm = col_character(),
  Term = col_character(),
  Handle = col_character(),
  Exclude = col_character(),
  CaseSensitiveTerm = col_character(),
  Note = col_character(),
  NewsTerm = col_character(),
  HeadlineTerm = col_character(),
  BodyTerm = col_character(),
  HeadlineTermFinancial = col_character(),
  Not = col_character(),
  UKFilter = col_logical(),
  Notes = col_character(),
  ID = col_double()
))


get_all_firms <- function() {
  for (i in seq_len(nrow(firm_df))) {
    name <- firm_df$Firm[[i]]
    print(name)
    dir_name <- paste0("data/", name)
    # FIX THIS BIT
    if (file.exists(paste0(dir_name, "/news.rds"))) {
      next
    }
    if (!dir.exists(dir_name)) {
      dir.create(dir_name)
    }
    query <- firm_df$HeadlineTerm[[i]]
    body_query <- firm_df$BodyTerm[[i]]
    
    if (is.na(query)) {
      next
    }
    
    if (str_detect(query, ";")) {
      query <- str_split(query, ";") %>% .[[1]] %>% str_trim() %>% paste0(collapse = " OR ")
    }
    
    if (is.na(body_query)) {
      body_query <- ""
    }
    df <- aylien(query, body_query, loop = T)
    write_rds(df, path = paste0(dir_name, "/news.rds"))
    log <- tibble(Firm = name, Query = query, Body_Query = body_query, Latest = max(df$published_at))
    write_csv(log, paste0(dir_name, "/log.csv"))
  }
}

all_rds <- function() {
  res <- NULL
  names <- dir("data")
  for (firm in names) {
    
    folder <- paste0("data/", firm, "/")
    if (!file.exists(paste0(folder, "news.rds"))) {
      next
    }
    print(firm)
    df <- readRDS(paste0(folder, "news.rds"))
    if (is_empty(df)) {
      next
    }
    res <- rbind(res, df)
  }
  res
}

process_firms <- function() {
  names <- dir("data")
  for (firm in names) {
    
    folder <- paste0("data/", firm, "/")
    if (!file.exists(paste0(folder, "news.rds"))) {
      next
    }
    print(firm)
    df <- readRDS(paste0(folder, "news.rds"))
    if (is_empty(df)) {
      next
    }
    csv <- news2csv(df) %>%
      select(id, published_at, title, source_name, clusters, title_sentiment_polarity, title_sentiment_score, body_sentiment_polarity, body_sentiment_score, link)
    write_tsv(csv, paste0(folder, "res.txt"))
  }
}

get_sources <- function() {
  names <- dir("data")
  res <- NULL
  for (firm in names) {
    
    folder <- paste0("data/", firm, "/")
    if (!file.exists(paste0(folder, "news.rds"))) {
      next
    }
    print(firm)
    df <- readRDS(paste0(folder, "news.rds"))
    if (is_empty(df)) {
      next
    }
    csv <- news2csv(df, filter = F) %>%
      select(source_name)
    res <- rbind(res, csv)
  }
  res
}

collate <- function() {
  firm_metadata <- firm_df %>% select(Firm, Parent_Firm, Department, Division) %>%
    mutate(Parent_Firm = case_when(is.na(Parent_Firm) ~ Firm,
                                   T ~ Parent_Firm))
  names <- dir("data")
  res <- NULL
  for (firm in names) {
    file <- paste0("data/", firm, "/res.txt")
    if (!file.exists(file)) {
      next
    }
    new_res <- read_tsv(file, col_types = cols(
      id = col_double(),
      published_at = col_datetime(format = ""),
      title = col_character(),
      source_name = col_character(),
      clusters = col_double(),
      title_sentiment_polarity = col_character(),
      title_sentiment_score = col_double(),
      body_sentiment_polarity = col_character(),
      body_sentiment_score = col_double(),
      link = col_character()
    )) %>%
      mutate(Firm = firm)
    res <- rbind(res, new_res)
  }
  res <- res %>% left_join(firm_metadata, by = "Firm")
  write_tsv(res, "all_news.txt", na = "")
}

try_several_times <- function(f, ..., times = 10, pause = 0, noisy = T) {
  
  function(...) {
    i <- 0
    while (i < times) {
      res <- tryCatch(f(...), error = function(e) e)
      if ("error" %in% class(res)) {
        i <- i + 1
        if (noisy) {
          print(paste0("Retrying: ", i))
        }
        Sys.sleep(pause)
      } else {
        return(res)
      }
    }
    stop("Failed all retries")
    
  }
}

aylien <- function(query, body_query = "", loop = F, cursor = "*") {
  base_url <- "https://api.aylien.com/news/stories?per_page=100&language=en&title="
  headers <- httr::add_headers(`X-AYLIEN-NewsAPI-Application-ID` = "c549b576",
                               `X-AYLIEN-NewsAPI-Application-Key` = "a8a53724eee2a904fd8c5f565feea1f4")
  full_query <- paste0(base_url, query)
  if (body_query != "") {
    full_query <- paste0(full_query, "&body=", body_query)
  }
  #full_query <- paste0(full_query, "&source.locations.country=GB")
  full_query <- full_query %>%
    paste0("&cursor=", cursor) %>%
    paste0("&published_at.start=2018-09-03T00:00:00Z")
  
  response <- try_several_times(httr::GET, times = 12, pause = 10)(utils::URLencode(full_query), headers)
  
  content <- httr::content(response)
  new_cursor <- content$next_page_cursor
  
  df <- ay2df(content$stories)
  if (nrow(df) == 0) {
    return(df)
  }
  print(tail(df$published_at, 1) %>% lubridate::ymd_hms())
  
  if (!loop || new_cursor == cursor) {
    return(df)
  } else {
    return(rbind(df, aylien(query, body_query, loop = T, cursor = new_cursor)))
  }
}

list2row <- function(l) {
  l %>% map_if(~length(.) != 1, function(x) list(x)) %>% as_tibble()
}

ay2df <- function(stories) {
  stories %>% map_df(list2row)
  
}

# id, published_at, source$name, title, body, 

over_rate_limit <- F

make_query <- function(str) {
  if (is.na(str)) {
    return(NA_character_)
  }
  strs <- str_split(str, ";", simplify = T) %>% str_trim()
  strs <- paste0('"', strs, '"')
  strs <- strs[!str_detect(strs, "^-")]
  res <- str_c(strs, collapse = " OR ")
  
  res <- str_replace(res, "\\\\", "")
  return(res)
}



get_api <- function(query) {
  # load remaining requests to check we're not over - return "Over rate limit" if so
  
  base_url <- "https://microsoft-azure-bing-news-search-v1.p.rapidapi.com/search?count=100&mkt=en-GB&q="
  headers <- httr::add_headers(`x-rapidapi-host` = "microsoft-azure-bing-news-search-v1.p.rapidapi.com", `x-rapidapi-key` = "f27ee02a9bmsh0dd7e243b9bcadfp1499e0jsn9fcd26e25ba3")
  
  formatted_query <- utils::URLencode(query)
  
  full_url <- paste0(base_url, formatted_query)
    
  response <- httr::GET(formatted_query, headers = headers)
  
  # store remaining requests so we don't go over
  remaining_requests <- httr::headers(x)[["x-ratelimit-requests-remaining"]]
  
  # save this somewhere and 
}


f_category <- function(x) x %>% map(pluck, "label") %>% paste0(collapse = "; ")

f_entities <- function(x, type = "title") x[[type]] %>% map(pluck, "text") %>% paste0(collapse = "; ")

f_keywords <- function(x) x %>% paste0(collapse = "; ")

f_links <- function(x) x$permalink

f_source_name <- function(x) x$name
f_source_country <- function(x) {
  location <- x$locations
  if (length(location) == 0) {
    return(NA_character_)
  } else {
    return(location[[1]]$country)
  }
}
f_sentiment <- function(x, type = "title", measure = "polarity") {
  if (is.null(x)) {
    if (measure == "score") {
      return(NA_real_)
    } else {
      return(NA_character_)
    }
    
  }
  if (!type %in% names(x)) {
    return(x[[measure]])
  }
  x[[type]][[measure]]
}

f_cluster <- function(x) {
  if (is_empty(x)) {
    return(NA_integer_)
  } else {
    return(x[[1]])
  }
}

strip_and_truncate <- function(strs) {
  strs %>% str_remove_all("[^[:alnum:]]") %>% str_sub(1, 10) %>% tolower()
}

filter_sources <- function(news) {
  sources <- readLines("sources.txt") %>%
    strip_and_truncate()
  news %>% filter(strip_and_truncate(source_name) %in% sources)
}

news2csv <- function(news, filter = T) {
  news <- news %>% mutate(source_name = map_chr(source, f_source_name)) 
  
  if (filter) {
    news <- filter_sources(news)
  }
  
  news <- news %>% mutate(categories = map_chr(categories, f_category),
                          entities_title = map_chr(entities, f_entities, type = "title"),
                          entities_body = map_chr(entities, f_entities, type = "body"),
                          keywords = map_chr(keywords, f_keywords),
                          summary = map_chr(summary, f_keywords),
                          link = map_chr(links, f_links),
                          #source_name = map_chr(source, f_source_name),
                          clusters = map_dbl(clusters, f_cluster),
                          source_country = map_chr(source, f_source_country),
                          title_sentiment_polarity = map_chr(sentiment, f_sentiment, type = "title", measure = "polarity"),
                          title_sentiment_score = map_dbl(sentiment, f_sentiment, type = "title", measure = "score"),
                          body_sentiment_polarity = map_chr(sentiment, f_sentiment, type = "body", measure = "polarity"),
                          body_sentiment_score = map_dbl(sentiment, f_sentiment, type = "body", measure = "score"))
  
  news %>% select_if(negate(is.list)) %>%
    mutate(body = str_replace_all(body, "[^[:alnum:]]", " "))
}

#ids$ent <- ids$entities %>% map(function(x) x$title %>% keep(~"Organisation" %in% .$types || "Company" %in% .$types) %>% map_chr(pluck, "text"))
#df <- df %>% filter(!is.na(clusters)) %>% group_by(Firm, clusters) %>% mutate(i = row_number()) %>% filter(i == 1) %>% select(-i) %>% ungroup() %>% rbind(df %>% filter(is.na(clusters)))

word_df <- df %>% select(Firm, title) %>% unnest_tokens(word, title) %>% anti_join(stop_words, by = "word")

mapper <- word_df %>% distinct(word) %>% mutate(stem = textstem::stem_words(word))

word_df <- word_df %>% left_join(mapper, by = "word") %>% select(-word) 
