library(tidyverse)
library(tidytext)
library(lubridate)

get_words <- function(tidy_sent) {
  tidy_sent <- tidy_sent %>% select(Firm, ID, Time, Text) %>% mutate(Time = ymd_hms(Time))
  tidy_clean <- tidy_sent %>% 
    mutate(Text = str_remove_all(Text, "[^\001-\177]"),
           Text = tolower(Text), 
           Text = str_replace_all(Text, "log [io]n(to)?o?", "login"),
           Text = str_replace_all(Text, "@fscs", "fscs"), 
           Text = str_replace_all(Text, "@fca", "fca"), 
           Text = str_replace_all(Text, "@thefca", "thefca"),
           Text = str_replace_all(Text, "@bankofengland", "bankofengland")) %>%
    mutate(Text = str_remove_all(str_remove_all(Text, "http[^ ]+"), "@[^ ]+")) %>%
    unnest_tokens(output = "word", input = "Text", token = "words") %>% 
    anti_join(stop_words, by = "word") %>% 
    mutate(word = str_remove_all(word, pattern = "[^a-z]"),
           stem_word = SnowballC::wordStem(word, language = "en") ,
           Date = as.Date(Time)) %>% 
    filter(word != "") %>%
    group_by(Firm, Date, Word = stem_word, Unstemmed_Word = word) %>%
    summarise(n = n())
  
  tidy_clean <- tidy_clean %>% group_by(Firm, Date, Word) %>% 
    mutate(n2 = sum(n), largest = n == max(n)) %>% 
    filter(largest) %>% 
    group_by(Firm, Date) %>% 
    mutate(duplicate = duplicated(Word)) %>% 
    filter(!duplicate)
  
  tidy_clean <- tidy_clean %>% 
    select(Firm, Date, Word = Unstemmed_Word, Stemmed_Word = Word, n = n2)
  return(tidy_clean)
}

tidy_tweets <- get_words(all_tweets)

tf_idf_date <- function(df, date){
  min_date <- date - 90
  max_date <- min_date + 90
  df <- df %>% filter(Date <= max_date, Date >= min_date) %>%
    group_by(Firm, Date) 
  
  unique(df$Firm) %>%
    map_df(function(firm) df %>% filter(Firm == firm) %>% bind_tf_idf(Word, Date, n)) %>%
    filter(Date == date)
}

x <- seq(min(tidy_tweets$Date), max(tidy_tweets$Date), by = "1 day") %>% map_df(tf_idf_date, df = tidy_tweets)

top_words <- x %>% group_by(Firm, Date) %>% top_n(10, tf_idf) %>% arrange(Firm, Date, -tf_idf) %>% filter(row_number() <= 10) %>% select(Firm, Date, Word, n, tf_idf) %>%
  arrange(Firm, Date, -n)

top_words <- top_words %>% group_by(Firm, Date) %>% mutate(Word_List = paste(Word, collapse = ", ")) %>% mutate(Word_List = case_when(row_number() == 1 ~ Word_List, T ~ NA_character_))

firms <- read_csv("twitter_firms.csv") %>% select(Firm, Parent_Firm) %>%
  mutate(Parent_Firm = case_when(is.na(Parent_Firm) ~ Firm,
                                 T ~ Parent_Firm))
top_words <- top_words %>% left_join(firms, by = "Firm")

write_csv(top_words, "W:/W Drive New Structure/Regulatory Operations/Chief Operating Office/Innovation/Tableau/Data/twitter_words.csv", na = "")

plot_words <- function() {
  firm <- "Halifax"
  
  top_words <-  x %>% 
    filter(str_detect(Firm, firm)) %>% 
    filter(Date >= "2020-03-28") %>% filter(n > 5) %>% 
    group_by(Date) %>% top_n(5) %>%
    ungroup()
  
  word_list <- x %>%
    filter(str_detect(Firm, firm), Word %in% top_words$Word) %>%
    group_by(Word) %>%
    summarise(n = sum(n)) %>%
    filter(n > 20) %>%
    select(-n)
  
  gf <- x %>% filter(str_detect(Firm, firm)) %>% inner_join(word_list)
  
  ggplot(gf, aes(x = Date, y = n)) + geom_bar(stat = "identity") + 
    facet_wrap(~Word, scales = "free_y")
}