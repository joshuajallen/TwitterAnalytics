import twitter
import os
import pandas as pd
import numpy as np
from datetime import datetime as dt

api = twitter.Api(consumer_key="consumer_key",
                  consumer_secret="consumer_secret",
                  access_token_key="access_token_key",
                  access_token_secret="access_token_secret")

def get_tweets(query, since_id=None, max_id=None):
    results = api.GetSearch(
        term=query, 
        count=100, 
        result_type="recent", 
        lang="en", 
        since_id=since_id,
        max_id=max_id)

    return(results)

def tweets2df(tweets, firm = None, handles = []):
    df = pd.DataFrame({
        "Firm":[firm] * len(tweets),
        "ID":[x.id for x in tweets], 
        "Time":[x.created_at for x in tweets], 
        "Text":[x.text for x in tweets],
        "User":[x.user.id for x in tweets],
        "RT":[None if x.retweeted_status == None else x.retweeted_status.id for x in tweets],
        "From_Firm":[x.user.screen_name in handles for x in tweets]
        })

    return(df)

def get_list_of_handles(x):
    list_of_handles = [t.strip() for t in x.split(";")]
    return(list_of_handles)

def make_query(x):
    term = " OR ".join(['"' + t.strip() + '"' if len(t) > 0 else t for t in x.Term.split(";")])
    list_of_handles = get_list_of_handles(x.Handle)
    handle = " OR ".join(["@" + t if len(t) > 0 else t for t in list_of_handles])
    exclude = " ".join(['-"' + t.strip() + '"' if len(t) > 0 else t for t in x.Exclude.split(";")])

    full_term = term + (" OR " if len(handle) > 0 else "") + handle + (" " if len(exclude) > 0 else "") + exclude

    return(full_term)

def get_all_tweets():
    df = pd.read_csv("twitter_firms.csv").fillna("")
    df = df.assign(Query = df.apply(make_query, axis = 1))
    df = df[df.Query != ""]
    
    df["Tweets"] = [tweets2df(get_tweets(query, since_id = latest_id), firm = firm, handles = get_list_of_handles(handles)) 
        for (query, firm, latest_id, handles) in zip(df.Query, df.Firm, df.Latest_ID, df.Handle)]

    df_to_write = pd.concat(df.Tweets.tolist())

    df_to_write.ID = df_to_write.ID.astype("int64")
    df_to_write.User = df_to_write.User.astype("int64")
    df_to_write.RT = df_to_write.RT.fillna(-1).astype("int64")

    filepath = "data/" + max([dt.strptime(t, "%a %b %d %H:%M:%S +0000 %Y") for t in df_to_write.Time]).strftime("%Y%m%d-%H%M%S") + ".csv"

    df_to_write.to_csv(filepath, index = False)

    df["Latest_ID"] = [x.ID[0] if x.shape[0] > 0 else old_status_id for (x, old_status_id) in zip(df.Tweets, df.Latest_ID)]

    df.Latest_ID = df.Latest_ID.astype("int64")

    df.drop(columns=["Query", "Tweets"]).to_csv("twitter_firms.csv", index = False)

    return(df_to_write)

