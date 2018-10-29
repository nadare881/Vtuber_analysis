# -*- coding: utf-8 -*-
from time import sleep

from tqdm import tqdm
import pandas as pd
import numpy as np
import requests
from requests_oauthlib import OAuth1Session, OAuth1

consumer_key = ""
consumer_secret = ""
token = ""
token_secret = ""

auth = OAuth1(consumer_key, consumer_secret, token, token_secret)
res = requests.get("https://api.twitter.com/1.1/statuses/user_timeline.json", auth=auth)
base = pd.read_csv("userlocal.csv")

tweet_df = []
tw_profiles = []
errors = []
params = {"count": 200,
          "page": 1}

for screen_name in tqdm(base.TW_screen_name.dropna().drop_duplicates()):
    params["screen_name"] = screen_name[1:]
    res = requests.get("https://api.twitter.com/1.1/statuses/user_timeline.json", auth=auth, params=params)
    if not res.ok:
        errors.append([screen_name, 0])
        continue
    items = res.json()
    if len(items) == 0:
        errors.append([screen_name, 1])
        continue
    item = items[0]
    display_name = item["user"]["name"]
    user_created_at = item["user"]["created_at"]
    friends = item["user"]["friends_count"]
    folloers = item["user"]["followers_count"]
    fav = item["user"]["favourites_count"]
    tweet_count = item["user"]["statuses_count"]
    tw_profiles.append([screen_name, display_name, user_created_at, friends, folloers, fav, tweet_count])

    for item in items:
        created_at = item["created_at"]
        mention_to = []
        for mention in item["entities"]["user_mentions"]:
            mention_to.append("@" + mention["screen_name"])
        mention_to = ",".join(mention_to)
        mentioned = len(mention_to) > 0
        retweeted = item["retweeted"]
        text = item["text"]
        tweet_df.append([screen_name, created_at, mention_to, mentioned, retweeted, text, len(text)])
    sleep(1.1)

profile_df = pd.DataFrame(tw_profiles)
profile_df.columns = ["screen_name", "display_name", "created_at", "follow", "follower", "favourites", "tweet_count"]
profile_df["created_at"] = profile_df.created_at.apply(lambda x: pd.Timestamp.strptime(x, "%a %b %d %H:%M:%S +0000 %Y"))
profile_df["TW_days"] = pd.datetime.now() - profile_df["created_at"]
profile_df["TW_days"] /= np.timedelta64(1, "D")

tweet_df = pd.DataFrame(tweet_df).drop_duplicates()
tweet_df.columns = ["screen_name", "tweet_date", "mention_to", "mentioned", "retweeted", "text", "text_len"]
tweet_df.drop("text", axis=1) #.to_csv("../csv/tweet_df.csv")

tweet_df["tweet_date"] = tweet_df.tweet_date.apply(lambda x: pd.Timestamp.strptime(x, "%a %b %d %H:%M:%S +0000 %Y"))
vtubers = set(tweet_df.screen_name.unique())
tweet_df.mention_to.fillna("", inplace=True)
mention_myself = []

for i in tqdm(range(tweet_df.shape[0])):
    mention_myself.append(tweet_df.iloc[i]["screen_name"] in tweet_df.iloc[i]["mention_to"])
tweet_df["mention_myself"] = mention_myself

tweet_df.mention_to.fillna("", inplace=True)
mention_vtuber = []
for i in tqdm(range(tweet_df.shape[0])):
    ans = -mention_myself[i]
    ans += len((vtubers.intersection(tweet_df.iloc[i]["mention_to"].split(","))))
    mention_vtuber.append(ans>0)
tweet_df["mention_vtuber"] = mention_vtuber

tmp = tweet_df.groupby("screen_name").apply(lambda x: x.sort_values("tweet_date"))
tmp["before_tweet_date"] = tmp.groupby(level=0)["tweet_date"].shift(1)
tmp["delta_tweet_sec"] = (tmp["tweet_date"] - tmp["before_tweet_date"])
tmp["delta_tweet_sec"] /= np.timedelta64(1, "s")

aggregations = {"mentioned": ["mean"],
                "retweeted": ["mean"],
                "text_len": ["mean"],
                "mention_myself": ["mean"],
                "mention_vtuber": ["mean"],
                "delta_tweet_sec": ["mean"]}

agg = tmp.groupby(level=0).agg(aggregations)
agg.columns = pd.Index([col[0] for col in agg.columns])
agg = agg.reset_index()
twitter_df = pd.merge(profile_df, agg, on="screen_name")
twitter_df.to_csv("twitter.csv")
