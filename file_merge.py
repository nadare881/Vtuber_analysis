import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import seaborn as sns

import re
import requests
from requests_oauthlib import OAuth1
import os
from tqdm import tqdm

from time import sleep

youtube = pd.read_csv("youtube.csv")
twitter = pd.read_csv("twitter.csv")
userlocal = pd.read_csv("userlocal.csv")
for df in [youtube, twitter, userlocal]:
    df.drop("Unnamed: 0", axis=1, inplace=True)
kyoyu = pd.read_csv("Vtuber共有用.csv", engine="python", encoding="utf-8")
kyoyu.columns = ["youtube_url", "channel_name", "since", "official", "dimention", "voice"]
kyoyu = kyoyu.drop(["channel_name", "since"], axis=1)
userlocal = userlocal.drop(["name", "channel_name", "office", "other_channel"], axis=1)

data = pd.merge(userlocal, kyoyu, how="left", on="youtube_url")
data = data.drop_duplicates(["youtube_url"])

data["official"] = data.official.replace(np.NaN, "").replace({"個人": "personal",
                                                                "企業": "official",
                                                                '同人': "personal",
                                                                '個人（同人）': "personal",
                                                                '※企業': "personal",
                                                                '法人': "official",
                                                                "": "personal"})

data.loc[data.dimention=="リアル", "dimention"] = ["三次元", "二次元", "二次元"]
data["dimention"] = data.dimention.replace({"二次元": "2D",
                                            "三次元": "3D"})

def rep(x):
    if type(x) == float:
        return np.NaN
    
    if x[0] == "男":
        return "voice_male"
    elif x[0] == "女":
        return "voice_female"
    elif x[:2] == "機械":
        return "voice_machine"
    else:
        return x
    
data["voice"] = data.voice.apply(rep).replace({"エフェクト": "voice_female",
                                               "ｓ": "voice_female",
                                               "ノイズ": "voice_machine",
                                               "猫": "voice_machine"})
data["channel_id"] = data["youtube_url"].apply(lambda x: x.split("/")[-1])

youtube = youtube.drop("channel_published_at", axis=1)
data = pd.merge(data, youtube, on="channel_id")

twitter = twitter.drop(["display_name", "created_at"], axis=1)
data = pd.merge(data, twitter, left_on="TW_screen_name", right_on="screen_name")

data = data.drop(["TW_screen_name", "youtube_url", "channel_id", "screen_name"], axis=1)
data.to_csv("input.csv")