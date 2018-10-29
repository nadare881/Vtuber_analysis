from time import sleep

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

apikey = ""

channels_params = {"key": apikey,
                   "part": "snippet,statistics",
                   "maxResults": 50}

search_params = {"key": apikey,
                 "part": "id,snippet",
                 "maxResults": 50,
                 "type": "video",
                 "order": "date"}

videos_params = {"key": apikey,
                 "part": "id,snippet,contentDetails,statistics,liveStreamingDetails",
                 "maxResults": 50,
                 "type": "video",
                 "order": "date"}

time_dict = {"H": 3600, "M": 60, "S": 1}


def get_channels(channel_id_params):
    tmp = []
    channels_params["id"] = channel_id_params
    r = requests.get("https://www.googleapis.com/youtube/v3/channels",
                     params=channels_params)
    for item in r.json()["items"]:
        id_ = item["id"]
        fans = item["statistics"]['subscriberCount']
        video_count = item["statistics"]['videoCount']
        view_count = item["statistics"]['viewCount']
        published_at = item["snippet"]["publishedAt"]
        tmp.append([id_, fans, video_count, view_count, published_at])
    return tmp


def get_videos(channel_id):
    video_ids = []
    search_params["channelId"] = channel_id
    search_response = requests.get("https://www.googleapis.com/youtube/v3/search",
                                   params=search_params)
    for item in search_response.json()["items"]:
        video_id = item["id"]["videoId"]
        video_ids.append(video_id)

    tmp = []
    videos_params["id"] = ",".join(video_ids)
    video_response = requests.get("https://www.googleapis.com/youtube/v3/videos",
                                  params=videos_params)

    for video_item in video_response.json()["items"]:
        if not "publishedAt" in video_item["snippet"].keys():
            continue
        published_at = video_item["snippet"]["publishedAt"]
        duration = video_item["contentDetails"]["duration"]
        title = video_item["snippet"]["title"]
        video_id = video_item["id"]

        view_count, like_count, dislike_count, comment_count = None, None, None, None

        if "viewCount" in video_item["statistics"].keys():
            view_count = video_item["statistics"]["viewCount"]
        if "likeCount" in video_item["statistics"].keys():
            like_count = video_item["statistics"]["likeCount"]
        if "dislikeCount" in video_item["statistics"].keys():
            dislike_count = video_item["statistics"]["dislikeCount"]
        if "commentCount" in video_item["statistics"].keys():
            comment_count = video_item["statistics"]["commentCount"]
        tags = ""
        if "tags" in video_item["snippet"].keys():
            tags = "$".join(video_item["snippet"]["tags"])
        islive = "liveStreamingDetails" in video_item.keys()
        tmp.append([channel_id, video_id, duration, title, view_count,
                    like_count, dislike_count, comment_count,
                    islive, tags, published_at])
    return tmp


def to_sec(duration):
    duration = duration[2:]
    for s in list("HMS"):
        duration = duration.replace(s, " {} ".format(s))
    duration = duration.split()
    sec = 0
    for a, b in zip(duration[::2], duration[1::2]):
        sec += int(a) * time_dict[b]
    return sec


userlocal_data = pd.read_csv("userlocal.csv")
channel_ids = userlocal_data.youtube_url.apply(lambda x: x.split("/")[-1])
channel_data = []
print("get_channel_data")
for start in tqdm(range(0, 2000, 50)):
    channel_id_param = ",".join(channel_ids[start:start+50])
    channel_data.extend(get_channels(channel_id_param))

video_data = []
print("get_videos")
for channel_id in tqdm(channel_ids):
    video_data.extend(get_videos(channel_id))

channel_data = pd.DataFrame(channel_data)
channel_data.columns = ["channel_id", "fans", "video_count", "view_count", "channel_published_at"]
channel_data["channel_published_at"] = channel_data.channel_published_at.apply(lambda x: pd.Timestamp.strptime(x, "%Y-%m-%dT%H:%M:%S.000Z"))
channel_data["Youtube_days"] = pd.datetime.now() - channel_data.channel_published_at
channel_data["Youtube_days"] /= np.timedelta64(1, "D")

video_data = pd.DataFrame(video_data)
video_data.columns = ["channel_id", "video_id", "duration", "title",
                      "view_count", "like_count", "dislike_count",
                      "comment_count", "islive", "tags", "published_at"]

video_data["duration"] = video_data.duration.apply(to_sec)
video_data["duration"] = video_data.duration.replace(0, np.NaN)
video_data.dropna(subset=["duration"], inplace=True)
video_data["published_at"] = video_data["published_at"].apply(lambda x: pd.Timestamp.strptime(x, "%Y-%m-%dT%H:%M:%S.000Z"))
video_data["tag_count"] = video_data["tags"].apply(lambda x: len(str(x).split("$")))
video_data["view_count_per_video"] = video_data["view_count"]
video_data.drop("view_count", axis=1, inplace=True)
video_data = video_data.fillna(0)

tmp = video_data.groupby("channel_id").apply(lambda x: x.sort_values("published_at"))
tmp["video_publish_delta"] = tmp["published_at"] - tmp.groupby(level=0)["published_at"].shift(1)
tmp["video_publish_delta"] /= np.timedelta64(1, "D")


aggregations = {"duration": ["mean"],
                "like_count": ["mean"],
                "dislike_count": ["mean"],
                "comment_count": ["mean"],
                "islive": ["mean"],
                "view_count_per_video": ["mean"],
                "video_publish_delta": ["mean"]}

tmp = tmp[list(aggregations.keys())].astype(np.float64)
agg = tmp.groupby(level=0).agg(aggregations)
agg.columns = pd.Index([col[0] for col in agg.columns])
agg = agg.reset_index()

youtube_data = pd.merge(left=channel_data, right=agg, on="channel_id")
youtube_data.to_csv("youtube.csv")