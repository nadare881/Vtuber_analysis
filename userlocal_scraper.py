import re
from time import sleep

from bs4 import BeautifulSoup
import pandas as pd
import requests
from tqdm import tqdm


def get_userpage(userlocal_id):
    r = requests.get("https://virtual-youtuber.userlocal.jp/user/" + userlocal_id)
    sleep(1)
    soup = BeautifulSoup(r.content, "lxml")
    TW_screen_name, other_channel = None, None
    if len(soup.select("body > div > div > div > div > div > div > div > span > span > a")):
        TW_screen_name = soup.select("body > div > div > div > div > div > div > div > span > span > a")[0].text
    if soup.find("div", class_="channel-body").find("a"):
        other_channel = soup.find("div", class_="channel-body").find("a").get("href")
    return TW_screen_name, other_channel


def get_youtube_url(userlocal_id):
    r = requests.get("https://virtual-youtuber.userlocal.jp/schedules/new?youtube=" + userlocal_id)
    sleep(1)
    soup = BeautifulSoup(r.content, "lxml")
    youtube_url = soup.find("input", class_="form-control border", id="live_schedule_channel_url").get("value")
    return youtube_url


def ranking_scrape(page):
    data = []
    r = requests.get("https://virtual-youtuber.userlocal.jp/document/ranking?page={}".format(page))
    sleep(1)
    soup = BeautifulSoup(r.content, "lxml")

    for j in range(50):
        col = soup.select("body > div > table > tbody > tr")[j]
        rank = col.find("strong").text.replace("位", "")
        name = re.sub(r"\s{3,40}", "", col.find_all("a")[1] \
                      .text.replace("\n", ""))
        userlocal_id = col.a.get("href").split("/")[-1]

        channel, office = None, None
        if col.find("span", class_="text-secondary") != None:
            channel = col.find("span", class_="text-secondary").text
        if col.find("div", class_="box-office"):
            office = col.find("div", class_="box-office").find("a").text

        TW_screen_name, other_channel = get_userpage(userlocal_id)
        youtube_url = get_youtube_url(userlocal_id)
        data.append([rank, name, channel, office, TW_screen_name, other_channel, youtube_url])
    return data


if __name__ == "__main__":
    data = []
    i = 1
    end = 40

    while i <= end:  # 合計4000秒くらいかかる
        try:
            for j in tqdm(range(i, end+1)):
                data.extend(ranking_scrape(j))
                i += 1
        except:
            sleep(15)
            pass

    df = pd.DataFrame(data)
    df.columns = ["rank", "name", "channel_name", "office", "TW_screen_name", "other_channel", "youtube_url"]
    df.to_csv("userlocal.csv")
