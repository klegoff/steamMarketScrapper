# -*- coding: utf-8 -*-
"""
Created on Sun Feb 20 15:58:32 2022

@author: killi
"""
import steam.webauth as wa
import numpy as np
import json
import requests
import pandas as pd

# Path
projectPath = "~/git_synchronized/steamMarketScrapper/"#"C:/Users/killi/Desktop/steamMarketScrapper/"
dataPath = projectPath + "data/"

# connect to steam
def getSession():
    user = wa.WebAuth('7AXXnddb5N48')
    session = user.cli_login('B7w38adb2JPBD68ab')
    return session

#### URL 
# item = np.random.choice(data["name"])

def getItemHistory(item, session):
    url = "https://steamcommunity.com/market/pricehistory/?appid=730&market_hash_name=" + item

    # get content & process
    out = session.get(url)

    out = json.loads(out.text)["prices"]
    df = pd.DataFrame(out, columns=["date","medianPrice($)", "sold"])

    #process date
    df["date"] = df["date"].apply(lambda el: pd.to_datetime(el.split(":")[0]))
    df["month"] = df.date.apply(lambda el : el.month)
    df["day"] = df.date.apply(lambda el : el.day)
    df["year"] = df.date.apply(lambda el : el.year)

    #process sold quantity
    df["sold"] = df["sold"].apply(int)
    return df

### get items
def getItems(currPos = 10, session=None):
    """
    retourne le résultat de la requete, pour 100 items à partir de la position donnée (currPos)
    """
    # mets en forme l'url
    splitUrl = ["https://steamcommunity.com/market/search/render/?",
    "start="+str(currPos),
    "&",
    "count=100"
    "search_descriptions=0",
    "&" ,
    "sort_column=default",
    "&",
    "sort_dir=desc",
    "&",
    "appid=730",
    "&",
    "norender=1"]
    
    url = ""
    
    for el in splitUrl:
        url += el
    
    # requete et mise en forme
    r = requests.get(url)
    out = r.text
    
    jsonData = json.loads(out)
    data = pd.DataFrame(jsonData["results"])
    
    return data
