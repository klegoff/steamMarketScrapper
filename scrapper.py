# -*- coding: utf-8 -*-
"""
Created on Sun Feb 20 15:58:32 2022
@author: klegoff
Scrapping function, using steam authentication adn some rest time to bypass the request limits
"""
import os, json, logging
import numpy as np
import pandas as pd

from requests.adapters import HTTPAdapter, Retry
import steam.webauth as wa

# Path
projectPath = os.getcwd() + os.sep
dataPath = projectPath + "data/"

# retrieve credentials
account, password = json.load(open(projectPath + "credentials.json")).values()

# connect to steam
def getSession():
    logging.debug("connecting to steam")
    user = wa.WebAuth(account)
    session = user.cli_login(password)
    # retry policy
    retry_policy = Retry(total = 12, backoff_factor = 1, status_forcelist = ["400"])
    session.mount("http://", HTTPAdapter(max_retries = retry_policy))
    return session

#### URL scrapping functions

# item = np.random.choice(data["name"]) #random example, for debugging

def getItemHistory(item, session):
    """
    get the historic values for the specified item
    do some basic formatting
    :inputs:
        item : item name in the market (type = str)
            example : "StatTrak™ AWP | Oni Taiji (Field-Tested)"
        session : steam session object
    :output:
        df : item history values (type = pd.DataFrame)
    """
    logging.debug("Get item history : " + item)

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

def getItems(currPos = 10, session=None):
    """
    return the result of the query, 100 items from the specified position
    :inputs:
        currPos : current position in the index (type = int)
        session : steam session object    
    :output:
        data : info about items (type = pd.DataFrame)
    """
    logging.debug("Get item data - pos " + str(currPos))

    # format the url
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
    
    # execute the query
    out = session.get(url)
    jsonData = json.loads(out.text)
    data = pd.DataFrame(jsonData["results"])
    return data
