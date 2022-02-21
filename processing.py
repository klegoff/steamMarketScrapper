# -*- coding: utf-8 -*-
"""
Created on Sun Feb 20 12:18:29 2022
@author: killi
Format raw data and save into formattedData.pickle
"""
import re
import os, sys 
import time
import pandas as pd
from scrapper import getSession, getItemHistory, getItems

# Path
projectPath = "~/git_synchronized/steamMarketScrapper/"#"C:/Users/killi/Desktop/steamMarketScrapper/"
dataPath = projectPath + "data/"

##############################
# functions
##############################

# Split skin name in "star", "weapon", "name", "wear"
def splitName(string):
    """
    string of full skin name -> name, weapon, star, wear
    """
    string = string.replace(" ","") # remove spacing
    
    # star
    star = "★" in string
    string = string.replace("★","")
    
    # stattrak
    stattrak = "StatTrak™" in string
    string = string.replace("StatTrak™","")

    weapon, skin, wear = re.split(r"[(|)]\s*", string)[:3]
   
    return weapon, skin, wear, star, stattrak

def scrapItems(session):
    """
    get all items
    """
    # récupère les données et concat
    dataList = []
    currPos = 0

    while True:
        # délai entre les requetes pour éviter un ban ip
        time.sleep(10)

        try :
            print("currPos=",currPos)
            data = getItems(currPos, session)
            # requete vide => on arrete
            if data.shape[0] == 0:
                break

            dataList.append(data)
            currPos += 100

        except Exception as e:
            print("currPos=",currPos)
            print(e)
            break

    data = pd.concat(dataList)
    data.reset_index(drop=True, inplace=True)

    return data

def scrapItemHistory(session):
    """
    scrap items history
    """
    df_list = []
    print("Item number to process :", data.shape[0])
    for item in data["name"]:
        print(item, len(df_list))
        time.sleep(10)
        history = getItemHistory(item, session)
        history["name"] = item
        df_list.append(history)

    df = pd.concat(df_list)
    return df

##############################
# scrap item names
##############################

session = getSession()

data = scrapItems(session)

# save data
data.to_pickle(dataPath + "rawItemData.pickle")

# Load data
#data = pd.read_pickle(dataPath + "rawData.pickle")

##############################
# process item names
##############################

# Drop useless columns
dropCol = ["hash_name","app_icon", "app_name", "asset_description"]
data.drop(columns=dropCol, inplace=True)

# filter out souvenir packages & navaja knife
mask = ~data["name"].apply(lambda el : "Package" in el)
mask &= ~(data["name"] == "★ Navaja Knife")
data = data.loc[mask].reset_index(drop=True)

out = data["name"].apply(splitName)

for i, col in enumerate(["weapon", "skin", "wear", "star", "stattrak"]):
    data[col] = out.map(lambda x: x[i])
    
# remove raw column
#dropCol = ["name"]
#data.drop(columns=dropCol, inplace=True)

# rename listings and prices columns
renameDict = {"sell_listings":"sellListing","sell_price":"buyListing","sell_price_text":"sellPrice($)","sale_price_text":"buyPrice($)"}
data.rename(columns=renameDict, inplace=True)

# cast prices to float
data[["sellPrice($)","buyPrice($)"]] =data[["sellPrice($)","buyPrice($)"]].applymap(lambda el : float(el.replace("$","")))

# keep chosen price range
mask = (data["buyPrice($)"] <= 500) & (data["buyPrice($)"] >= 120)
data = data.loc[mask]

data.to_pickle(dataPath + "skinData.pickle")

##############################
# scrap item history
##############################

df = scrapItemHistory(session)

# save formatted dataset
df.to_pickle(dataPath + "historyData.pickle")
data.to_pickle(dataPath + "skinData.pickle")


