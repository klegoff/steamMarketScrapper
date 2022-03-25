# -*- coding: utf-8 -*-
"""
Created on Sun Feb 20 12:18:29 2022
@author: klegoff
Scrap data from items and their prices history,
format raw data
"""
SCRAP_ITEMS = True # speed up the process by skipping the item scrapping, for debug purpose

import os, re, time, logging
import numpy as np
import pandas as pd
from scrapper import getSession, getItemHistory, getItems

# Path
projectPath = os.getcwd() + os.sep 
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
    
    try:
        weapon, skin, wear = re.split(r"[(|)]\s*", string)[:3]
    except:
        #designed for special items (vanilla knifes)
        weapon, skin, wear = string, "Vanilla", "None"

    return weapon, skin, wear, star, stattrak

def scrapItems(session):
    """
    get all items, by request of 100 items
    :input:    
        session : steam session object
    :output:
        data : item dataset (type = pd.DataFrame)
    """
    dataList = []
    currPos = 0

    while True:
        #delai between requests to avoid ip ban
        #time.sleep(10) #request errors are manage by retry_

        try :
            logging.warning("currPos="+str(currPos))
            data = getItems(currPos, session)
            # we stop when query results are empty
            if data.shape[0] == 0:
                break

            dataList.append(data)
            currPos += 100

        except Exception as e:
            logging.warning("currPos="+str(currPos))
            logging.warning(e)
            break

    data = pd.concat(dataList)
    data.reset_index(drop=True, inplace=True)

    return data

def scrapItemHistory(data, session):
    """
    scrap item historical prices, from the list of items
    :inputs:
        data : dataset of all products , of which we want to get historical values (type = pd.DataFrame)
        session : steam session object
    :output:
        df : dataset of product historical values (type = pd.DataFrame)
    """
    df_list = []
    logging.warning("Item number to process :" + str(data.shape[0]))
    for item in data["name"]:
        try:
            logging.warning(item + " - " + str(len(df_list)))
            #time.sleep(10) #request errors are managed with request_policy
            history = getItemHistory(item, session)
            history["name"] = item
            df_list.append(history)
        except Exception as e:
            logging.warning(e)
            
    df = pd.concat(df_list)
    return df

##############################
# scrap item names
##############################
file0 = dataPath + "rawSkinData.pickle"

session = getSession()

if SCRAP_ITEMS:
    # 16 000 items, processed by 10
    data = scrapItems(session)

    # save data
    data.to_pickle(file0)

else:
    # Load data
    data = pd.read_pickle(file0)

##############################
# process item names
##############################

file1 = dataPath + "skinData.pickle"

# remove banned keywords (sticker, capsule, etc...)
bannedKeyword = ["Sticker", "Capsule", "Graffiti", "Package", "Pin", "Patch", "Pass", "Key", "Holo", "Foil", "Music Kit", "Audience", "Challengers", "EMS Katowice", "Pallet"]
mask = data.name.apply(lambda el : (np.sum([k in el for k in bannedKeyword]) == 0))
data = data.loc[mask].reset_index(drop=True)

# split names
out = data["name"].apply(splitName)

for i, col in enumerate(["weapon", "skin", "wear", "star", "stattrak"]):
    data[col] = out.map(lambda x: x[i])

# rename listings and prices columns
renameDict = {"sell_listings":"sellListing","sell_price":"buyListing","sell_price_text":"sellPrice($)","sale_price_text":"buyPrice($)"}
data.rename(columns=renameDict, inplace=True)

# cast prices to float
data[["sellPrice($)","buyPrice($)"]] =data[["sellPrice($)","buyPrice($)"]].applymap(lambda el : float(el.replace("$","").replace(",","")))

# filter only some items (speeds up the next scrapper) 
#mask = (data["buyPrice($)"] <= 420) & (data["buyPrice($)"] >= 200)
#data = data.loc[mask]

# remove duplicates
data = data.drop_duplicates(subset="name").reset_index()

data.to_pickle(file1)

##############################
# scrap item history
##############################

file2 = dataPath + "rawHistoryData.pickle"

# request historic, item by item
df = scrapItemHistory(data, session)

# save formatted dataset
df.to_pickle(file2)
