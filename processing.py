# -*- coding: utf-8 -*-
"""
Created on Sun Feb 20 12:18:29 2022
@author: klegoff
Scrap data from items and their prices history,
format raw data
"""
# speed up the process by skipping part of the process, for debug purpose
# use False only on parts that have alread been completed
SCRAP_ITEMS = True # if False, skip item scrapping
SCRAP_HISTORY = True # if False, skip historical values scrapping
PROCESS_HISTORY = True # if False, skip the historical data formatting

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

def processDates(historyData):
    """
    around dates to 00:00 of the current day
    the goal is to get regularity in the data (by day)
    input:
        historyData (type = pd.Dataframe) containing "date" column (type = pd.Timestamp)
    output:
        columns of dates resulting of the processing
    """
    def aroundDate(t):
        """
        around date to the same day at 00:00
        input : 
            t (pd.Timestamp)
        """
        if t.hour == 0:
            return t
        else:
            return t.ceil("D") - pd.Timedelta('1D')

    # every unique dates
    unique_dates = list(historyData["date"].drop_duplicates())

    # link between date and around date
    around_dict = dict(zip(unique_dates,list(map(aroundDate, unique_dates))))

    # replace dates 
    return historyData["date"].map(around_dict)

##### manage index and grouping, resulting of previous date processing

    
def combineDuplicates(historyData):
    """
    find references (item, date) that are duplicated
    and fusion them in one line per day
    """
    
    def idxGroupBy(historyData):
        """
        return indexes of historyData (df) that correspond to group_by operation
        """
        grp_df = historyData.groupby(["name", "date"])

        g = grp_df.groups # here groupby computes index, and is the most time consuming part
        groups = np.array(list(g.keys()))
        idxGroups = list(g.values())
        idxGroups = [list(el) for el in idxGroups]

        # number of val for each group
        lenGroups = np.array([len(el) for el in idxGroups])

        # get indexes of groupsby (idxGroups), where we have duplicates (lenGroups > 1)
        mask = (lenGroups > 1)
        dfIdxToProcess = list(map(idxGroups.__getitem__, np.where(mask)[0])) 
        dfIdxToKeep =     list(map(idxGroups.__getitem__, np.where(~mask)[0])) 

        return dfIdxToKeep, dfIdxToProcess

    def computeSyntheticArray(groupHistoryArray):
        """
        concatenate a group of rows with data from the same day and same item
        optimized with numpy
        input:
            groupHistoryArray (type = np.array) values for 1 day and 1 item (/!\ can have length > 1)
        """
        priceArray, quantityArray = groupHistoryArray[:,1], groupHistoryArray[:,2]
        new_qty = quantityArray.sum()
        new_price = (priceArray * quantityArray).sum() / new_qty
        return np.array([groupHistoryArray[0,0], new_price,new_qty, groupHistoryArray[0,-1]])

    dfIdxToKeep, dfIdxToProcess = idxGroupBy(historyData)

    # convert to numpy arrays
    historyDataArray = historyData.values

    # synthetize references with many values per day & item
    outArray = [computeSyntheticArray(historyDataArray[grp_idx]) for grp_idx in dfIdxToProcess]
    formattedData = pd.DataFrame(outArray, columns=historyData.columns)

    # get dataset with no duplicates
    keepData = historyData.iloc[np.concatenate(dfIdxToKeep)]

    # gather both 
    return pd.concat([keepData, formattedData]).reset_index(drop=True)

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

if SCRAP_HISTORY:
    # request historic, item by item
    historyData = scrapItemHistory(data, session)

    # save formatted dataset
    historyData.to_pickle(file2)


else :
    # we load the formatted dataset
    historyData = pd.read_pickle(file2)

##############################
# format historical data
##############################
file4 = dataPath + "historyData.pickle"

if PROCESS_HISTORY:
    
    logging.warning("Processing historical values")
    
    # formate historical data
    historyData = historyData.drop(columns=["month", "day","year"])

    # process dates : we convert to 00:00 time
    historyData["date"] = processDates(historyData)

    # combine (date, item) references with many values (> 1)
    historyData.reset_index(drop=True, inplace=True)
    formattedHistoryData = combineDuplicates(historyData) # approx 15 min on the whole dataset

    # convert into timeseries format
    ts_df = pd.pivot_table(formattedHistoryData, index="name", columns="date")

    # save
    ts_df.to_pickle(file4)
