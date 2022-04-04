# steamMarketScrapper
Scrap market historical prices for csgo items.


**To launch** :<br/>
  -fill "credentials.json" with a steam account <br/>
  -exectute "processing.py" (ex : "nohup python3 processing.py &") <br/>
<br/>
<br/>
The project returns **these files**: <br/>
  -"rawSkinData.pickle" : every items of the csgo steam market <br/>
  -"skinData.pickle" : same after processing (filter some items, remove duplicates) <br/>
  -"rawHistoryData.pickle" : raw historical values for the items in the skinData <br/>
  -"historyData.pickle" : formatted historical value, at daily frequency (when many values on a same day, we sum sold quantities and takes the weighted mean of the medians prices) <br/>
<br/>
<br/>
Using python 3.8.10
