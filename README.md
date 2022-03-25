# steamMarketScrapper
Scrap market historical prices for csgo items.

To launch :
  -fill "credentials.json" with a steam account
  -exectute "processing.py" (ex : "nohup python3 processing.py &")


The project returns three files:
  -"rawSkinData.pickle" : every items of the csgo steam market
  -"skinData.pickle" : same after processing (filter some items, remove duplicates)
  -"rawHistoryData.pickle" : historic values for every product in the previous file.
  
Using python 3.8.10
