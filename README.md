# steamMarketScrapper
Scrap market historical prices for csgo items.


**To launch** :<br/>
  -fill "credentials.json" with a steam account <br/>
  -exectute "processing.py" (ex : "nohup python3 processing.py &") <br/>
<br/>
<br/>
The project returns **three files**: <br/>
  -"rawSkinData.pickle" : every items of the csgo steam market <br/>
  -"skinData.pickle" : same after processing (filter some items, remove duplicates) <br/>
<br/>
<br/>
Using python 3.8.10
