# fudstop
The all-in-one market data, market research, trading, and data science hub.

## PIP install:

```py
pip install fudstop4
```


#STEP 1
Populate the .env file with the API keys you have / wish to use.


from fudstop4.apis.polygonio.polygon_options import PolygonOptions

# Acts as both Polygon API handler and database manager
```py
    db = PolygonOptions(
    user='YOUR USER',
    database='YOUR DATABASE',
    host='localhost',
    port=5432,
    password='YOUR PASSWORD'
)
```




## Polygon SDK imports and related helpers
```py
from fudstop4.apis.polygonio.polygon_options import PolygonOptions #acts as a database and options SDK for polygon
from fudstop.apis.polygonio.async_polygon_sdk import Polygon #stock related functions

poly = Polygon()
db = PolygonOptions(user='YOUR USER', database = 'YOUR DATABASE', host = 'localhost', port = 5432, password = 'YOUR PASSWORD') # you can input your POSTGRES credentials here

```


## Webull SDK imports and related helpers
```py
from fudstop4.apis.webull.webull_trading import WebullTrading #trading related functions
from fudstop4.apis.webull.webull_markets import WebullMarkets #market related functions
from fudstop4.apis.webull.webull_ta import WebullTA #technical analysis related functions
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions #options related functions
from fudstop4.apis.helpers import generate_webull_headers # requires access_token from developer window (tutorial coming soon)

trading = WebullTrading()
markets = WebullMarkets()
wb_opts = WebullOptions()
wb_ta = WebullTA()

headers = generate_webull_headers()


async def main(ticker:str='SPY'):

    volume_analysis = await trading.volume_analysis(ticker)

    #most functions are dot-notated for ease of attribute access. all dot-notated attributes are either single values or lists of values

    buy_pct = volume_analysis.buy_pct

    dataframe = volume_analysis.df #most functions have a dataframe options stored as 'df' or 'as_dataframe'

    print(dataframe)

#asyncio.run(main()) uncomment to run


#example with headers
async def news(ticker:str='AAPL'):
    """Gets news for a ticker. (headers required)"""
    news = await trading.ai_news(ticker, headers=headers)

    print(news.as_dataframe)

#asyncio.run(news()) uncomment to run

```

## Options Clearing Corporation API

```py
from fudstop4.apis.occ.occ_sdk import occSDK
occ = occSDK()

async def stock_info(ticker:str='SPY'):
    """Gets important stock information for a ticker"""
    info = await occ.stock_info(ticker)

    dataframe = info.as_dataframe

    print(dataframe)

#asyncio.run(stock_info()) uncomment to run

```

## Yahoo Finance API

```py
from fudstop4.apis.yf.yf_sdk import yfSDK

yf = yfSDK()

async def major_holders(ticker:str='MSFT'):
    """Gets major holders for a ticker"""

   dataframe = await yf.major_holders(ticker)
   print(dataframe)

#asyncio.run(major_holders()) uncomment to run
```

## Newyork FED API (Sync SDK)

```py
from fudstop4.apis.newyork_fed.newyork_fed_sdk import FedNewyork
nyfed = FedNewYork()


swaps = nyfed.liquidity_swaps_latest() #gets the latest liquidity swaps
soma = nyfed.soma_holdings()  # gets the latest soma holdings

```

## Useful helper imports


```py
from fudstop.apis.helpers import is_etf #checks if a ticker is an etf or not
from fudstop.apis.helpers import generate_webull_headers #can be passed in as headers for webull functions
from fudstop.apis.helpers import format_large_numbers_in_dataframe2 #converts large numbers to readable formats
from fudstop.apis.helpers import chunk_string #chunks a string of data for pagination
from fudstop._markets.list_sets.ticker_lists import most_active_tickers # top list of the most actively traded options tickers
from fudstop._markets.list_sets.ticker_lists import all_tickers #imports a list of all traded equity tickers
```


...... more to come ........
         
 
