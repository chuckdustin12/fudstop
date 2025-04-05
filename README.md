# fudstop
The all-in-one market data, market research, trading, and data science hub.

## PIP install:

```
pip install fudstop4
```


#STEP 1
Populate the .env file with the API keys you have / wish to use.


from fudstop4.apis.polygonio.polygon_options import PolygonOptions

# Acts as both Polygon API handler and database manager
```db = PolygonOptions(
    user='YOUR USER',
    database='YOUR DATABASE',
    host='localhost',
    port=5432,
    password='YOUR PASSWORD'
)
```




## PolygonOptions acts as a database manager and polygon.io SDK for options related data (API key required)
```from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions(user='YOUR USER', database = 'YOUR DATABASE', host = 'localhost', port = 5432, password = 'YOUR PASSWORD') # you can input your POSTGRES credentials here
```


## call functions (MOST ARE ASYNC)

```
async def example1():
  
  #connect to the database (optional)
  await db.connect()

  all_options_data = await opts.get_option_chain_all(underlying_asset='SPY') # gets all options for SPY

  #most functions are dot-notated for ease of access

  dataframe = all_options_data.df
  volume = all_options_data.volume #list of volume values
  oi = all_options_data.oi #list of oi values
  strike = all_options_data.strike # list of strikes
  expiry = all_options_data.expiry # list of expirations
  call_put = all_options_data.call_put # list of call/put option types

  #easily find unusual options with volume of > 500
  for s, e, cp, v, o in zip(volume,oi, call_put, strike, expiry):
      if v > o and v > 500:
          print(f"Strike ${s} {cp} has unusual options volume of {v} versus {oi} open interest expiring {e}.")
```


## STEP 4 - Useful helper imports
```
from fudstop.apis.helpers import is_etf #checks if a ticker is an etf or not
from fudstop.apis.helpers import generate_webull_headers #can be passed in as headers for webull functions
from fudstop.apis.helpers import format_large_numbers_in_dataframe2 #converts large numbers to readable formats
from fudstop.apis.helpers import chunk_string #chunks a string of data for pagination
from fudstop._markets.list_sets.ticker_lists import most_active_tickers # top list of the most actively traded options tickers
from fudstop._markets.list_sets.ticker_lists import all_tickers #imports a list of all traded equity tickers
```


...... more to come ........
         
 
