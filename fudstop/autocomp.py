import os
from dotenv import load_dotenv
load_dotenv()
from apis.polygonio.polygon_options import PolygonOptions
from apis.webull.webull_options import WebullOptions
import pandas as pd
wo = WebullOptions(connection_string=os.environ.get('WEBULL_OPTIONS'))
opts = PolygonOptions()


selected_ticker = None
async def ticker_autocomp(inter, ticker: str):
    global selected_ticker
    await opts.connect()
    if not ticker:
        return ["TYPE A TICKER"]

    # assuming db_manager is globally defined and connected
    query = f"""SELECT DISTINCT ticker FROM options_data WHERE ticker LIKE '{ticker}%' LIMIT 24;"""
    results = await opts.fetch(query)
    
    if not results:
        return []

    # convert to DataFrame just for demonstration, not actually needed
    df = pd.DataFrame(results, columns=['ticker'], index=None)
    selected_ticker = ticker
    # Return the symbols
    return df['ticker'].str.upper().tolist()[:24]


async def strike_autocomp(inter, strike: str):
    global selected_ticker  # Declare the variable as global to read it
    if not strike:
        return ["TYPE A STRIKE"]
        
    query = f"""SELECT DISTINCT CAST(strike AS text) FROM options_data WHERE ticker = '{selected_ticker}' AND CAST(strike AS text) LIKE '{strike}%' LIMIT 24;"""
    results = await opts.fetch(query)
    if not results:
        return []
    df = pd.DataFrame(results, columns=['strike'])
    # Return the symbols
    return df['strike'].str.lower().tolist()[:24]

    


async def expiry_autocomp(inter, expiry: str):

    global selected_ticker  # Declare the variable as global to read it
    if not expiry:
        return ["CHOOSE", "AN", "EXPIRY"]
        
    query = f"""SELECT DISTINCT CAST(expiry AS text) FROM options_data WHERE ticker = '{selected_ticker}' AND CAST(expiry AS text) LIKE '{expiry}%' LIMIT 24;"""
    results = await opts.fetch(query)
    if not results:
        return []
    df = pd.DataFrame(results, columns=['expiry'])
    # Return the symbols
    df['expiry'] = pd.to_datetime(df['expiry'])
    df['expiry'] = df['expiry'].apply(lambda x: x.strftime('%y%m%d'))
    return df['expiry'].str.lower().tolist()[:24]


