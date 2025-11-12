import os
from dotenv import load_dotenv
load_dotenv()
from apis.polygonio.polygon_options import PolygonOptions
import pandas as pd
opts = PolygonOptions(database='fudstop3')
import asyncpg

selected_ticker = None
async def ticker_autocomp(inter, ticker: str):
    global selected_ticker
    await opts.connect()
    if not ticker:
        return ["TYPE A TICKER"]

    # assuming db_manager is globally defined and connected
    query = f"""SELECT DISTINCT ticker FROM master_all_two WHERE ticker LIKE '{ticker}%' LIMIT 24;"""
    results = await opts.fetch(query)
    
    if not results:
        return []

    # convert to DataFrame just for demonstration, not actually needed
    df = pd.DataFrame(results, columns=['ticker'], index=None)
    selected_ticker = ticker
    # Return the symbols
    return df['ticker'].str.upper().tolist()[:24]

# Database connection parameters
DATABASE_CONFIG = {
    'user': 'chuck',
    'password': 'fud',
    'database': 'fudstop3',
    'host': 'localhost',
    'port': 5432
}

async def column_autocomp(inter, column_hint: str):
    # Connect to the database
    conn = await asyncpg.connect(**DATABASE_CONFIG)

    # Query to fetch column names from the "master_all_two" table
    query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'master_all_two' AND column_name LIKE $1;
    """

    # Search for columns matching the hint
    results = await conn.fetch(query, f'{column_hint}%')

    # Close the connection
    await conn.close()

    if not results:
        return []

    # Convert to DataFrame for demonstration
    df = pd.DataFrame(results, columns=['column_name'], index=None)

    # Return the column names as a list for autocomplete
    return df['column_name'].str.upper().tolist()




async def strike_autocomp(inter, strike: str):
    global selected_ticker  # Declare the variable as global to read it
    if not strike:
        return ["TYPE A STRIKE"]
        
    query = f"""SELECT DISTINCT CAST(strike AS text) FROM master_all_two WHERE ticker = '{selected_ticker}' AND CAST(strike AS text) LIKE '{strike}%' LIMIT 24;"""
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
        
    query = f"""SELECT DISTINCT CAST(expiry AS text) FROM master_all_two WHERE ticker = '{selected_ticker}' AND CAST(expiry AS text) LIKE '{expiry}%' LIMIT 24;"""
    results = await opts.fetch(query)
    if not results:
        return []
    df = pd.DataFrame(results, columns=['expiry'])
    # Return the symbols
    df['expiry'] = pd.to_datetime(df['expiry'])
    df['expiry'] = df['expiry'].apply(lambda x: x.strftime('%Y-%M-%d'))
    return df['expiry'].str.lower().tolist()[:24]


