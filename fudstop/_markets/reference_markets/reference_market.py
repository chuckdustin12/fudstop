import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[3])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
import aiohttp
import csv
import asyncio
import requests
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
from conditional_scans import ConditionalScans
from apis.webull.webull_markets import WebullMarkets
from apis.webull.webull_trading import WebullTrading
from apis.polygonio.async_polygon_sdk import Polygon
from apis.polygonio.polygon_options import PolygonOptions
from _markets.list_sets.ticker_lists import most_active_tickers
from discord_.embeddings import Embeddings
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed




class ReferenceMarket(Embeddings):
    def __init__(self):
        
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')     
   
        #conditions

        self.accumulation = os.environ.get('accumulation')
        self.fire_sale = os.environ.get('fire_sale')
        self.neutral_zone = os.environ.get('neutral_zone')
        self.trading = WebullTrading()
        self.markets = WebullMarkets()
        self.poly = Polygon(connection_string=os.environ.get('POLYGON_STRING'))
        self.opts = PolygonOptions(user='chuck', database='market_data')
        self.conditions = ConditionalScans()







    async def volume_analysis_screener(self, ticker:str):

        """
        Scan for fire sale conditions - where the overall  volume on the day is 70% or more sell volume.


        CONDITIONS:


        >>> fire_sale: volume on the day is recorded as 70% or more sell volume.

        >>> accumulation: volume on the day is recorded as 70% or more buy volume.


        >>> neutral_zone: volume on the day is recorded as 70% or more neutral volume.
        
        
        """

        volume_analysis = await self.trading.volume_analysis(ticker)
        data_dict = getattr(volume_analysis, 'data_dict', None)
        print(ticker)
        if volume_analysis is not None:
            if data_dict is not None:
                data_dict.update({'ticker': ticker})
            if volume_analysis.buyPct >= 70:
                condition = 'accumulation'

                await self.volume_analysis_embed(condition, self.accumulation, data_dict)
                

            
            if volume_analysis.sellPct >= 70:
                condition = 'fire_sale'
                await self.volume_analysis_embed(condition, self.fire_sale, data_dict)


            if volume_analysis.nPct >= 70:
                condition = 'neutral_zone'
                await self.volume_analysis_embed(condition,self.neutral_zone, data_dict)


    async def run_markets(self):
        all_data = await self.poly.get_all_tickers()

        min_opens = all_data.min_o
        min_closes = all_data.min_c
        tickers = all_data.ticker

        for ticker, open_value, close_value in zip(tickers, min_opens, min_closes):
            # Calculate the percentage difference
            if open_value and close_value:  # Ensure neither value is None or zero to avoid division errors
                percentage_difference = abs(close_value - open_value) / open_value * 100

                # Check if the percentage difference is 1% or more
                if percentage_difference >= 1:
                    print(f"{ticker}'s MINUTE Open ({open_value}) and Close ({close_value}) are more than 1% apart!")

                    hook = AsyncDiscordWebhook(os.environ.get('main_chat'), content=f"{ticker}'s MINUTE Open ({open_value}) and Close ({close_value}) are more than 1% apart!")

                    await hook.execute()

    async def refresh_options(self, ticker):
        data = await self.opts.get_option_chain_all(underlying_asset=ticker)
        headers = [
            'strike', 'expiry', 'dte', 'time_value', 'moneyness', 'liquidity_score', 'cp', 
            'exercise_style', 'option_symbol', 'theta', 'theta_decay_rate', 'delta', 
            'delta_theta_ratio', 'gamma', 'gamma_risk', 'vega', 'vega_impact', 'timestamp', 
            'oi', 'open', 'high', 'low', 'close', 'intrinstic_value', 'extrinsic_value', 
            'leverage_ratio', 'vwap', 'conditions', 'price', 'trade_size', 'exchange', 'ask', 
            'bid', 'spread', 'spread_pct', 'iv', 'bid_size', 'ask_size', 'vol', 'mid', 
            'change_to_breakeven', 'underlying_price', 'ticker', 'return_on_risk', 'velocity', 
            'sensitivity', 'greeks_balance', 'opp'
        ]

        if data is not None:
            # Open a CSV file to write data
            with open('all_options_data.csv', mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(headers)

                # Write each row to the CSV file within the 'with' block
                for i,row in data.df.iterrows():
                    writer.writerow(row)
                    

market = ReferenceMarket()
async def refresh_markets():
    # Initialize tasks with volume_analysis_screener for each ticker
    #tasks = [market.volume_analysis_screener(ticker) for ticker in most_active_tickers]
    tasks = []
    # Add run_markets task
    #tasks.append(market.run_markets())
    while True:
        # Add run_options tasks for each ticker
        tasks.extend([market.refresh_options(ticker) for ticker in most_active_tickers])
        tasks.append(market.run_markets())
        # Use asyncio.gather to run all tasks concurrently
        await asyncio.gather(*tasks)

### ANALYZE

import pandas as pd
from apis.helpers import human_readable
df = pd.read_csv('files/all_option_data.csv')



# Randomize the DataFrame
df = df.sample(frac=1).reset_index(drop=True)

def process_row(row):
    # Your processing logic for each row
    ask = row['ask']
    bid = row['bid']
    mid = row['mid']
    spread = row['spread']
    spread_pct = row['spread_pct']
    ask_size=  row['ask_size']
    bid_size = row['bid_size']


    theta = row['theta']
    theta_decay_rate = row['theta_decay_rate']
    vega = row['vega']
    vega_impact = row['vega_impact']
    gamma = row['gamma']
    gamma_risk = row['gamma_risk']
    delta = row['delta']
    delta_theta_ratio = row['delta_theta_ratio']

    intrinsic_value = row['intrinstic_value']
    extrinsic_value = row['extrinsic_value']
    time_value = row['time_value']

    vol = row['vol']
    oi = row['oi']
    iv = row['iv']
    liquidity_score = row['liquidity_score']


    change_ratio = row['change_ratio']
    velocity = row['velocity']



    open = row['open']
    high = row['high']
    low = row['low']
    close = row['close']
    vwap = row['vwap']






    dte = row['dte']
    price = row['underlying_price']
    # Assuming human_readable is a function you've defined
    symbol = human_readable(row['option_symbol'])
    strike = row['strike']
    cp = row['cp']
    expiry = row['expiry']
    
    
    results = market.conditions.refined_options_analysis(row)
    if results is not None:
        if results['theta_risk'] == 'Low':
            print(row)    
    
async def process_data_asynchronously(executor, df):
    loop = asyncio.get_running_loop()

    for _, row in df.iterrows():
        # Schedule each row to be processed individually
        await loop.run_in_executor(executor, process_row, row)

async def main():
    with ThreadPoolExecutor() as executor:
        await process_data_asynchronously(executor, df)

# Run the async function with multi-threading
asyncio.run(main())