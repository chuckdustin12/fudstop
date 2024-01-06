import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
from dotenv import load_dotenv
load_dotenv()
from discord_webhook import AsyncDiscordWebhook
from discord_embeds.embeddings import Embeddings
from fudstop.apis.webull.webull_trading import WebullTrading as AsyncWebullSDK
webull = AsyncWebullSDK()
import time
import aiohttp
import asyncio
import pandas as pd
from apis.helpers import calculate_countdown, calculate_setup
from aiohttp.client_exceptions import ClientConnectorError, ClientOSError
import aiohttp
from asyncio import Semaphore
from webull_options.webull_options import WebullOptions
from database_.database_manager import DatabaseManager
sema = Semaphore(10)
db = DatabaseManager(host='localhost', port=5432, user='chuck', password='fud', database='markets')
wb_opts = WebullOptions(user='chuck', database='markets')
fudstop_logo = os.environ.get('fudstop')
min1_td9 = os.environ.get("min1_td9")
min5_td9 = os.environ.get("min5_td9")
min15_td9 = os.environ.get("min15_td9")
min20_td9 = os.environ.get("min20_td9")
min30_td9 = os.environ.get("min30_td9")
min60_td9 = os.environ.get("min60_td9")
min120_td9 = os.environ.get("min120_td9")
min240_td9 = os.environ.get("min240_td9")
day_td9 = os.environ.get("day_td9")
min2_td9 = os.environ.get("min2_td9")
min3_td9 = os.environ.get("min3_td9")
min10_td9 = os.environ.get("min10_td9")
week_td9 = os.environ.get("week_td9")
month_td9 = os.environ.get("month_td9")
embeds = Embeddings()




async def get_bars(ticker, conn, interval:str='m1', timeStamp=None):
    if ticker == 'I:SPX':
        ticker = 'SPXW'
    elif ticker =='I:NDX':
        ticker = 'NDX'
    elif ticker =='I:VIX':
        ticker = 'VIX'
    
    tickerid = await webull.get_ticker_id(ticker)




    if timeStamp is None:
        # if not set, default to current time
        timeStamp = int(time.time())

    base_fintech_gw_url = f'https://quotes-gw.webullfintech.com/api/quote/charts/query?tickerIds={tickerid}&type={interval}&count=500&timestamp={timeStamp}&extendTrading=1'
    print(base_fintech_gw_url)
    async with sema:
        async with aiohttp.ClientSession() as session:
            if interval == 'm1':
                timespan = 'minute'
            elif interval == 'm60':
                timespan = '1 hour'
            elif interval == 'm5':
                timespan = '5 minute'
            elif interval == 'm15':
                timespan = '15 minute'
            elif interval == 'm30':
                timespan = '30 minute'
            elif interval == 'm60':
                timespan = '1 hour'
            elif interval == 'm120':
                timespan = '2 hour'
            elif interval == 'm240':
                timespan = '4 hour'
            elif interval == 'd1':
                timespan = 'day'
            elif interval == 'w':
                timespan = 'weekly'
            elif interval == 'm':
                timespan = 'monthly'
            async with session.get(base_fintech_gw_url, headers=wb_opts.headers) as resp:

                r = await resp.json()

                try:
                    data = r[0]['data']
                    if data is not None:
                        try:
                            parsed_data = []
                            for entry in data:
                                values = entry.split(',')
                                if values[-1] == 'NULL':
                                    values = values[:-1]
                                elif values[-1] == 'NULL':
                                    values = values[:-1]  # remove the last element if it's 'NULL'
                                parsed_data.append([float(value) if value != 'null' else 0.0 for value in values])
                            
                            sorted_data = sorted(parsed_data, key=lambda x: x[0], reverse=True)
                            
                            # Dynamically assign columns based on the length of the first entry
                            columns = ['Timestamp', 'Open', 'Close', 'High', 'Low', 'N', 'Volume', 'Vwap'][:len(sorted_data[0])]
                            
                            df = pd.DataFrame(sorted_data, columns=columns)
                            # Convert the Unix timestamps to datetime objects in UTC first
                            df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s', utc=True)

                            # Convert UTC to Eastern Time (ET)
                            df['Timestamp'] = df['Timestamp'].dt.tz_convert('US/Eastern')
                            df['Timestamp'] = df['Timestamp'].dt.tz_localize(None)
                            df['Ticker'] = ticker
                            df['timespan'] = interval
        

                            df['ticker'] = ticker

                            
                            td9_df = df.head(13)

                            setup_phase = calculate_setup(td9_df)
                            countdown_phase = calculate_countdown(td9_df)

                            df = df.head(13)
                            await wb_opts.db_manager.create_table(df, table_name='td9', unique_column='ticker, td9_state, timespan')
    
                            td9_state = "Setup Complete" if setup_phase else "Countdown Complete" if countdown_phase else "Not in TD9 State"


                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'm1':
                                #await plot_td9_chart(df, interval) if df is not None else ...
                    
                                hook = AsyncDiscordWebhook(min1_td9, content=f'{ticker} {td9_state}')
                                await embeds.send_td9_embed(interval, min1_td9, ticker, td9_state, conn)
                                # Assuming you have a connection pool or a single connection to your database
                                
          


                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'm5':
                                asyncio.create_task(embeds.send_td9_embed(interval, min5_td9, ticker, td9_state, conn))
      
                                # Insert data into the market_data table
               


                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'm15':


                                asyncio.create_task(embeds.send_td9_embed(interval, min15_td9, ticker, td9_state, conn))

                   
                                
                                # Insert data into the market_data table
                                
          

                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'm30':

                                asyncio.create_task(embeds.send_td9_embed(interval, min30_td9, ticker, td9_state, conn))
    
                                
      
                                
                         

                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'm60':
  
                                asyncio.create_task(embeds.send_td9_embed(interval, min60_td9, ticker, td9_state, conn))

                          
                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'm120':

                               
                                asyncio.create_task(embeds.send_td9_embed(interval, min120_td9, ticker, td9_state, conn))



                    
                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'm240':

                       
                                asyncio.create_task(embeds.send_td9_embed(interval, min240_td9, ticker, td9_state, conn))

                                
          
   
                     
                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'd1':

                        
                                asyncio.create_task(embeds.send_td9_embed(interval, day_td9, ticker, td9_state, conn))
  
            
                    
                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'w':

                                asyncio.create_task(embeds.send_td9_embed(interval, week_td9, ticker, td9_state, conn))
                                # Assuming you have a connection pool or a single connection to your database
             
         
                 
                            if td9_state in ('Setup Complete', 'Countdown Complete') and interval == 'm':

                                asyncio.create_task(embeds.send_td9_embed(interval, month_td9, ticker, td9_state, conn))
                                # Assuming you have a connection pool or a single connection to your database
 
                    
                        except (KeyError, ValueError, ClientConnectorError, OSError, ClientOSError, IndexError):
                            print(f'Error - {ticker}')
                except (KeyError, IndexError, ClientOSError, OSError):
                    print(f'Td9 error - {ticker}')




async def run_td9():

    conn = await db.connect()
    while True:
        
        try:
            while True:
                intervals = ['m1', 'm5','m15', 'm30', 'd1', 'm60', 'm120', 'm240', 'w', 'm']
                tasks = [get_bars(ticker, interval=interval, timeStamp=None, conn=conn) for ticker in wb_opts.most_active_tickers for interval in intervals]
                await asyncio.gather(*tasks)
        except Exception as e:
            print(e)


asyncio.run(run_td9())