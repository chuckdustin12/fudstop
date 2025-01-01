import sys
from pathlib import Path

import json
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
import random
from asyncpg.exceptions import UniqueViolationError
from aiohttp.client_exceptions import ContentTypeError
from collections import deque
from dotenv import load_dotenv
from embeddings import vol_anal_embed, create_newhigh_embed, profit_ratio_02_embed, profit_ratio_98_embed, option_condition_embed, sized_trade_embed, conditional_embed
from _markets.list_sets.dicts import hex_color_dict
from apis.discord_.discord_sdk import DiscordSDK
from apis.helpers import identify_sector
from _markets.list_sets.ticker_lists import most_active_tickers
from _markets.list_sets.dicts import healthcare, etfs, basic_materials, industrials, communication_services,consumer_cyclical,consumer_defensive,financial_services,energy,real_estate,utilities, technology
from analyzers import OptionDataAnalyzer
from market_handlers.database_ import MarketDBManager
from datetime import datetime, timedelta
from _markets.market_handlers.list_sets import indices_names_and_symbols_dict, CRYPTO_DESCRIPTIONS,CRYPTO_HOOKS
load_dotenv()
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
from monitor import EquityOptionTradeMonitor
from polygon.websocket import WebSocketClient, Market
from polygon.websocket.models import WebSocketMessage, EquityAgg,EquityQuote,EquityTrade,IndexValue
from fudstop.apis.polygonio.mapping import option_condition_desc_dict,option_condition_dict,OPTIONS_EXCHANGES,stock_condition_desc_dict,stock_condition_dict,indicators,quote_conditions,STOCK_EXCHANGES
from list_sets.dicts import all_forex_pairs, crypto_currency_pairs
from apis.polygonio.technicals import Technicals
from apis.webull.webull_trading import WebullTrading
from apis.polygonio.polygon_database import PolygonDatabase
# Create a reverse dictionary
all_forex_pairs = {v: k for k, v in all_forex_pairs.items()}
from typing import List
import asyncio
import time
from asyncio import Queue
import pandas as pd
import logging
from __init__ import Inserts
from _markets.webhook_dicts import option_conditions_hooks
from apis.polygonio.async_polygon_sdk import Polygon
from apis.polygonio.polygon_options import PolygonOptions
class MultiMarkets(Inserts):
    def __init__(self, user, database, port, host, password):
        self.poly = Polygon(host='localhost', user='chuck', database='market_data', password='fud', port=5432)
        self.discord = DiscordSDK()
        self.feed_db = PolygonDatabase(host='localhost', user='chuck', port=5432, password='fud', database='market_data')
        self.technicals = Technicals()
        self.db = MarketDBManager(user=user,database=database,port=port,host=host,password=password)
        self.markets = [Market.Stocks, Market.Options,  Market.Forex] #Market.Forex]# Market.Indices]
        self.subscription_patterns = {
            Market.Options: ["T.*,A.*"],
            Market.Stocks: ["A.*,T.*"],
           # Market.Indices: ["A.*"],
            #Market.Crypto: ['XT.*'],
            Market.Forex: ['CAS.*']

        }
        self.ticker_cache = {}
        self.trading = WebullTrading()
        self.time_day = 'day'
        self.time_hour = 'hour'
        self.time_minute = 'minute'
        self.time_week = 'week'
        self.time_month='month'
        self.queue = asyncio.Queue()
        self.analyzer = OptionDataAnalyzer()
        self.ticker_queue = asyncio.Queue()
        self.created_channels = set()  # A set to keep track of created channels
        self.last_ticker = None
        self.consecutive_count = 0
        self.indices_names=indices_names_and_symbols_dict
        self.agg_tickers = deque(maxlen=25)
        self.trade_tickers = deque(maxlen=250)
        self.opts = PolygonOptions(user='chuck', database='market_data', host='localhost', port=5432, password='fud')
        self.timespans = ['minute', 'hour', 'day', 'week', 'month']
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')    
    # Function to check if the stock should be processed
    async def should_process_stock(self, ticker):
        current_time = time.time()
        if ticker in self.ticker_cache and current_time - self.ticker_cache[ticker] < 60:
            return False
        self.ticker_cache[ticker] = current_time
        return True
    async def send_and_execute_webhook(self, hook: AsyncDiscordWebhook, embed: DiscordEmbed):
        hook.add_embed(embed)
        await hook.execute()

    async def create_channel_if_not_exists(self, ticker, name):
        # Check if the channel already exists
        if ticker not in self.created_channels:
            # If not, create the channel and add its name to the set
            await self.discord.create_channel(name=ticker, channel_description=name)
            self.created_channels.add(ticker)



    async def stock_rsi(self, ticker):
        
        


        timespan=random.choice(self.timespans)
        rsi_min_task = asyncio.create_task(self.poly.rsi(ticker=ticker, timespan=random.choice(self.timespans)))

        rsi = await asyncio.gather(rsi_min_task)
        rsi = rsi[0]
        latest_rsi = None
        if rsi is not None and rsi.rsi_value is not None:
            latest_rsi = rsi.rsi_value[0]


        if timespan == 'hour':
            hook = AsyncDiscordWebhook(os.environ.get('osob_hour'))

        if timespan == 'day':
            hook = AsyncDiscordWebhook(os.environ.get('osob_day'))


        if timespan == 'minute':
            hook = AsyncDiscordWebhook(os.environ.get('osob_minute'))

        if timespan == 'week':
            hook = AsyncDiscordWebhook(os.environ.get('osob_week'))

        if timespan == 'month':
            hook = AsyncDiscordWebhook(os.environ.get('osob_month'))

        
        if timespan is not None and hook is not None and latest_rsi is not None:

            color = hex_color_dict['green'] if latest_rsi <=30 else hex_color_dict['red'] if latest_rsi >= 70 else hex_color_dict['grey']
            status = f'oversold {timespan}' if color == hex_color_dict['green'] else f'overbought {timespan}' if color == hex_color_dict['red'] else None
            
            description=f"```py\n{ticker} is {status} on the {timespan} with an RSI value of {latest_rsi}.```"
            
            if status is not None:
                sector = await identify_sector(ticker)
                embed = DiscordEmbed(title=f"RSI Results - ALL {timespan}", description=f"```py\n{ticker} is {status} on the {timespan} with an RSI value of {latest_rsi}.```", color=color)
                embed.set_timestamp()
                embed.set_footer(f'overbought / oversold RSI feed - {timespan}')
                hook.add_embed(embed)
                asyncio.create_task(hook.execute())


                data_dict = { 
                    'ticker':ticker,
                    'sector': sector,
                    'status': status,
                    'description': description,
                }


                df = pd.DataFrame(data_dict, index=[0])
                asyncio.create_task(self.db.batch_insert_dataframe(df, 'feeds', 'ticker, status, description'))






    async def stock_macd(self, ticker):
       
        macd_m= asyncio.create_task(self.technicals.run_technical_scanner(ticker, self.time_minute))
        macd_d= asyncio.create_task(self.technicals.run_technical_scanner(ticker, self.time_day))
        macd_h= asyncio.create_task(self.technicals.run_technical_scanner(ticker, self.time_hour))
        macd_w= asyncio.create_task(self.technicals.run_technical_scanner(ticker, self.time_week))
        macd_mth= asyncio.create_task(self.technicals.run_technical_scanner(ticker, self.time_month))
        

        await asyncio.gather(macd_m, macd_d, macd_h, macd_w, macd_mth)



    async def crypto_conditions(self, dollar_cost, symbol, exchange, conditions, timestamp, size, price, color):


        if symbol in CRYPTO_HOOKS and dollar_cost >= 100:
            hook = CRYPTO_HOOKS[symbol]
            desc = CRYPTO_DESCRIPTIONS[symbol]

            
            webhook = AsyncDiscordWebhook(hook, content="<@375862240601047070>")
            embed = DiscordEmbed(title=f"{symbol} | Live Trades", description=f"```py\n{desc}```", color=hex_color_dict[color])
            embed.add_embed_field(name=f"Exchange:", value=f"> **{exchange}**")
            embed.add_embed_field(name=f"Side:", value=f"> **{conditions}**")
            embed.add_embed_field(name="Trade Info:", value=f"> Price: **${price}**\n> Size: **{size}**")
            embed.add_embed_field(name=f"Time:", value=f"> **{timestamp}**")
            embed.add_embed_field(name=f'Dollar Cost', value=f"# > **{dollar_cost}**")
            embed.set_footer(text=f"{symbol} | {conditions} | {dollar_cost} | {timestamp} | 10k+ cost | Data by polygon.io", icon_url=os.environ.get('fudstop_logo'))
            embed.set_timestamp()

            webhook.add_embed(embed)

            await webhook.execute()


        if symbol in CRYPTO_HOOKS and dollar_cost is not None and dollar_cost >= 10000 and conditions == 'Buy Side':
            hook = os.environ.get('crypto_10k_buys')
            desc = CRYPTO_DESCRIPTIONS[symbol]
            data_dict = { 
                'type': '10k buys',
                'dollar_cost': dollar_cost,
                'ticker': symbol,
                'description': desc,
                'exchange': exchange,
                'conditions': conditions,
                'timestamp': timestamp,
                'size': size,
                'price': price,
                'color': color
            }
            webhook = AsyncDiscordWebhook(hook, content="<@375862240601047070>")
            embed = DiscordEmbed(title=f"{symbol} | Live Trades", description=f"```py\n{desc}```", color=hex_color_dict['green'])
            embed.add_embed_field(name=f"Exchange:", value=f"> **{exchange}**")
            embed.add_embed_field(name=f"Side:", value=f"> **{conditions}**")
            embed.add_embed_field(name="Trade Info:", value=f"> Price: **${price}**\n> Size: **{size}**")
            embed.add_embed_field(name=f"Time:", value=f"> **{timestamp}**")
            embed.add_embed_field(name=f"Dollar Cost:", value=f"> **${dollar_cost}**")

            embed.set_footer(text=f"{symbol} | {conditions} | {round(float(dollar_cost),2)} | {timestamp} | 10k+ cost | Data by polygon.io", icon_url=os.environ.get('fudstop_logo'))
            embed.set_timestamp()

            webhook.add_embed(embed)
            asyncio.create_task(webhook.execute())
            asyncio.create_task(self.db.batch_insert_dataframe(df, table_name='large_crypto', unique_columns='insertion_timestamp'))

        if symbol in CRYPTO_HOOKS and dollar_cost is not None and dollar_cost >= 10000 and conditions == 'Sell Side':
            hook=os.environ.get('crypto_10k_sells')
     
            desc = CRYPTO_DESCRIPTIONS[symbol]
            data_dict = { 
                'type': '10k sells',
                'dollar_cost': dollar_cost,
                'ticker': symbol,
                'description': desc,
                'exchange': exchange,
                'conditions': conditions,
                'timestamp': timestamp,
                'size': size,
                'price': price,
                'color': color
            }
            webhook = AsyncDiscordWebhook(hook, content="<@375862240601047070>")
            embed = DiscordEmbed(title=f"{symbol} | Live Trades", description=f"```py\n{desc}```", color=hex_color_dict['red'])
            embed.add_embed_field(name=f"Exchange:", value=f"> **{exchange}**")
            embed.add_embed_field(name=f"Side:", value=f"> **{conditions}**")
            embed.add_embed_field(name="Trade Info:", value=f"> Price: **${price}**\n> Size: **{size}**")
            embed.add_embed_field(name=f"Time:", value=f"> **{timestamp}**")
            embed.add_embed_field(name=f"Dollar Cost:", value=f"> **${dollar_cost}**")

            embed.set_footer(text=f"{symbol} | {conditions} | {round(float(dollar_cost),2)} | {timestamp} | 10k+ cost | Data by polygon.io", icon_url=os.environ.get('fudstop_logo'))
            embed.set_timestamp()

            webhook.add_embed(embed)

            asyncio.create_task(webhook.execute())

            df = pd.DataFrame(data_dict)
            asyncio.create_task(self.db.batch_insert_dataframe(df, table_name='large_crypto', unique_columns='insertion_timestamp'))


    
    async def process_batches(self, tickers_string: asyncio.Queue):
        


        data= await self.opts.multi_snapshot(tickers_string)




        df = data.as_dataframe.rename(columns={'intrinstic_value': 'intrinsic_value'})
        

        asyncio.create_task(self.db.batch_insert_dataframe(data.as_dataframe, table_name='poly_opts', unique_columns='insertion_timestamp', batch_size=1))

        df = data.as_dataframe
        for i,row in df.iterrows():
            yield row




    # Function to handle incoming WebSocket messages
            
    async def handle_msg(self, msgs: WebSocketMessage):

        
        monitor = EquityOptionTradeMonitor()
        
        for m in msgs:
         
            event_type = m.event_type



            if event_type == 'A' and m.symbol.startswith('O:'):
                
                async for data in self.db.insert_option_aggs(m):
                    ticker = data.get('option_symbol')
                    symbol = data.get('ticker')
                    self.agg_tickers.append(m.symbol)  # Append to the instance's deque

                    # When we have 250 tickers, process them
                    if len(self.agg_tickers) == 25:
                        
                        tickers_string = ','.join(self.agg_tickers)
                        
                        # Process the tickers_string as needed
                    

                        self.agg_tickers.clear()
                        try:
                            async for row in self.process_batches(tickers_string):
                                processed_data = set()  # Initialize a set to store processed data points
                                oi_value = row['oi']
                                underlying_symbol_value = row['ticker']
                                underlying_price_value = row['underlying_price']
                                strike_value = row['strike']
                                change_percent_value = row['change_percent']
                                ask_value = row['ask']
                                bid_value = row['bid']
                                print(oi_value)
                                


                                if row['cp'] == 'call':
                                    color = hex_color_dict.get('green')
                                else:
                                    color = hex_color_dict.get('red')

                                    
                                if oi_value >= 100000:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('oi_100kplus'), row=row, color=color, description='```py\nThis feed represents options trading with open interest with 100,000 or more.```', status='OI 5k-10k', sector=sector))




                            
                                if oi_value >= 5000 and oi_value <= 9999:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('oi_5k10k'), row=row, color=color, description='```py\nThis feed represents options trading with open interest between 5,000 and 10,000```', status='OI 5k-10k', sector=sector))
                                    


                                if oi_value >= 10000 and oi_value <= 49999:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('oi_10k50k'), row=row, color=color, description='```py\nThis feed represents options trading with open interest between 10,000 and 50,000```', status='OI 10k-50k', sector=sector))
                                



                                if oi_value >= 50000 and oi_value <= 99999:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('oi_50k100k'), row=row, color=color, description='```py\nThis feed represents options trading with open interest between 50,000 and 100,000```', status='OI 50k-100k', sector=sector))
                                    
            
                      


                                if row['theta'] is not None and row['theta'] <= -0.01 and row['theta'] >= -0.03 and ask_value >= 0.18 and ask_value <= 1.50:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('theta_resistant'), row=row, color=color, description='```py\nThese feeds represent THETA RESISTANT options, or option trades that have theta values between -0.01 and -0.03, or lose between $1 and $3 per day.```', status='Theta Resistant', sector=sector))



                                if row['ticker'] in ['SPY', 'IWM', 'QQQ', 'TQQQ', 'SQQQ'] and row['vol'] >= 5000:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('index_surge'), row=row, color=color, description='```py\nThese feeds represent INDEX SURGES, or option trades with 5k+ volume that occur on index tickers such as SPY, QQQ, IWM, etc.```', status='Index Surge', sector=sector))


                                if row['iv_percentile'] is not None and row['iv_percentile'] <= 0.25 and row['theta'] is not None and row['theta'] <= -0.01 and row['theta'] >= -0.06:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('iv_percentile'), row=row, color=color, description='```py\nThese feeds represent LOW IV PERCENTILE, meaning IV is low relative to the historical measurement over the last year. Also - Theta values will range between -0.01 and -0.06 in these feeds.```', status='IV Percentile', sector=sector))


                                if row['vol'] is not None and row['vol'] >= 500 and row['vol'] <= 999:
                                    sector = await identify_sector(row['ticker'])
                                    
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('vol_5001k'), row=row, color=color, description='```py\nThis feed represents an option trade with volume between 500 and 1,000 contracts.```', status='vol 500-1k', sector=sector))
                                if row['vol'] is not None and row['vol'] >= 1000 and row['vol'] <= 9999:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('vol_1k10k'), row=row, color=color, description='```py\nThis feed represents an option trade with volume between 1,000 and 10,000 contracts.```', status='vol 1k-10k', sector=sector))
                                if row['vol'] is not None and row['vol'] >= 10000 and row['vol'] <= 49999:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('vol_10k50k'), row=row, color=color, description='```py\nThis feed represents an option trade with volume between 10,000 and 50,000 contracts.```', status='vol 10k-50k', sector=sector))
                                if row['vol'] is not None and row['vol'] >= 50000:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('vol_50kplus'), row=row, color=color, description='```py\nThis feed represents an option trade with volume of more than 50,000 contracts.```', status='vol 50k+', sector=sector))


                                if row['change_percent'] <= -65 and row['dte'] > 1:
                                    sector = await identify_sector(row['ticker'])
                                    asyncio.create_task(conditional_embed(webhook=os.environ.get('opt_change_percent'), row=row, color=hex_color_dict.get('red'), description='```py\nThis feed represents options that are down 65% or less on the day.```', status='Change% -65% or less', sector=sector))



                        except Exception as e:
                            print(e)
                            continue







            #stock aggs
            if event_type == 'A' and not m.symbol.startswith('I:') and not m.symbol.startswith("O:"):
                if m.symbol in set(most_active_tickers):
                    async for data in self.db.insert_stock_aggs(m):
                        ticker = data.get('ticker')
                        
                        await self.queue.put(data)



            #option trades
            
            if event_type == 'T' and m.symbol.startswith('O:'):
                
                async for data in self.db.insert_option_trades(m):
                    size = data.get('size')
                    symbol = data.get('ticker')
                
                    ticker = data.get('option_symbol')
                    
                    dollar_cost = data.get('dollar_cost')
                    expiry = data.get('expiry')
                    strike = data.get('strike')
                    call_put = data.get('call_put')
                    hour_of_day = data.get('hour_of_day')
                    weekday = data.get('weekday')
                    conditions = data.get('conditions')
                    price = data.get('price')
                    volume_change = data.get('volume_change')
                    price_change = data.get('price_change')
                    exchange = data.get('exchange')
                    self.trade_tickers.append(ticker)
                
                    hook = option_conditions_hooks[data.get('conditions')]
                    if data.get('size') is not None and data.get('size') >= 500 and data.get('conditions') in option_conditions_hooks:
                        
                        asyncio.create_task(option_condition_embed(price_to_strike=data.get('price_to_strike'),conditions=data.get('conditions'), option_symbol=ticker,underlying_symbol=symbol,strike=data.get('strike'),call_put=data.get('call_put'),expiry=data.get('expiry'),price = data.get('price'), size=data.get('size'),exchange = data.get('exchange'),volume_change = data.get('volume_change'),price_change=data.get('price_change'),weekday=data.get('weekday'),hour_of_day=data.get('hour_of_day'), hook=hook))


    





            #stock trades
            if event_type == 'T' and not m.symbol.startswith('O:') and not m.symbol.startswith('I:'):
                async for data in self.db.insert_stock_trades(m):

                    ticker = data.get('ticker')
                    if ticker in set(most_active_tickers):
        
                        # Call the repeated_hits method
                        last_five_trades = await monitor.repeated_hits(data)
                        embed = DiscordEmbed(title='Repeated Stock Hits', description=f"# > {ticker}", color=hex_color_dict['gold'])
                        
                        if last_five_trades:
                            # Do something with the last five trades

                            counter = 0
                            for trade in last_five_trades:
                                counter = counter + 1
                                trade_type = trade['type']
                                ticker = trade['ticker']
                                trade_exchange = trade['trade_exchange']
                                trade_price = trade['trade_price']
                                trade_size = trade['trade_size']
                                trade_conditions = trade['trade_conditions']
                                embed.add_embed_field(name=f"Trade Info | {counter}", value=f"> Exchange: **{trade_exchange}**\n> Price: **${trade_price}**\n> Size: **{trade_size}**\n> Conditions: **{trade_conditions}**")
                            
                    
                            hook = AsyncDiscordWebhook(os.environ.get('repeated_hits'))
                            embed.set_timestamp()

                            asyncio.create_task(self.send_and_execute_webhook(hook, embed))


                    
            # elif event_type == 'XL2':
            #     async for data in self.db.insert_l2_book(m):
            #         bids = m.bid_prices
            #         bids = [item for sublist in bids for item in sublist]
            #         ticker = m.pair

            #         asks = m.ask_prices
            #         asks = [item for sublist in asks for item in sublist]

                    
            #         data_dict = { 
            #             'ask': asks,
            #             'bid':bids,
            #             'ticker': ticker,
            #         }

            #         df = pd.DataFrame(data_dict)

            #         print(df)

            #         await self.db.batch_insert_dataframe(df, table_name='l2_book', unique_columns='insertion_timestamp')






            elif event_type == 'XT':
                async for data in self.db.insert_crypto_trades(m):
                    ticker = data.get('ticker')

                    # Call the repeated_hits method
                    last_five_trades = await monitor.repeated_hits(data)

                    # Check if the ticker has appeared 5 times in a row
                    if last_five_trades == 5:
                        embed = DiscordEmbed(title='Repeated Stock Hits', description=f"# > {ticker}")
                        print(f"{ticker} has appeared 5 times in a row.")
                        hook = AsyncDiscordWebhook(os.environ.get('repeated_hits'))
                        hook.add_embed(embed)
                        await hook.execute()


                    asyncio.create_task(self.crypto_conditions(data.get('dollar_cost'), data.get('ticker'), data.get('exchange'), data.get('conditions'), data.get('timestamp'),data.get('size'), data.get('price'),data.get('color')))
                    

            elif event_type == 'CAS':
                async for data in self.db.insert_forex_aggs(m=m):
                    ticker = data.get('ticker')

                    if ticker in self.db.currency_pairs and data.get('volume') >= 2:
                        random_color_name = random.choice(list(hex_color_dict.values()))
                      
                        hook = AsyncDiscordWebhook(self.db.forex_hook_dict.get(ticker))
                        embed = DiscordEmbed(title=f"Forex Trades - {data.get('ticker')} || {data.get('name')}", description=f"```py\nThis feed represents live forex trades for the pair {data.get('name')}\n{self.db.pair_to_description.get(data.get('ticker'))}```", color=random_color_name)
                        embed.add_embed_field(name=f"Stats:", value=f"> Open: **${data.get('open')}**\n> High: **${data.get('high')}**\n> Low: **${data.get('low')}**\n> Now: **${data.get('close')}**\n>")
                        embed.add_embed_field(name=f"Volume:", value=f"> **{data.get('volume')}**")
                        embed.set_timestamp()
                        embed.set_footer(text='Data by Polygon.io | Implemented by FUDSTOP', icon_url=os.environ.get('fudstop_logo'))
                        hook.add_embed(embed)

                        await hook.execute()


    async def insert_new_prices(self, ticker, type, fifty_high, price, fifty_low, timestamp):
        try:

    

            # Insert data into the market_data table
            await self.conn.execute('''
                INSERT INTO new_prices(ticker, type, fifty_high, price, fifty_low, timestamp)
                VALUES($1, $2, $3, $4, $5, $6)
                ''', ticker, type, fifty_high, price, fifty_low, timestamp)
            

        except UniqueViolationError:
            pass





market = MultiMarkets(host='localhost', user='chuck', database='market_data', port=5432, password='fud')


async def main():
    
    while True:  # Restart mechanism
        try:
            await run_main_tasks()
        except Exception as e:
            print(e)
            logging.error(f"Critical error in main loop: {e}")
            logging.info("Restarting main loop...")
            await asyncio.sleep(10)  # Pause before restarting

# Main async function to connect to all markets with their respective subscriptions
async def run_main_tasks():
    await market.db.connect()
    clients = []
    for live_market in market.markets:
        patterns = market.subscription_patterns.get(live_market, [])
        for pattern in patterns:
            client = WebSocketClient(subscriptions=[pattern], api_key=os.environ.get('YOUR_POLYGON_KEY'), market=live_market, verbose=False)
            clients.append(client.connect(market.handle_msg))

    await asyncio.gather(*clients, return_exceptions=True)  # Wait for all clients to finish



asyncio.run(main())
