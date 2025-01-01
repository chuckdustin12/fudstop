import sys
from pathlib import Path
import random
import json
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
from dotenv import load_dotenv
load_dotenv()
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
from polygon.websocket import WebSocketClient
from embeddings import sector_dark_pool_embed, create_stock_embed
from polygon.websocket.models import WebSocketMessage
from typing import List
import asyncio
from fudstop.apis.polygonio.mapping import STOCK_EXCHANGES, stock_condition_dict
from _markets.list_sets.dicts import energy,utilities,basic_materials,industrials,healthcare,financial_services,communication_services,consumer_cyclical,consumer_defensive,hex_color_dict,etfs,real_estate,technology
from apis.polygonio.polygon_database import PolygonDatabase
from apis.polygonio.async_polygon_sdk import Polygon
from polygon.websocket import EquityAgg,EquityQuote,EquityTrade
from functools import partial
from datetime import datetime
import pandas as pd
c = WebSocketClient(subscriptions=["T.*,A.*"], api_key=os.environ.get('YOUR_POLYGON_KEY'))


class StockMarket:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.database = PolygonDatabase(user='chuck', database='market_data', host='localhost', password='fud', port=5432)
        self.poly = Polygon(user='chuck', host='localhost', port=5432, database='market_data', password='fud')
        self.te_tickers = technology
        self.he_tickers = healthcare
        self.fs_tickers = financial_services
        self.cs_tickers = communication_services
        self.cd_tickers = consumer_defensive
        self.cc_tickers = consumer_cyclical
        self.in_tickers = industrials
        self.bm_tickers = basic_materials
        self.en_tickers = energy
        self.ut_tickers = utilities
        self.re_tickers = real_estate
        self.colors = hex_color_dict
        self.etf_tickers = etfs

        self.sector_webhooks = { 
            'cc': os.environ.get('cc'),
            'fs': os.environ.get('fs'),
            'cd': os.environ.get('cd'),
            'cs': os.environ.get('cs'),
            'he': os.environ.get('he'),
            'bm': os.environ.get('bm'),
            'in': os.environ.get('in'),
            'etf': os.environ.get('etf'),
            'ut': os.environ.get('ut'),
            'en': os.environ.get('en'),
            'te': os.environ.get('te'),
            're': os.environ.get('re'),

        }

        self.timespans = ['day', 'week', 'minute', 'hour', 'month']

    async def get_logo(self, ticker):
        return await self.poly.get_polygon_logo(ticker)

    async def stock_rsi(self, ticker, sector):
        if sector == 'cc':
            sector = 'Consumer Cyclical'
        elif sector == 'cd':
            sector = 'Consumer Defensive'
        elif sector == 'cs':
            sector = 'Communication Services'
        elif sector == 'fs':
            sector = 'Financial Services'
        elif sector == 'te':
            sector = 'Technology'
        elif sector == 'etf':
            sector = 'ETFs'
        elif sector == 'bm':
            sector = 'Basic Materials'
        elif sector == 'in':
            sector = 'Industrials'
        elif sector == 'he':
            sector = 'Healthcare'
        elif sector == 'en':
            sector = 'Energy'
        elif sector == 'ut':
            sector = 'Utilities'




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
            status = 'oversold' if color == hex_color_dict['green'] else 'overbought' if color == hex_color_dict['red'] else None
            if status is not None:
                embed = DiscordEmbed(title=f"RSI Results - {timespan}", description=f"```py\n{ticker} is {status} on the {timespan} with an RSI value of {latest_rsi}.```", color=color)
                embed.set_timestamp()
                embed.add_embed_field(name=f"Sector:", value=f"# > {sector}")
                embed.set_footer(f'overbought / oversold RSI feed - {timespan}')
                hook.add_embed(embed)
                asyncio.create_task(hook.execute())


            data_dict = { 
                'ticker': ticker,
                'timespan': timespan,
                'rsi': latest_rsi,
                'status': status,
                'sector': sector
                

            }

            df = pd.DataFrame(data_dict, index=[0])

            asyncio.create_task(self.database.batch_insert_dataframe(df, table_name='rsi', unique_columns='ticker, timespan, status'))
    async def get_sector(self, ticker):
        
        if ticker in energy:
            return 'en'
        elif ticker in utilities:
            return 'ut'
        elif ticker in basic_materials:
            return 'bm'
        elif ticker in industrials:
            return 'in'
        elif ticker in healthcare:
            return 'he'
        elif ticker in financial_services:
            return 'fs'
        elif ticker in communication_services:
            return 'cs'
        elif ticker in consumer_cyclical:
            return 'cc'
        elif ticker in consumer_defensive:
            return 'cd'
        elif ticker in real_estate:
            return 're'
        elif ticker in technology:
            return 'te'
        elif ticker in etfs:
            return 'etf'
        else:
            return f'Unknown for {ticker}'
    async def handle_msg(self, msgs: WebSocketMessage, data_queue=asyncio.Queue):
        for m in msgs:
            sector = await self.get_sector(m.symbol)
            if isinstance(m, EquityAgg):
                data = {
                    'type': 'EquityAgg',
                    'ticker': m.symbol,
                    'close_price': m.close,
                    'high_price': m.high,
                    'low_price': m.low,
                    'open_price': m.open,
                    'volume': m.volume,
                    'official_open': m.official_open_price,
                    'accumulated_volume': m.accumulated_volume,
                    'vwap_price': m.vwap,
                    'agg_timestamp': datetime.fromtimestamp(m.end_timestamp / 1000.0) if m.end_timestamp is not None else None,
                    'sector': sector
                }


                data = {
                    'type': 'EquityAgg',
                    'ticker': m.symbol,
                    'close_price': m.close,
                    'high_price': m.high,
                    'low_price': m.low,
                    'open_price': m.open,
                    'volume': m.volume,
                    'official_open': m.official_open_price,
                    'accumulated_volume': m.accumulated_volume,
                    'vwap_price': m.vwap,
                    'agg_timestamp': datetime.fromtimestamp(m.end_timestamp / 1000.0) if m.end_timestamp is not None else None,
                    'sector': sector
                }
                data['change_percent'] = ((data['close_price'] - data['open_price']) / data['open_price'] * 100) if data['open_price'] else 0
                data['price_spread'] = data['high_price'] - data['low_price']
                data['volume_change'] = data['volume'] - data['accumulated_volume']
                data['relative_vwap'] = data['vwap_price'] - data['close_price']
                data['agg_timestamp'] = pd.to_datetime(data['agg_timestamp'])
                df = pd.DataFrame(data, index=[0])
                asyncio.create_task(data_queue.put(data))
                asyncio.create_task(self.database.batch_insert_dataframe(df, table_name='stock_aggs', unique_columns='insertion_timestamp'))

                


            elif isinstance(m, EquityTrade):

                data = { 
                    'type': 'EquityTrade',
                    'ticker': m.symbol,
                    'trade_exchange': STOCK_EXCHANGES.get(m.exchange),
                    'trade_price': m.price,
                    'trade_size': m.size,
                    'trade_conditions': [stock_condition_dict.get(condition) for condition in m.conditions] if m.conditions is not None else [],
                    'trade_timestamp': datetime.fromtimestamp(m.timestamp / 1000.0) if m.timestamp is not None else None,
                    'sector': sector
                }
                # Flatten the 'trade_conditions' list into a string
                trade_conditions_str = ', '.join(filter(None, data['trade_conditions'])) if data['trade_conditions'] is not None else ''
                data['trade_conditions'] = trade_conditions_str
                data['dollar_cost'] = data['trade_price'] * data['trade_size'] if data['trade_price'] is not None and data['trade_size'] is not None else None
                data['volume_indicator'] = "High" if data['trade_size'] is not None and data['trade_size'] > 1000 else "Medium" if data['trade_size'] > 100 else "Low"
                data['timestamp_hour'] = data['trade_timestamp'].hour if data.get('trade_timestamp') else None
                data['price_range'] = data.get('high_price', 0) - data.get('low_price', 0)
                data['trade_timestamp'] = pd.to_datetime(data['trade_timestamp'])
                df = pd.DataFrame(data, index=[0])
                asyncio.create_task(data_queue.put(data))

                asyncio.create_task(self.database.batch_insert_dataframe(df, table_name='stock_trades', unique_columns='insertion_timestamp'))

                
market = StockMarket()



async def consumer(data_queue):
    while True:
        data = await data_queue.get()

        if data.get('type') == 'EquityTrade':
            ticker = data.get('ticker')
            exchange = data.get('trade_exchange')
            price = data.get('trade_price')
            size = data.get('trade_size')
            dollar_cost = data.get('dollar_cost')
            conditions = data.get('trade_conditions')
            volume_indicator = data.get('volume_indicator')
            trade_timestamp = data.get('trade_timestamp')
            sector = data.get('sector')
            
            
            if ticker in market.database.most_active_tickers:
                sector = await market.get_sector(sector)


                asyncio.create_task(market.stock_rsi(ticker=ticker, sector=sector))
                webhook_url = market.sector_webhooks.get(sector)
            
                if webhook_url is not None and dollar_cost >= 150000 and 'FINRA' not in exchange:
                    
                    hook = AsyncDiscordWebhook(webhook_url, content=f"<@375862240601047070>")
                    embed = DiscordEmbed(title=f"Sector Feed - {sector} | {ticker}", description=f"```py\n> Size: **{size}**\n> Price: **${price}**\n> Dollar Cost: **${round(float(dollar_cost),2)}**```", color=market.colors.get('blue'))
                    embed.add_embed_field(name=f"Conditions:", value=f"> **{conditions}**")
                    embed.add_embed_field(name=f"Exchange:", value=f"> **{exchange}**")
                    embed.add_embed_field(name=f"Volume Indicator:", value=f"> **{volume_indicator}**")
                    embed.set_thumbnail(logo)
                    embed.set_timestamp()
                    embed.set_footer(text=f'{ticker} | {trade_timestamp}', icon_url=os.environ.get('fudstop_logo'))

                    hook.add_embed(embed)
                    await hook.execute()

                if webhook_url is not None and dollar_cost >= 1000000 and 'FINRA' in exchange:
                    logo = await market.get_logo(ticker)
                    asyncio.create_task(sector_dark_pool_embed(logo, ticker, webhook_url, sector, dollar_cost, conditions, exchange, size, price, volume_indicator, trade_timestamp))

                    df = pd.DataFrame(data, index=[0]) 

                    asyncio.create_task(market.database.batch_insert_dataframe(df, 'live_dps', unique_columns='insertion_timestamp'))














async def main():
    data_queue = asyncio.Queue()
    
    await market.database.connect()
    # Use partial to pass data_queue as an argument to handle_msg
    handle_msg_with_queue = partial(market.handle_msg, data_queue=data_queue)

    # Correctly using asyncio.gather with the modified handle_msg function
    await asyncio.gather(c.connect(handle_msg_with_queue), consumer(data_queue))

# Running the main function
if __name__ == "__main__":
    asyncio.run(main())