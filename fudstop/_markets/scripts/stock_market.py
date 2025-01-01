import sys
from pathlib import Path

import json
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
from dotenv import load_dotenv

load_dotenv()
import random
from apis.helpers import map_stock_conditions, STOCK_EXCHANGES, stock_condition_dict, TAPES
from apis.helpers import format_large_number, convert_to_ns_datetime
from apis.polygonio.async_polygon_sdk import Polygon
from fudstop._markets.list_sets.ticker_lists import most_active_tickers
from polygon.websocket import WebSocketClient, EquityTrade  
from polygon.websocket.models import WebSocketMessage
from typing import List
from discord_webhook import DiscordEmbed, DiscordWebhook
# type: ignore
import orjson
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
from _markets.list_sets.dicts import hex_color_dict
import asyncio
import asyncpg



class StockMarketLive:
    def __init__(self):
        self.pool = None
        
        self.connection_string = os.environ.get('STOCK_MARKET')  # Use one environment variable for connection string
        self.polygon = Polygon(host='localhost', user='chuck', password='fud', database='market_data', port=5432)
        self.timespans = ['minute', 'hour', 'day', 'week', 'month']

        self.c = WebSocketClient(subscriptions=["T.*"], custom_json=orjson, api_key=os.environ.get('YOUR_POLYGON_KEY'))

    async def connect(self):
        self.pool = await asyncpg.create_pool(host='localhost', user='chuck', database='market_data', port=5432, password='fud', min_size=1, max_size=10)
        return self.pool
    async def fetch(self, query):
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query)
            return records
        


    async def fetch_latest_trade(self, ticker):
        async with self.pool.acquire() as connection:
            # The query must have a placeholder for the ticker parameter
            query = "SELECT symbol,size,price,timestamp FROM trades WHERE symbol = $1 ORDER BY timestamp DESC LIMIT 1;"
            latest_trade = await connection.fetchrow(query, ticker)
            return latest_trade
    # async def insert_trade(self, trade_data):
    #     async with self.pool.acquire() as connection:
    #         async with connection.transaction():
    #             # The SQL INSERT statement with placeholders for the actual trade data
    #             insert_query = """
    #             INSERT INTO trades (
    #                 ticker, exchange, tape, price, size,
    #                 conditions, timestamp, sequence_number, trf_id, trf_timestamp
    #             ) VALUES (
    #                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
    #             )
    #             """
    #             await connection.execute(
    #                 insert_query,
    #                 trade_data['event_type'],
    #                 trade_data['ticker'],
    #                 trade_data['exchange'],
    #                 trade_data['id'],  # Assuming this is the 'external_id'
    #                 trade_data['tape'],
    #                 trade_data['price'],
    #                 trade_data['trade_size'],
    #                 trade_data['conditions'],  # Ensure this is passed as an array of strings
    #                 trade_data['timestamp'],
    #                 trade_data['sequence_number'],
    #                 trade_data['trf_id'],
    #                 trade_data['trf_timestamp']
    #             )

    #     return self.pool

    async def handle_msg(self, msgs: WebSocketMessage):
        
        
        for message in msgs:

            if hasattr(message, 'conditions') and message.symbol in set(most_active_tickers):
                conditions = [stock_condition_dict.get(i) for i in message.conditions] if message.conditions is not None else []

                exchange = STOCK_EXCHANGES.get(message.exchange)    
                timestamp = convert_to_ns_datetime(message.timestamp)
                trf_timestamp = convert_to_ns_datetime(message.timestamp)

                tape = TAPES.get(message.tape)
                print(timestamp)


                dollar_cost = message.price * message.size


                trade_data = { 

                    'symbol': message.symbol,
                    
                    
                    'event_type': message.event_type,
                    'exchange': exchange,
                    'id': message.id,
                    'tape': tape,
                    'price': message.price,
                    'size': message.size,
                    'conditions': conditions,
                    'timestamp': timestamp,
                    
                    'sequence_number': message.sequence_number,
                    'trf_timestamp': trf_timestamp,
                    'trf_id': message.trf_id,
                    'dollar_cost': dollar_cost,
                    
                    
                }




                #await self.insert_trade(trade_data)
                timespan=random.choice(self.timespans)
                rsi_min_task = asyncio.create_task(self.polygon.rsi(ticker=message.symbol, timespan=random.choice(self.timespans)))
     
                rsi = await asyncio.gather(rsi_min_task)
                rsi = rsi[0]
                latest_rsi = None
                if rsi is not None and rsi.rsi_value is not None:
                    latest_rsi = rsi.rsi_value[0]
                    print(latest_rsi)

                if timespan == 'hour':
                    hook = AsyncDiscordWebhook(os.environ.get('hour_osob'))

                if timespan == 'day':
                    hook = AsyncDiscordWebhook(os.environ.get('day_osob'))


                if timespan == 'minute':
                    hook = AsyncDiscordWebhook(os.environ.get('minute_osob'))

                if timespan == 'week':
                    hook = AsyncDiscordWebhook(os.environ.get('week_osob'))

                if timespan == 'month':
                    hook = AsyncDiscordWebhook(os.environ.get('month_osob'))

                
                if timespan is not None and hook is not None and latest_rsi is not None:
                    color = hex_color_dict['green'] if latest_rsi <=30 else hex_color_dict['red'] if latest_rsi >= 70 else hex_color_dict['grey']
                    status = 'oversold' if color == hex_color_dict['green'] else 'overbought' if color == hex_color_dict['red'] else None
                    if status is not None:
                        try:
                            embed = DiscordEmbed(title=f"RSI Results - ALL {timespan}", description=f"```py\n{message.symbol} is {status} on the {timespan} with an RSI value of {latest_rsi}.```", color=color)
                            embed.set_timestamp()
                            embed.set_footer(f'overbought / oversold RSI feed - {timespan}')
                            hook.add_embed(embed)
                            await hook.execute()
                        except TypeError:
                            continue

market = StockMarketLive()

async def main():
    await market.connect()
    await asyncio.gather(market.c.connect(market.handle_msg))
    

if __name__ == '__main__':
    asyncio.run(main())
