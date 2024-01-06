import sys
from pathlib import Path
import asyncpg
from datetime import datetime
from decimal import Decimal
import orjson
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

import os
from dotenv import load_dotenv
load_dotenv()

from polygon.websocket import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Market, EquityAgg
from typing import List
import asyncio
from apis.helpers import get_human_readable_string




class OptionsLiveMarket:
    def __init__(self):
        self.pool = None

        self.c = WebSocketClient(subscriptions=["T.*"], custom_json=orjson,market=Market.Options, api_key=os.environ.get('YOUR_POLYGON_KEY'))
        self.pool = None
        self.connection_string = os.environ.get('OPTIONS_MARKET')  # Use one environment variable for connection string

       

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.connection_string, min_size=1, max_size=10)
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
    async def insert_or_update_symbol(self, trade_data):
        expiry_date = datetime.strptime(trade_data['expiry_date'], '%Y-%m-%d').date()
        call_put = 'C' if trade_data['call_put'] == 'Call' else 'P'

        new_price = Decimal(trade_data['price'])

        async with self.pool.acquire() as connection:
            try:
                # Start a transaction
                async with connection.transaction():
                    # Check if the option symbol already exists
                    current_price = await connection.fetchval(
                        'SELECT price FROM options WHERE option_symbol = $1',
                        trade_data['option_symbol']
                    )

                    if current_price is not None:
                        # Ensure that current_price is a Decimal
                        current_price = Decimal(current_price)
                        # Calculate the price difference
                        price_diff = new_price - current_price

                        # Update the existing record with the new price
                        await connection.execute(
                            '''
                            UPDATE options
                            SET price = $1
                            WHERE option_symbol = $2
                            ''',
                            trade_data['price'],
                            trade_data['option_symbol']
                        )

                        # Insert the price change into the history table
                        await connection.execute(
                            '''
                            INSERT INTO price_history (option_symbol, price, price_change)
                            VALUES ($1, $2, $3)
                            ''',
                            trade_data['option_symbol'],
                            new_price,
                            price_diff
                        )
                    else:
                        # Insert the new record into the options table
                        await connection.execute(
                            '''
                            INSERT INTO options (option_symbol, underlying_symbol, strike_price, call_put, expiry_date, price)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ''',
                            trade_data['option_symbol'],
                            trade_data['underlying_symbol'],
                            trade_data['strike_price'],
                            call_put,
                            expiry_date,
                            trade_data['price']
                        )
                        
                        # Insert the initial price into the history table with no price change
                        await connection.execute(
                            '''
                            INSERT INTO price_history (option_symbol, price)
                            VALUES ($1, $2)
                            ''',
                            trade_data['option_symbol'],
                            trade_data['price']
                        )

                return True
            except asyncpg.UniqueViolationError:
                print(f"The option symbol {trade_data['option_symbol']} already exists.")
                return False
            except Exception as e:
                print(f"An error occurred: {e}")
                return False


    async def handle_msg(self, msgs: WebSocketMessage):
        for m in msgs:
            ticker = m.symbol
            components = get_human_readable_string(ticker)
            underlying_symbol = components.get('underlying_symbol')
            call_put = components.get('call_put')
            strike_price = components.get('strike_price')
            expiry_date = components.get('expiry_date')


            symbol_name = { 
                'option_symbol': m.symbol.replace('O:', ''),
                'underlying_symbol': underlying_symbol,
                'strike_price': strike_price,
                'call_put': call_put,
                'expiry_date': expiry_date,
                
                'price': m.price,
            }
        

            await self.insert_or_update_symbol(symbol_name)



market = OptionsLiveMarket()



async def main():
    await market.connect()
    await asyncio.gather(market.c.connect(market.handle_msg))


asyncio.run(main())