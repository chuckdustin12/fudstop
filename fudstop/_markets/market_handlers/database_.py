import sys
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()
import json
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from apis.polygonio.polygon_database import PolygonDatabase
from polygon.websocket import WebSocketMessage
from apis.helpers import convert_to_ns_datetime
import os
from math import isnan
import json
from datetime import datetime
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
from asyncio import Queue
import random
import asyncio
from pytz import timezone
from market_handlers.list_sets import indices_names_and_symbols_dict
from _markets.list_sets.dicts import hex_color_dict
from apis.polygonio.mapping import stock_condition_desc_dict,stock_condition_dict,STOCK_EXCHANGES,OPTIONS_EXCHANGES, TAPES,option_condition_desc_dict,option_condition_dict,indicators,quote_conditions
import pandas as pd
from apis.helpers import get_human_readable_string, calculate_price_to_strike
from datetime import datetime
from math import isnan
from datetime import timezone
from pytz import timezone
utc = timezone('UTC')
import pytz
from pytz import timezone
from list_sets.dicts import all_forex_pairs, crypto_currency_pairs
all_forex_pairs = {v: k for k, v in all_forex_pairs.items()}
aware_datetime = utc.localize(datetime.utcnow())
from .list_sets import crypto_conditions_dict, crypto_exchanges
class MarketDBManager(PolygonDatabase):
    def __init__(self, host, port, user, password, database, **kwargs):
        self.host=host
        self.port=port
        self.user=user
        self.password=password
        self.database=database
        self.indices_names = indices_names_and_symbols_dict
        self.pair_to_description = {
            'EUR/USD': "The Euro to US Dollar pair represents the two large global economies in Europe and the United States. It's a good indicator of the economic health between the Eurozone and the US, and is one of the most traded forex pairs. Changes in this pair could signal shifts in economic strength between the regions.",
            'USD/JPY': "The US Dollar to Japanese Yen pair reflects the relationship between the world's largest and third-largest economies. It's often seen as a barometer for global economic health and investor sentiment towards risk. The Yen is also a safe-haven currency, often strengthening during times of market turmoil.",
            'GBP/USD': "The British Pound to US Dollar, also known as 'Cable', signifies the economic health between the UK and the US. This pair is sensitive to political changes and economic policies, especially those related to Brexit and trade relations.",
            'AUD/USD': "The Australian Dollar to US Dollar, also known as 'Aussie', often correlates with commodity prices, especially metals and mining outputs, as Australia is a significant commodities exporter. It's a useful pair for gauging global commodity demand.",
            'USD/CAD': "The US Dollar to Canadian Dollar, often called 'Loonie', is closely tied to commodity prices, particularly oil, as Canada is a major oil exporter. Movements in this pair can indicate changes in energy market trends.",
            'USD/CHF': "The US Dollar to Swiss Franc, known as 'Swissie', is another safe-haven currency pair. The Swiss Franc often strengthens in times of geopolitical tension or financial uncertainty, making it a key pair for risk assessment.",
            'CNY/USD': "The Chinese Yuan to US Dollar reflects the economic health and trade relations between China and the US. Given China's role as a major global exporter and its unique economic policies, this pair is crucial for understanding East-West economic dynamics.",
            'NZD/USD': "The New Zealand Dollar to US Dollar, or 'Kiwi', is influenced by New Zealand's agricultural and trade sectors. It's a valuable pair for gauging the health of the Asia-Pacific region's economy and trade conditions.",
            'USD/CNH': "The US Dollar to Offshore Chinese Yuan provides insight into international investors' expectations of the Chinese economy, separate from the onshore CNY market. It's crucial for understanding the international sentiment towards Chinese economic policies and conditions."
        }
        self.forex_hook_dict = {
            'EUR/USD': os.environ.get('eurusd'),
            'USD/JPY': os.environ.get('usdjpy'),
            'GBP/USD': os.environ.get('gbpusd'),
            'AUD/USD': os.environ.get('audusd'),
            'USD/CAD': os.environ.get('usdcad'),
            'USD/CHF': os.environ.get('usdchf'),
            'CNY/USD': os.environ.get('cnyusd'),
            'NZD/USD': os.environ.get('nzdusd'),
            'USD/CNH': os.environ.get('usdcnh'),
        }
        self.currency_pairs = [
            'EUR/USD',
            'USD/JPY',
            'GBP/USD',
            'AUD/USD',
            'USD/CAD',
            'USD/CHF',
            'CNY/USD',
            'NZD/USD',
            'USD/CNH'
        ]

        self.colors = hex_color_dict
        super().__init__(host=host,port=port,database=database,password=password,user=user,**kwargs)


  

    async def insert_stock_trades(self, m):

        data = { 
            'type': 'EquityTrade',
            'ticker': m.symbol,
            'trade_exchange': STOCK_EXCHANGES.get(m.exchange),
            'trade_price': m.price,
            'trade_size': m.size,
            'trade_conditions': [stock_condition_dict.get(condition) for condition in m.conditions] if m.conditions is not None else [],
            'trade_timestamp': m.timestamp
        }


        df = pd.DataFrame(data)



        await self.batch_insert_dataframe(df, table_name='stock_trades', unique_columns='insertion_timestamp')
        yield data


    async def insert_l2_book(sel,m):
        
        yield m


    async def insert_crypto_trades(self, m):
        
        


        conditions = [crypto_conditions_dict.get(i) for i in m.conditions]
        color = 'red' if 'Sell' in conditions[0] else 'green'
        dollar_cost = m.size * m.price

        data = { 
            'type': m.event_type,
            'ticker': m.pair,
            'exchange': crypto_exchanges.get(m.exchange),
            'id': m.id,
            'price': m.price,
            'size': m.size,
            'conditions': conditions,
            'color': color,
            'dollar_cost': dollar_cost
        }

    
        df = pd.DataFrame(data, index=[0])

        if dollar_cost >= 1000:
            await self.batch_insert_dataframe(df, table_name='crypto_trades', unique_columns='insertion_timestamp')

        if data.get('dollar_cost') >= 5000:
            yield data
    async def insert_forex_aggs(self, m):

        name = all_forex_pairs.get(m.pair)
        print(name)
        data_quotes= { 
            'ticker': m.pair,
            'name': name,
            'open': m.open,
            'close': m.close,
            
            'high': m.high,
            'low': m.low,
            'volume': m.volume

        }
        df = pd.DataFrame(data_quotes, index=[0])

        if data_quotes.get('volume') >=3 and data_quotes.get('ticker') in self.currency_pairs:


            asyncio.create_task(self.batch_insert_dataframe(df, table_name='forex_aggs', unique_columns='insertion_timestamp'))

            yield data_quotes


    async def insert_stock_aggs(self, m):
        
        data = {
            'type': 'A',
            'ticker': m.symbol,
            'close_price': m.close,
            'high_price': m.high,
            'low_price': m.low,
            'open_price': m.open,
            'volume': m.volume,
            'official_open': m.official_open_price,
            'accumulated_volume': m.accumulated_volume,
            'vwap_price': m.vwap,
            'agg_timestamp': m.end_timestamp
        }
        
        df = pd.DataFrame(data, index=[0])
        await self.batch_insert_dataframe(df, table_name='stock_aggs', unique_columns='insertion_timestamp')
        yield data

    async def insert_stock_quotes(self, m):
        
        indicator = [indicators.get(indicator) for indicator in m.indicators] if m.indicators is not None else []
        data = {
        'type': 'Q',
        'ticker': m.symbol,
        'ask': m.ask_price,
        'bid':m.bid_price,
        'ask_size': m.ask_size,
        'bid_size':m.bid_size,
        'indicator': indicator,
        'condition':quote_conditions.get(m.condition),

        
        'ask_exchange':STOCK_EXCHANGES.get(m.ask_exchange_id),
        'bid_exchange':STOCK_EXCHANGES.get(m.bid_exchange_id),
        
        'timestamp': m.timestamp,
        'tape': TAPES.get(m.tape)}

        df = pd.DataFrame(data, index=[0])

        await self.batch_insert_dataframe(df, table_name='stock_quotes', unique_columns='insertion_timestamp')
        yield data
    async def insert_option_trades(self, m):
 
        us_central = pytz.timezone('US/Central')
        utc = pytz.UTC
        symbol = get_human_readable_string(m.symbol)
        strike = symbol.get('strike_price')
        expiry = symbol.get('expiry_date')
        call_put = symbol.get('call_put')
        underlying_symbol = symbol.get('underlying_symbol')
        trade_message_data = {}
        trade_message_data['type'] = 'EquityOptionTrade'
        trade_message_data['expiry'] = expiry
        trade_message_data['expiry'] =  datetime.strptime(expiry, '%Y-%m-%d').date()
        trade_message_data['call_put'] = call_put
        trade_message_data['ticker'] = underlying_symbol
        trade_message_data['strike'] = strike
        

        trade_message_data['option_symbol'] = m.symbol
        trade_message_data['price'] = m.price
        trade_message_data['size'] = m.size
        

        
        trade_message_data['price_to_strike'] = calculate_price_to_strike(m.price, strike)


        timestamp = datetime.fromtimestamp(m.timestamp / 1000.0, tz=utc)

        trade_message_data['hour_of_day'] = timestamp.hour



        trade_message_data['conditions'] = [option_condition_dict.get(condition) for condition in m.conditions] if m.conditions is not None else []
        trade_message_data['conditions'] = trade_message_data['conditions'][0]
        trade_message_data['weekday'] = timestamp.weekday()
        trade_message_data['exchange'] = OPTIONS_EXCHANGES.get(m.exchange)


 

        df = pd.DataFrame(trade_message_data, index=[0])
        await self.batch_insert_dataframe(df, table_name='option_trades', unique_columns='insertion_timestamp')
        yield trade_message_data


    async def insert_option_aggs(self, m):
        us_central = pytz.timezone('US/Central')
        utc = pytz.UTC
        symbol = get_human_readable_string(m.symbol)
        strike = symbol.get('strike_price')
        expiry = symbol.get('expiry_date')
        call_put = symbol.get('call_put')
        underlying_symbol = symbol.get('underlying_symbol')
        agg_message_data = {}

        agg_message_data['type'] = 'EquityOptionAgg'
        agg_message_data['ticker'] = underlying_symbol
        agg_message_data['strike'] = strike
        agg_message_data['expiry'] = expiry
        agg_message_data['expiry']  =datetime.strptime(expiry, '%Y-%m-%d').date()
        agg_message_data['call_put'] = call_put
        agg_message_data['option_symbol'] = m.symbol
        agg_message_data['total_volume'] = m.accumulated_volume
        agg_message_data['volume'] = m.volume
        agg_message_data['day_vwap'] = m.aggregate_vwap
        agg_message_data['official_open'] = m.official_open_price
        agg_message_data['last_price'] = m.close
        agg_message_data['open'] = m.open




        agg_message_data['price_diff'] = agg_message_data['last_price'] - agg_message_data['official_open']
        # Moneyness
        if not isnan(agg_message_data['strike']):
            agg_message_data['moneyness'] = agg_message_data['last_price'] / agg_message_data['strike']
        
        # Price-VWAP Difference
        agg_message_data['price_vwap_diff'] = agg_message_data['last_price'] - agg_message_data['day_vwap']    
        # Price Percentage Change
        if not isnan(agg_message_data['official_open']):
            agg_message_data['price_percent_change'] = ((agg_message_data['last_price'] - agg_message_data['official_open']) / agg_message_data['official_open']) * 100
        
        # Volume Percentage of Total
        if not isnan(agg_message_data['total_volume']):
            agg_message_data['volume_percent_total'] = (agg_message_data['volume'] / agg_message_data['total_volume']) * 100
        
        # Volume-to-Price
        if not isnan(agg_message_data['last_price']):
            agg_message_data['volume_to_price'] = agg_message_data['volume'] / agg_message_data['last_price']
        



        volume = agg_message_data.get('volume', None)
        total_volume = agg_message_data.get('total_volume')
        ticker = agg_message_data.get('ticker')
        expiry = agg_message_data.get('expiry')
        strike = agg_message_data.get('strike')
        call_put = agg_message_data.get('call_put')
        sym = agg_message_data.get('option_symbol')
        day_vwap = agg_message_data.get('day_vwap')
        official_open = agg_message_data.get('official_open')
        price = agg_message_data.get('price')
        open = agg_message_data.get('open')
        price_diff = agg_message_data.get('price_diff')
        moneyness = agg_message_data.get('moneyness')
        price_vwap_diff = agg_message_data.get('price_vwap_diff')
        price_percent_change = agg_message_data.get('price_percent_change')
        volume_percent_total = agg_message_data.get('volume_percent_total')
        volume_to_price = agg_message_data.get('volume_to_price')
        agg_timestamp = agg_message_data.get('agg_timestamp')
        agg_timestamp = pd.to_datetime(agg_timestamp)




        if volume > 500 and volume == total_volume:
            hook = AsyncDiscordWebhook(os.environ.get('total_volume'), content=f"<@375862240601047070>")
            embed = DiscordEmbed(title=f'{ticker} {strike} {call_put} {expiry}', description=f'```py\nThis feed is returning tickers where the last trade for the contract == the total volume for that contract on the day.```', color=self.hex_colors['yellow'])
            embed.add_embed_field(name=f"Feed:", value=f"> **Volume == Total Volume**", inline=False)
            embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${official_open}**\n> Now: **${open}**\n> Price % Change: **{round(float(price_percent_change),2)}%**\n> Price Diff: **{price_diff}**\n> VWAP: **${day_vwap}**", inline=False)
            embed.add_embed_field(name=f"Extras:", value=f"> Price/VWAP Diff: **{round(float(price_vwap_diff),2)}%**\n> Moneyness: **{round(float(moneyness),2)}%**")
            embed.add_embed_field(name=f"Volume:", value=f"> Trade: **{float(volume):,}**\n> Total: **{total_volume}**\n> Volume % Total: **{round(float(volume_percent_total),2)}%**\n> Volume to Price: **{round(float(volume_to_price),2)}%**")
            embed.set_timestamp()
            embed.set_footer(text=f'{sym} | {agg_timestamp}')
            hook.add_embed(embed)
            asyncio.create_task(hook.execute())


        df = pd.DataFrame(agg_message_data, index=[0])
        await self.batch_insert_dataframe(df, table_name='option_aggs', unique_columns='insertion_timestamp')
        yield agg_message_data


    async def insert_indices_aggs_minute(self, m):
        name = indices_names_and_symbols_dict.get(m.symbol)
        start_timestamp = pd.to_datetime(m.start_timestamp)
        end_timestamp = pd.to_datetime(m.end_timestamp)

        data_queue_data = { 
            'type': 'A',
            'ticker': m.symbol,
            'name': name,
            'day_open': m.official_open_price,
            'minute_open': m.open,
            'minute_high': m.high,
            'minute_low': m.low,
            'minute_close': m.close,
            'minute_start': start_timestamp,
            'minute_end': end_timestamp
        }


        df = pd.DataFrame(data_queue_data, index=[0])

        await self.batch_insert_dataframe(df, table_name='indices_aggs_minute', unique_columns='insertion_timestamp')      

        
        yield data_queue_data


    async def insert_indices_aggs_second(self, m):
        name = indices_names_and_symbols_dict.get(m.symbol)

        data_queue_data = { 
            'type': 'indices_second',
            'official_open': m.official_open_price,
            'name': name,
            'ticker': m.symbol,
            'open': m.open,
            'high': m.high,
            'low': m.low,
            'close': m.close,
            'minute_start': m.start_timestamp,
            'minute_end': m.end_timestamp
        }


        df = pd.DataFrame(data_queue_data, index=[0])

        await self.batch_insert_dataframe(df, table_name='indices_aggs_second', unique_columns='insertion_timestamp')      

        
        yield data_queue_data





