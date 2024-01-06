import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)


#import main libraries
import asyncio
from datetime import datetime
from collections import deque
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
from polygon.websocket import WebSocketClient, Market, Feed
from polygon.websocket import WebSocketClient


#import configuration, helpers, and conversion mappings
from market_handlers.cfg import hex_colors
from market_handlers.list_sets import CRYPTO_DESCRIPTIONS,CRYPTO_HOOKS
from apis.polygonio.mapping import stock_condition_dict, option_condition_dict, OPTIONS_EXCHANGES


#import message handlers
from market_handlers.forex import handle_forex_msg
from market_handlers.stocks import handle_msg
from market_handlers.crypto import handle_crypto_msg
from market_handlers.options import handle_option_msg
#from market_handlers.indices import indices_handler

#import the most active tickers
from list_sets.ticker_lists import most_active_tickers
#ensure no duplicates exist
most_active_tickers = set(most_active_tickers)


#define all markets
markets = [Market.Options, Market.Stocks, Market.Indices, Market.Crypto, Market.Forex]



#subscribe to all 5 polygon.io websocket clusters. [mix and match as needed]
subscription_patterns = {
    Market.Options: ["T.*,A.*"], #trades, aggregates
    Market.Stocks: ["A.*,T.*,Q.*"], #aggregates, trades, quotes
    #Market.Indices: ["AM.*"], # aggregates - per minute
    Market.Crypto: ['XT.*, XL2.*'], # crypto trades, level 2 book
    Market.Forex: ['CA.*, C.*'] # forex aggregates - per minute, forex quotes

}

# Mapping each market to its handler
market_handlers = {
    Market.Stocks: handle_msg,
    Market.Options: handle_option_msg,
    #Market.Indices: indices_handler,
    Market.Crypto: handle_crypto_msg,
    Market.Forex: handle_forex_msg
    }




# Initialize deques for EquityTrade and EquityOptionAgg
# to process batches of 250 incoming symbols / tickers 
# into postgres database


equity_trade_deque = deque(maxlen=250)
equity_option_agg_deque = deque(maxlen=250)


#import scripts
from scripts.universal_snapshot import get_universal_snapshot



#import sdk kits for streamlined data analysis / processing
from apis.discord_.discord_sdk import DiscordSDK
from apis.stocksera_.stocksera_ import StockSera
from apis.nasdaq.nasdaq_sdk import Nasdaq
from apis.webull.webull_markets import WebullMarkets
from apis.webull.webull_trading import WebullTrading
from apis.polygonio.async_polygon_sdk import Polygon
from apis.newyork_fed.newyork_fed_sdk import FedNewyork
from apis.ofr.ofr_sdk import OFR



#set your API keys in an .env file. 
#see README.md


discord = DiscordSDK()
stockSera = StockSera()
nasdaq = Nasdaq()
wbmarkets = WebullMarkets(connection_string=None) #optional - pass a database connection_string
wbtrading = WebullTrading()
newyorkfed = FedNewyork()
ofr = OFR(connection_string=None) #optional - pass a database connection_string




#the consumer function for data processing in real time

async def consumer(queue: asyncio.Queue):
    batch = []

    while True:
        data = await queue.get()
        #print(queue.qsize()) debugging line 
      
        


        type = data.get('type')








        """
        >>> WORK ZONE...



        PERFORM ALL ANALYSIS AND DATABASE INTEGRATION HERE.


        THIS IS REAL-TIME DATA PROCESSING.


        YOU HAVE SEVERAL TOOLS AT YOUR DISPOSAL VIA THE VARIOUS APIS.


        THE SKY IS YOUR LIMIT.
        
        """







        #define and perform conversions for
        #stock T.* (EquityTrade)

        if type == 'EquityTrade':
            stock_symbol = data.get('stock_symbol')
            exchange = data.get('exchange')
            price = data.get('trade_price')
            size = data.get('trade_size')
            conditions = data.get('trade_conditions')
            timestamp = data.get('trade_timestamp')
            for condition in conditions:
                if condition in stock_condition_dict and size > 1000:
               
                    hook = stock_condition_dict[condition]
                  
                
                    if condition not in stock_condition_dict:
                        print(f'New condition / unsaved condition found: {condition}')

                    


        #defining the EquityAgg
        #stock instance variables
        #A.*



        elif type == 'EquityAgg':
            
            stock_symbol = data.get('stock_symbol')
            if stock_symbol in most_active_tickers:
                close = data.get('close_price')
                high = data.get('high_price')
                open = data.get('open_price')
                low = data.get('low_price')
                volume = data.get('volume')
                official_open = data.get('official_open')
                total_volume = data.get('accumulated_volume')
                vwap = data.get('vwap_price')
                timestamp = data.get('timestamp')


                #.. do work here .. #




        #defining the EquityQuote
        #stock instance variables
        #Q.*

        elif type == 'EquityQuote':
            
            quote_symbol = data.get('quote_symbol')
            if quote_symbol in most_active_tickers:
                ask = data.get('ask')
                bid = data.get('bid')
                ask_size = data.get('ask_size')
                bid_size = data.get('bid_size')
                ask_exchange = data.get('ask_exchange')
                bid_exchange = data.get('bid_exchange')
                timestamp = data.get('timestamp')
                tape = data.get('tape')


                # do work here #





















        #define the EquityOptionAggregates instance

        """
        >>> EXAMPLE WITH LIVE FEATURE ENGINEERING EXAMPLE
        

        !. Several features have been engineered to display versatility in application on top of the provided attributes
        from the polygon API.


        >>> EXAMPLE WITH LIVE DATABASE INTEGRATION EXAMPLE

        !. As real-time option aggregates hit the SIP feed, they are batched in chunks of 250, passed to the polygon.io UniversalSnapshot
        API, and the real-time data and latest reference data are combined & stored in database in real-time.


        """


        if type =='EquityOptionAgg':

            option_symbol = data.get('option_symbol')
            if option_symbol is not None:
               
                equity_option_agg_deque.append(option_symbol)
                
                if len(batch) >= 250:
                    collected_aggs_symbols = ','.join(equity_option_agg_deque)
                    try:
                        data = await get_universal_snapshot(collected_aggs_symbols)


                    
                
                        

                        

                        for i in data:
                            trade_timestamp = i.get('last_trade.sip_timestamp')
                            if trade_timestamp is not None:
                                trade_timestamp = datetime.fromtimestamp(trade_timestamp / 1e9)

                            trade_conditions = i.get('last_trade.conditions', None)
                            if trade_conditions is not None:
                                trade_conditions = option_condition_dict.get(trade_conditions[0])

                            data_dict = {
                                'name': i.get('name'),
                                'option_symbol': i.get('ticker'),
                                'underlying_symbol': i.get('underlying_asset.ticker'),
                                'strike': i.get('details.strike_price'),
                                'call_put': i.get('details.contract_type'),
                                'expiry': (i.get('details.expiration_date')),
                                'underlying_price': i.get('underlying_asset.price'),
                                'change': i.get('session.change'),
                                'change_percent': i.get('session.change_percent'),
                                'early_change': i.get('session.early_trading_change'),
                                'early_change_percent': i.get('session.early_trading_change_percent'),
                                'change_to_break_even': i.get('underlying_asset.change_to_break_even'),
                                'break_even_price': i.get('break_even_price'),
                                'open': i.get('session.open'),
                                'high': i.get('session.high'),
                                'low': i.get('session.low'),
                                'close': i.get('session.close'),
                                'previous_close': i.get('session.previous_close'),
                                'volume': i.get('session.volume'),
                                'oi': i.get('open_interest'),
                                'iv': i.get('implied_volatility'),
                                'delta': i.get('greeks.delta'),
                                'gamma': i.get('greeks.gamma'),
                                'theta': i.get('greeks.theta'),
                                'vega': i.get('greeks.vega'),
                                'trade_size': i.get('last_trade.size'),
                                'trade_price': i.get('last_trade.price'),
                                'trade_exchange': OPTIONS_EXCHANGES.get(i.get('last_trade.exchange')),
                                'trade_conditions': trade_conditions,
                                'trade_timestamp': trade_timestamp,
                                'ask': i.get('last_quote.ask'),
                                'ask_size': i.get('last_quote.ask_size'),
                                'ask_exchange': OPTIONS_EXCHANGES.get(i.get('last_quote.ask_exchange')),
                                'bid': i.get('last_quote.bid'),
                                'bid_size': i.get('last_quote.bid_size'),
                                'bid_exchange': OPTIONS_EXCHANGES.get(i.get('last_quote.bid_exchange'))}
                                                



                            if data_dict['expiry'] is not None:
                                current_date = datetime.now()

                                def parse_date(date_str):
                                    year, month, day = map(int, date_str.split('-'))
                                    return datetime(year, month, day)
                                expiry_date = parse_date(data_dict['expiry'])
                            if data_dict['underlying_price'] is not None and data_dict['strike'] is not None and data_dict['strike'] != 0:
                                data_dict['moneyness'] = data_dict['underlying_price'] / data_dict['strike']
                            else:
                                data_dict['moneyness'] = None

                            # Calculate Time to Expiry (in days)
                            if data_dict['expiry'] is not None:
                                try:
                                    expiry_date = datetime.strptime(data_dict['expiry'], '%Y-%m-%d')
                                    current_date = datetime.now()
                                    if expiry_date is not None and current_date is not None:
                                        data_dict['time_to_expiry'] = (expiry_date - current_date).days
                                    else:
                                        data_dict['time_to_expiry'] = None
                                except ValueError:
                                    # Handle date parsing errors if needed
                                    data_dict['time_to_expiry'] = None
                            else:
                                data_dict['time_to_expiry'] = None
                            # Calculate Intrinsic Value

                            # Calculate Intrinsic Value
                            if data_dict.get('underlying_price') is not None and data_dict.get('strike') is not None:
                                data_dict['intrinsic_value_call'] = max(0, data_dict['underlying_price'] - data_dict['strike'])
                                data_dict['intrinsic_value_put'] = max(0, data_dict['strike'] - data_dict['underlying_price'])
                            else:
                                data_dict['intrinsic_value_call'] = None
                                data_dict['intrinsic_value_put'] = None

                            # Calculate Extrinsic Value
                            if all(k in data_dict and data_dict[k] is not None for k in ['close', 'intrinsic_value_call', 'intrinsic_value_put']):
                                data_dict['extrinsic_value'] = data_dict['close'] - max(data_dict['intrinsic_value_call'], data_dict['intrinsic_value_put'])
                            else:
                                data_dict['extrinsic_value'] = None



                            # Calculate Liquidity Score
                            if data_dict.get('volume') is not None and data_dict.get('oi') is not None:
                                data_dict['liquidity_score'] = data_dict['volume'] * data_dict['oi']
                            else:
                                data_dict['liquidity_score'] = None

                            # Calculate Implied Leverage
                            if data_dict.get('underlying_price') is not None and data_dict.get('close') is not None and data_dict['close'] != 0:
                                data_dict['implied_leverage'] = data_dict['underlying_price'] / data_dict['close']
                            else:
                                data_dict['implied_leverage'] = None

                            # Calculate Delta to Theta Ratio
                            if data_dict.get('delta') is not None and data_dict.get('theta') is not None and data_dict['theta'] != 0:
                                data_dict['delta_to_theta_ratio'] = data_dict['delta'] / data_dict['theta']
                            else:
                                data_dict['delta_to_theta_ratio'] = None

                            # Calculate Cost of Theta using Close Price
                            if data_dict.get('theta') is not None and data_dict.get('close') is not None and data_dict['close'] != 0:
                                data_dict['cost_of_theta'] = data_dict['theta'] / data_dict['close']
                            else:
                                data_dict['cost_of_theta'] = None

                            # # Calculate Risk-Reward Ratio
                            # data_dict['risk_reward_ratio'] = data_dict['potential_profit'] / data_dict['potential_loss']

    
                            batch.append(data_dict)

                            if len(batch) >= 250:

                                await asyncio.sleep(900)
                                batch.clear()
                                        
                    except Exception as e:
                        print(f'Error {e}')


        """
        EXAMPLE WITH  DISCORD USING LIVE INDICES DATA


        >>> *** Tutorial Coming Soon *** <<<       
        
        """


        #!. Create a couple of channels in your discord server, create a webhook for each. One for SPX and one for NDX related indices.
        spx_indices = ""
        ndx_indices = ""
        #spx_indices = os.environ.get('spx_indices')
        #ndx_indices = os.environ.get('ndx_indices')








        if type == 'Indices':
            ticker = data.get('ticker')
            min_open = data.get('minute_open')
            min_high = data.get('minute_high')
            min_low = data.get('minute_low')
            min_close = data.get('minute_close')
            start = data.get('minute_start')
            end = data.get('minute_end')
            official_open = data.get('day_open')
            name = data.get('name')


            #if 'SPX' in name:
                #!. Uncomment if you have a webhook 
                #hook = AsyncDiscordWebhook(spx_indices)
                #embed = DiscordEmbed(title=f"INDICES MARKET - SPX RELATED", description=f'```py\n{name}```', color=hex_colors['magenta'])
                #embed.add_embed_field(name=name, value=f"{ticker}")
                #embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **{data.get('day_open')}**\n> Now: **{data.get('minute_close')}**")
                #embed.add_embed_field(name=f"1min Aggs:", value=f"> Open: **${round(float(min_open),2)}**\n> High: **${round(float(min_high),2)}**\n> Low: **${round(float(min_low),2)}**\n> Close: **${round(float(min_close),2)}**")
                #embed.add_embed_field(name=f"Recorded Time:", value=f"> Start: **{start}**\n> End: **{end}**")
                #hook.add_embed(embed)
                #await hook.execute()

            #if 'NASDAQ' in name:
                #!. Uncomment if you have a webhook 
                #hook = AsyncDiscordWebhook(ndx_indices)
                #embed = DiscordEmbed(title=f"INDICES MARKET - SPX RELATED", description=f'```py\n{name}```', color=hex_colors['magenta'])
                #embed.add_embed_field(name=name, value=f"{ticker}")
                #embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **{data.get('day_open')}**\n> Now: **{data.get('minute_close')}**")
                #embed.add_embed_field(name=f"1min Aggs:", value=f"> Open: **${round(float(min_open),2)}**\n> High: **${round(float(min_high),2)}**\n> Low: **${round(float(min_low),2)}**\n> Close: **${round(float(min_close),2)}**")
                #embed.add_embed_field(name=f"Recorded Time:", value=f"> Start: **{start}**\n> End: **{end}**")
                #hook.add_embed(embed)
                #await hook.execute()





        #defining OptionTrades - realtime options!


        if type == 'EquityOptionTrade':
            expiry = data.get('expiry')
            option_symbol = data.get('option_symbol')
            call_put = data.get('call_put')
            strike = data.get('strike')
            underlying_symbol = data.get('underlying_symbol')
            option_symbol = data.get('option_symbol')
            price = data.get('price')
            price_change = data.get('price_change')
            size = data.get('size')
            volume_change = data.get('volume_change')
            conditions = data.get('conditions')
            exchange = data.get('exchange')
            price_to_strike = data.get('price_to_strike')
            hour_of_day = data.get('hour_of_day')
            weekday = data.get('weekday')
            timestamp = data.get('timestamp')
            dollar_cost = (100 * price) * size
        










        #defining crypto trades! real-time!

        if type == 'XT':
            if data.get('conditions') == 'Buy Side':
                color = hex_colors['green']


            elif data.get('conditions') == 'Sell Side':
                color = hex_colors['red']


            #using a dictionary of crypto hooks
            if data.get('symbol') in CRYPTO_HOOKS:
                price = data.get('price')
                size = data.get('size')
                hook = CRYPTO_HOOKS[data.get('symbol')]
                desc = CRYPTO_DESCRIPTIONS.get(data.get('symbol'))
                
                webhook = AsyncDiscordWebhook(hook, content="@everyone <@375862240601047070>")
                embed = DiscordEmbed(title=f"{data.get('symbol')} | Live Trades", description=f"```py\n{desc}```", color=color)
                embed.add_embed_field(name=f"Exchange:", value=f"> **{data.get('exchange')}**")
                embed.add_embed_field(name=f"Side:", value=f"> **{data.get('conditions')}**")
                embed.add_embed_field(name="Trade Info:", value=f"> Price: **${price}**\n> Size: **{size}**")
                embed.add_embed_field(name=f"Time:", value=f"> **{data.get('timestamp')}**")

                embed.set_footer(text='Data by Polygon.io | Implemented by FUDSTOP | https://www.discord.gg/fudstop')
                embed.set_timestamp()

                webhook.add_embed(embed)

                asyncio.gather(send_webhooks(webhook))


async def send_webhooks(webhook):
    await webhook.execute()


import logging

import os

logging.basicConfig(level=logging.INFO)

import asyncio



#run it all!

#if you have postgres.. uncomment the code below and provide your credentials.



#more detailed tutorial coming!


#this program leverages all 5 real-time markets from polygon.io and several SDK kits to perform deep, real-time data analysis across all major markets

#discord and postgres integrated

async def main():
    num_workers = int(os.environ.get("NUM_WORKERS", 50))
    #max_connections = int(os.environ.get("MAX_CONNECTIONS", 10))
    #connection_semaphore = asyncio.Semaphore(max_connections)

    logging.info("Starting up...")
    #await db.connect()
    data_queue = asyncio.Queue()  # Set a reasonable size limit

    # Create consumer tasks
    consumer_tasks = [asyncio.create_task(consumer(data_queue)) for _ in range(num_workers)]

    # Create WebSocket client tasks
    websocket_tasks = []
    for market in markets:
        #async with connection_semaphore:
        client = WebSocketClient(api_key=os.environ.get('YOUR_POLYGON_KEY'), subscriptions=subscription_patterns[market], market=market, feed=Feed.RealTime)
        websocket_tasks.append(client.connect(lambda msgs, handler=market_handlers[market]: handler(msgs, data_queue)))

    try:
        await asyncio.gather(*websocket_tasks)
        await asyncio.gather(*consumer_tasks)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        # Implement some error recovery logic here

asyncio.run(main())