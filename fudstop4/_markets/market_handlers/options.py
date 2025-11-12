import os
from dotenv import load_dotenv
load_dotenv()
from polygon.websocket import WebSocketMessage,WebSocketClient, EquityAgg,EquityQuote,EquityTrade, Market
import pytz
from pytz import timezone
from discord_webhook import AsyncDiscordWebhook, DiscordEmbed
import asyncio
from apis.polygonio.mapping import OPTIONS_EXCHANGES, option_condition_desc_dict, option_condition_dict
from datetime import datetime
from math import isnan
from datetime import timezone
from .cfg import hex_colors
from apis.helpers import calculate_price_to_strike, get_human_readable_string

from pytz import timezone
batch_data_aggs = []
batch_data_trades = []



utc = timezone('UTC')
aware_datetime = utc.localize(datetime.utcnow())


async def handle_option_msg(msgs: WebSocketMessage, data_queue: asyncio.Queue):
    global batch_data_aggs, batch_data_trades


    send_tasks = []  # List to hold all send tasks
    agg_message_data = {}
    symbol_state = {}
    for m in msgs:


        
        us_central = pytz.timezone('US/Central')
        utc = pytz.UTC
        symbol = get_human_readable_string(m.symbol)
        strike = symbol.get('strike_price')
        expiry = symbol.get('expiry_date')
        call_put = symbol.get('call_put')
        underlying_symbol = symbol.get('underlying_symbol')

        ticker_data = { 
            'underlying_symbol': underlying_symbol,
            'strike': strike,
            'call_put': call_put,
            'expiry': expiry,
            'option_symbol': m.symbol
        }
        # if db_manager is not None:
        #     asyncio.create_task(db_manager.insert_option_symbol(ticker_data['underlying_symbol'],ticker_data['call_put'], ticker_data['strike'], ticker_data['expiry'], ticker_data['option_symbol']))


        if isinstance(m, EquityTrade):
            trade_message_data = {}
            trade_message_data['type'] = 'EquityOptionTrade'
            trade_message_data['expiry'] = expiry
            trade_message_data['expiry'] =  datetime.strptime(expiry, '%Y-%m-%d').date()
            trade_message_data['call_put'] = call_put
            trade_message_data['underlying_symbol'] = underlying_symbol
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

            # Add price and volume-based features
            prev_state = symbol_state.get(m.symbol, {})
            trade_message_data['price_change'] = m.price - prev_state.get('prev_price', m.price)
            trade_message_data['volume_change'] = m.size - prev_state.get('prev_volume', m.size)

            # Update state
            symbol_state[m.symbol] = {'prev_price': m.price, 'prev_volume': m.size}
            # if db_manager is not None:
            #     asyncio.create_task(db_manager.save_structured_message(trade_message_data, "optiontrade"))

            
    

            await data_queue.put(trade_message_data)
        if isinstance(m, EquityAgg):
            

            agg_message_data['type'] = 'EquityOptionAgg'
            agg_message_data['underlying_symbol'] = underlying_symbol
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
            ticker = agg_message_data.get('underlying_symbol')
            expiry = agg_message_data.get('expiry')
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


            await data_queue.put(agg_message_data)
            if volume > 500 and volume == total_volume:
                hook = AsyncDiscordWebhook(os.environ.get('total_volume'), content=f"<@375862240601047070>")
                embed = DiscordEmbed(title=f'{ticker} {strike} {call_put} {expiry}', description=f'```py\nThis feed is returning tickers where the last trade for the contract == the total volume for that contract on the day.```', color=hex_colors['yellow'])
                embed.add_embed_field(name=f"Feed:", value=f"> **Volume == Total Volume**", inline=False)
                embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${official_open}**\n> Now: **${open}**\n> Price % Change: **{round(float(price_percent_change),2)}%**\n> Price Diff: **{price_diff}**\n> VWAP: **${day_vwap}**", inline=False)
                embed.add_embed_field(name=f"Extras:", value=f"> Price/VWAP Diff: **{round(float(price_vwap_diff),2)}%**\n> Moneyness: **{round(float(moneyness),2)}%**")
                embed.add_embed_field(name=f"Volume:", value=f"> Trade: **{float(volume):,}**\n> Total: **{total_volume}**\n> Volume % Total: **{round(float(volume_percent_total),2)}%**\n> Volume to Price: **{round(float(volume_to_price),2)}%**")
                embed.set_timestamp()
                embed.set_footer(text=f'{sym} | {agg_timestamp}')
                hook.add_embed(embed)
                await hook.execute()
                # if db_manager is not None:
                    # asyncio.create_task(db_manager.save_structured_message(agg_message_data, "optionagg"))


# async def process_and_store_data(data_list):
#     for data_dict in data_list:
#         # Parse each dictionary
#         break_even_price = data_dict.get('break_even_price', None)
#         session_change = data_dict.get('session.change', None)
#         session_change_percent = data_dict.get('session.change_percent', None)
#         session_early_trading_change = data_dict.get('session.early_trading_change', None)
#         session_early_trading_change_percent = data_dict.get('session.early_trading_change_percent', None)
#         session_close = data_dict.get('session.close', None)
#         session_high = data_dict.get('session.high', None)
#         session_low = data_dict.get('session.low', None)
#         session_open = data_dict.get('session.open', None)
#         session_volume = data_dict.get('session.volume', None)
#         session_previous_close = data_dict.get('session.previous_close', None)
        
#         contract_type = data_dict.get('details.contract_type', None)
#         exercise_style = data_dict.get('details.exercise_style', None)
#         expiration_date = data_dict.get('details.expiration_date', None)
#         strike_price = data_dict.get('details.strike_price', None)
        
#         delta = data_dict.get('greeks.delta', None)
#         gamma = data_dict.get('greeks.gamma', None)
#         theta = data_dict.get('greeks.theta', None)
#         vega = data_dict.get('greeks.vega', None)
        
#         implied_volatility = data_dict.get('implied_volatility', None)
        
#         last_ask = data_dict.get('last_quote.ask', None)
#         last_ask_size = data_dict.get('last_quote.ask_size', None)
#         last_ask_exchange = OPTIONS_EXCHANGES.get(data_dict.get('last_quote.ask_exchange'))
#         last_bid = data_dict.get('last_quote.bid', None)
#         last_bid_size = data_dict.get('last_quote.bid_size', None)
#         last_ask_exchange = OPTIONS_EXCHANGES.get(data_dict.get('last_quote.bid_exchange'))
#         midpoint = data_dict.get('last_quote.midpoint', None)


#         last_trade_timestamp = data_dict.get('last_trade.sip_timestamp', None)


#         if last_trade_timestamp is not None:
#             last_trade_timestamp = datetime.fromtimestamp(last_trade_timestamp / 1e9)
#         last_trade_conditions = [option_condition_dict.get(c) for c in last_trade_conditions] if last_trade_conditions is not None else []
#         last_trade_conditions = last_trade_conditions[0]
#         last_trade_exchange = OPTIONS_EXCHANGES.get(data_dict.get('last_trade'))
#         last_trade_price = data_dict.get('last_trade.price', None)
#         last_trade_size = data_dict.get('last_trade.size', None)

        
#         open_interest = data_dict.get('open_interest', None)
        
#         change_to_break_even = data_dict.get('underlying_asset.change_to_break_even', None)
#         asset_price = data_dict.get('underlying_asset.price', None)
#         asset_ticker = data_dict.get('underlying_asset.ticker', None)

#         name = data_dict.get('name', None)
#         ticker = data_dict.get('ticker', None)
#         await insert_into_database(
#             break_even_price=break_even_price,
#             session_change=session_change,
#             session_change_percent=session_change_percent,
#             session_early_trading_change=session_early_trading_change,
#             session_early_trading_change_percent=session_early_trading_change_percent,
#             session_close=session_close,
#             session_high=session_high,
#             session_low=session_low,
#             session_open=session_open,
#             session_volume=session_volume,
#             session_previous_close=session_previous_close,
#             contract_type=contract_type,
#             exercise_style=exercise_style,
#             expiration_date=expiration_date,
#             strike_price=strike_price,
#             delta=delta,
#             gamma=gamma,
#             theta=theta,
#             vega=vega,
#             implied_volatility=implied_volatility,
#             last_ask=last_ask,
#             last_ask_size=last_ask_size,
#             last_ask_exchange=last_ask_exchange,
#             last_bid=last_bid,
#             last_bid_size=last_bid_size,
#             last_bid_exchange=last_bid_exchange,
#             midpoint=midpoint,
#             last_trade_timestamp=last_trade_timestamp,
#             last_trade_conditions=last_trade_conditions,
#             last_trade_price=last_trade_price,
#             last_trade_size=last_trade_size,
#             last_trade_exchange=last_trade_exchange,
#             open_interest=open_interest,
#             change_to_break_even=change_to_break_even,
#             asset_price=asset_price,
#             asset_ticker=asset_ticker,
#             name=name,
#             ticker=ticker
#         )