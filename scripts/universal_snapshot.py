import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

import os

from dotenv import load_dotenv
load_dotenv()

from apis.polygonio.mapping import option_condition_dict, OPTIONS_EXCHANGES
from list_sets.ticker_lists import most_active_tickers

from apis.helpers import flatten_dict
import aiohttp
import asyncio
from aiohttp import ClientTimeout
from asyncio import Semaphore
sema = Semaphore()

from datetime import datetime

YOUR_API_KEY = os.environ.get('YOUR_POLYGON_KEY')

async def process_universal_snapshot(data):
    batch = []
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

    if len(batch) == 250:
        return batch





async def get_universal_snapshot(ticker, retries=1): #✅
    """Fetches the Polygon.io universal snapshot API endpoint"""
    timeout = ClientTimeout(total=10)  # 10 seconds timeout for the request
    
    for retry in range(retries):
       # async with sema:
        url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&apiKey={YOUR_API_KEY}&limit=250"
        print(url)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(url) as resp:
                    data = await resp.json()
                    results = data.get('results', None)
    
                    if results is not None:
                        flattened_results = [flatten_dict(result) for result in results]
                        return flattened_results
                        
            except aiohttp.ClientConnectorError:
                print("ClientConnectorError occurred. Retrying...")
                continue
            
            except aiohttp.ContentTypeError as e:
                print(f"ContentTypeError occurred: {e}")  # Consider logging this
                continue
            
            except Exception as e:
                print(f"An unexpected error occurred: {e}")  # Consider logging this
                continue



async def process_contract(data):
        
    data = await get_universal_snapshot(data)

    if data is not None:
        for contract in data:
            try:

                option_symbol = contract.get('ticker')
                expiry = contract.get('details').get('expiration_date')
                strike = contract.get('details').get('strike_price')
                call_put = contract.get('details').get('contract_type')
                underlying_symbol = contract.get('underlying_asset').get('ticker')
                underlying_price = contract.get('underlying_asset').get('price')
                change_to_breakeven = contract.get('underlying_asset').get('change_to_breakeven')

                

                trade_size = contract.get('last_trade').get('size')
                trade_price = contract.get('last_trade').get('price')
                trade_conditions = contract.get('last_trade', None).get('conditions', None)
                sip_timestamp = contract.get('last_trade', None).get('sip_timestamp', None)
                if sip_timestamp is not None:
                    sip_timestamp = datetime.fromtimestamp(sip_timestamp / 1e9)
                trade_conditions = [option_condition_dict.get(c) for c in trade_conditions] if trade_conditions is not None else []
                trade_conditions = trade_conditions[0]
                trade_exchange = OPTIONS_EXCHANGES.get(contract.get('last_trade').get('exchange', None))
                break_even_price = contract.get('break_even_price', None)
                implied_volatility = contract.get('implied_volatility', None)
                open_interest = contract.get('open_interest', None)
                name = contract.get('name', None)
                open = contract.get('session').get('open', None)
                high = contract.get('session').get('high', None)
                low = contract.get('session').get('low', None)
                close = contract.get('session').get('close', None)
                volume = contract.get('session').get('volume', None)
                prev_close = contract.get('session').get('previous_close')
                early_trading_change_percent = contract.get('session').get('early_trading_change_percent')
                change = contract.get('session').get('change')
                early_trading_change = contract.get('session').get('early_trading_change')
                change_percent = contract.get('session').get('change_percent', None)

                delta = contract.get('greeks').get('delta')
                vega = contract.get('greeks').get('vega')
                theta = contract.get('greeks').get('theta')
                gamma = contract.get('greeks').get('gamma')

                ask = contract.get('last_quote').get('ask')
                bid = contract.get('last_quote').get('bid')
                ask_size = contract.get('last_quote').get('ask_size')
                bid_size = contract.get('last_quote').get('bid_size')
                ask_exchange = OPTIONS_EXCHANGES.get(contract.get('last_quote').get('ask_exchange'))
                bid_exchange = OPTIONS_EXCHANGES.get(contract.get('last_quote').get('bid_exchange'))


                data_dict = {
                    'option_symbol': option_symbol,
                    'expiry': expiry,
                    'strike': strike,
                    'call_put': call_put,
                    'underlying_symbol': underlying_symbol,
                    'underlying_price': underlying_price,
                    'change_to_breakeven': change_to_breakeven,
                    'trade_size': trade_size,
                    'trade_price': trade_price,
                    'trade_conditions': trade_conditions,
                    'sip_timestamp': sip_timestamp,
                    'trade_exchange': trade_exchange,
                    'break_even_price': break_even_price,
                    'implied_volatility': implied_volatility,
                    'open_interest': open_interest,
                    'name': name,
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume,
                    'prev_close': prev_close,
                    'early_trading_change_percent': early_trading_change_percent,
                    'change': change,
                    'early_trading_change': early_trading_change,
                    'change_percent': change_percent,
                    'delta': delta,
                    'vega': vega,
                    'theta': theta,
                    'gamma': gamma,
                    'ask': ask,
                    'bid': bid,
                    'ask_size': ask_size,
                    'bid_size': bid_size,
                    'ask_exchange': ask_exchange,
                    'bid_exchange': bid_exchange
                }

            except Exception as e:
                # Handle any other exceptions that might occur
                print(f"An error occurred: {e}")

async def process_symbol(symbol):
    data = await get_universal_snapshot(symbol)

    if data is not None:
        return await asyncio.gather(*[process_contract(contract) for contract in data])
# async def main():
   
#     print("Database connected.")

#     tasks = []
#     batch_size = 8  # You can adjust this number based on your needs

#     for symbol in most_active_tickers:
#         print(f"Preparing task for symbol: {symbol}")
#         tasks.append(process_symbol(symbol))

#         if len(tasks) >= batch_size:
#             try:
#                 print(f"Executing batch of {batch_size} tasks.")
#                 await asyncio.gather(*tasks)
#                 print(f"Completed batch of {batch_size} tasks.")
#             except Exception as e:
#                 print(f"An error occurred while processing a batch: {e}")
#             finally:
#                 tasks.clear()  # Clear the list for the next batch, regardless of success or failure

#     # Handle any remaining tasks
#     if tasks:
#         try:
#             print(f"Executing final batch of {len(tasks)} tasks.")
#             await asyncio.gather(*tasks)
#             print("Completed final batch.")
#         except Exception as e:
#             print(f"An error occurred while processing the final batch: {e}")

#     print("All tasks completed.")

# asyncio.run(main())