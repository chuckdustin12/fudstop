import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)

import os

from dotenv import load_dotenv
load_dotenv()

from apis.polygonio.mapping import option_condition_dict, OPTIONS_EXCHANGES


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


