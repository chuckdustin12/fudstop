import sys
from pathlib import Path
import pytz
import pandas as pd
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

from typing import List, Optional
from dataclasses import dataclass, field
from polygon_helpers import convert_datetime_list
from polygonio.mapping import stock_condition_dict,STOCK_EXCHANGES
from typing import Dict
from datetime import datetime

class StockSnapshot:
    def __init__(self, data):

        if isinstance(data, list):
            self.ticker = [i.get('ticker') for i in data]
            self.todaysChangePerc = [float(i.get('todaysChangePerc')) for i in data]
            self.todaysChange = [float(i.get('todaysChange')) for i in data]
            self.updated = [i.get('updated') for i in data]



            day = [i.get('day') for i in data]

            # Your existing code with handling for None values
            self.day_o = [float(i.get('o', 0)) for i in day]
            self.day_h = [float(i.get('h', 0)) for i in day]
            self.day_l = [float(i.get('l', 0)) for i in day]
            self.day_c = [float(i.get('c', 0)) for i in day]
            self.day_v = [float(i.get('v', 0)) for i in day]
            self.day_vw = [float(i.get('vw', 0)) for i in day]

            lastQuote = [i.get('lastQuote') or {} for i in data]
            self.ask = [float(i.get('P', 0)) for i in lastQuote]
            self.ask_size = [float(i.get('S', 0)) for i in lastQuote]
            self.bid = [float(i.get('p', 0)) for i in lastQuote]
            self.bid_size = [float(i.get('s', 0)) for i in lastQuote]
            self.quote_timestamp = [self.convert_timestamp(i.get('t')) for i in lastQuote]

            # Step 1: Extract lastTrade and trade conditions for each ticker
            lastTrade = [i.get('lastTrade') for i in data]
            self.trade_conditions = [i.get('c', []) for i in lastTrade]  # Use empty list as default

            # Step 2: Process each ticker's conditions
            self.trade_conditions_strings = []  # Store the string of conditions for each ticker

            for condition in self.trade_conditions:
                # Flatten the lists while mapping each value using the stock_condition_dict
                flattened_conditions = [
                    stock_condition_dict.get(cond, f"Unknown ({cond})")
                    for cond in (condition if isinstance(condition, list) else [condition])
                ]
                # Join the list of condition names into a comma-separated string for this ticker
                trade_conditions_string = ', '.join(flattened_conditions)
                self.trade_conditions_strings.append(trade_conditions_string)

            # Print or use the results for all tickers
            for idx, conditions_string in enumerate(self.trade_conditions_strings):
                print(f"Ticker {idx + 1}: {conditions_string}")


            self.trade_id = [i.get('i') for i in lastTrade]
            self.trade_price = [float(i.get('p', 0)) for i in lastTrade]
            self.trade_size = [float(i.get('s', 0)) for i in lastTrade]
            self.trade_timestamp = [self.convert_timestamp(i.get('t')) for i in lastTrade]
            self.trade_exchange = [i.get('x') for i in lastTrade]
                


            min = [i.get('min') for i in data]
            self.min_av = [float(i.get('av')) for i in min]
            self.min_timestamp = [self.convert_timestamp(i.get('t')) for i in min]
            self.min_trades = [float(i.get('n', 0)) for i in min]
            self.min_o = [float(i.get('o', 0)) for i in min]
            self.min_h = [float(i.get('h', 0)) for i in min]
            self.min_l = [float(i.get('l', 0)) for i in min]
            self.min_c = [float(i.get('c', 0)) for i in min]
            self.min_v = [float(i.get('v', 0)) for i in min]
            self.min_vw = [float(i.get('vw', 0)) for i in min]



            prevDay = [i.get('prevDay') for i in data]
            self.o = [float(i.get('o', 0)) for i in prevDay]
            self.h = [float(i.get('h', 0)) for i in prevDay]
            self.l = [float(i.get('l', 0)) for i in prevDay]
            self.c = [float(i.get('c', 0)) for i in prevDay]
            self.v = [float(i.get('v', 0)) for i in prevDay]
            self.vw = [float(i.get('vw', 0)) for i in prevDay]

            print(self.trade_conditions_strings)
            self.data_dict = { 
                'ticker': self.ticker,
                'prev_open': self.o,
                'prev_high': self.h,
                'prev_low': self.l,
                'prev_close': self.c,
                'prev_volume': self.v,
                'prev_vwap': self.vw,
                'open': self.day_o,
                'high': self.day_h,
                'low': self.day_l,
                'close': self.day_c,
                'volume': self.day_v,
                'vwap': self.day_vw,
                'change': self.todaysChange,
                'change_pct': self.todaysChangePerc,
                'min_open': self.min_o,
                'min_high': self.min_h,
                'min_low': self.min_l,
                'min_close': self.min_c,
                'min_volume': self.min_v,
                'min_vwap': self.min_vw,
                'min_trades': self.min_trades,
                'min_timestamp': self.min_timestamp,
                'ask': self.ask,
                'ask_size': self.ask_size,
                'bid': self.bid,
                'bid_size': self.bid_size,
                'last_tradeprice': self.trade_price,
                'last_tradesize': self.trade_size,
                'last_tradeid': self.trade_id,
                'last_tadetime': self.trade_timestamp,
                'last_tradeconditions': self.trade_conditions_strings,
                'last_tradeexchange': self.trade_exchange,


            }


            self.as_dataframe = pd.DataFrame(self.data_dict)
        else:
            ticker = data.get('ticker')
            self.todaysChangePerc = float(ticker.get('todaysChangePerc', 0))
            self.todaysChange = float(data.get('todaysChange', 0))
            self.updated = data.get('updated')

            day = data.get('day', {})
            self.day_o = float(day.get('o', 0))
            self.day_h = float(day.get('h', 0))
            self.day_l = float(day.get('l', 0))
            self.day_c = float(day.get('c', 0))
            self.day_v = float(day.get('v', 0))
            self.day_vw = float(day.get('vw', 0))

            lastQuote = data.get('lastQuote', {})
            self.ask = float(lastQuote.get('P', 0))
            self.ask_size = float(lastQuote.get('S', 0))
            self.bid = float(lastQuote.get('p', 0))
            self.bid_size = float(lastQuote.get('s', 0))
            self.quote_timestamp = self.convert_timestamp(lastQuote.get('t'))

            # Process trade conditions
            lastTrade = data.get('lastTrade', {})
            self.trade_conditions = lastTrade.get('c', [])  # Default to an empty list

            # Map trade conditions using stock_condition_dict
            flattened_conditions = [
                stock_condition_dict.get(cond, f"Unknown ({cond})")
                for cond in (self.trade_conditions if isinstance(self.trade_conditions, list) else [self.trade_conditions])
            ]
            self.trade_conditions_string = ', '.join(flattened_conditions)
            print(f"Trade Conditions: {self.trade_conditions_string}")

            self.trade_id = lastTrade.get('i')
            self.trade_price = float(lastTrade.get('p', 0))
            self.trade_size = float(lastTrade.get('s', 0))
            self.trade_timestamp = self.convert_timestamp(lastTrade.get('t'))
            self.trade_exchange = lastTrade.get('x')

            min_data = data.get('min', {})
            self.min_av = float(min_data.get('av', 0))
            self.min_timestamp = self.convert_timestamp(min_data.get('t'))
            self.min_trades = float(min_data.get('n', 0))
            self.min_o = float(min_data.get('o', 0))
            self.min_h = float(min_data.get('h', 0))
            self.min_l = float(min_data.get('l', 0))
            self.min_c = float(min_data.get('c', 0))
            self.min_v = float(min_data.get('v', 0))
            self.min_vw = float(min_data.get('vw', 0))

            prevDay = data.get('prevDay', {})
            self.o = float(prevDay.get('o', 0))
            self.h = float(prevDay.get('h', 0))
            self.l = float(prevDay.get('l', 0))
            self.c = float(prevDay.get('c', 0))
            self.v = float(prevDay.get('v', 0))
            self.vw = float(prevDay.get('vw', 0))

            print(self.trade_conditions_string)

            self.data_dict = {
                'ticker': ticker,
                'prev_open': self.o,
                'prev_high': self.h,
                'prev_low': self.l,
                'prev_close': self.c,
                'prev_volume': self.v,
                'prev_vwap': self.vw,
                'open': self.day_o,
                'high': self.day_h,
                'low': self.day_l,
                'close': self.day_c,
                'volume': self.day_v,
                'vwap': self.day_vw,
                'change': self.todaysChange,
                'change_pct': self.todaysChangePerc,
                'min_open': self.min_o,
                'min_high': self.min_h,
                'min_low': self.min_l,
                'min_close': self.min_c,
                'min_volume': self.min_v,
                'min_vwap': self.min_vw,
                'min_trades': self.min_trades,
                'min_timestamp': self.min_timestamp,
                'ask': self.ask,
                'ask_size': self.ask_size,
                'bid': self.bid,
                'bid_size': self.bid_size,
                'last_tradeprice': self.trade_price,
                'last_tradesize': self.trade_size,
                'last_tradeid': self.trade_id,
                'last_tadetime': self.trade_timestamp,
                'last_tradeconditions': self.trade_conditions_string,
                'last_tradeexchange': self.trade_exchange,
            }

            self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])  # Create a single-row DataFrame




    def convert_timestamp(self, timestamp):
        if timestamp is None:
            return None
        # Convert nanoseconds to seconds
        timestamp_in_seconds = timestamp / 1_000_000_000
        # Convert to datetime and then to desired string format
        dt = datetime.fromtimestamp(timestamp_in_seconds, pytz.timezone('America/Chicago'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')



class SingleStockSnapshot:
    def __init__(self, data):

        ticker = data.get('ticker')
        self.todaysChangePerc = float(data.get('todaysChangePerc', 0))
        self.todaysChange = float(data.get('todaysChange', 0))
        self.updated = data.get('updated')

        day = data.get('day', {})
        self.day_o = float(day.get('o', 0))
        self.day_h = float(day.get('h', 0))
        self.day_l = float(day.get('l', 0))
        self.day_c = float(day.get('c', 0))
        self.day_v = float(day.get('v', 0))
        self.day_vw = float(day.get('vw', 0))

        lastQuote = data.get('lastQuote', {})
        self.ask = float(lastQuote.get('P', 0))
        self.ask_size = float(lastQuote.get('S', 0))
        self.bid = float(lastQuote.get('p', 0))
        self.bid_size = float(lastQuote.get('s', 0))
        self.quote_timestamp = self.convert_timestamp(lastQuote.get('t'))

        # Process trade conditions
        lastTrade = data.get('lastTrade', {})
        self.trade_conditions = lastTrade.get('c', [])  # Default to an empty list

        # Map trade conditions using stock_condition_dict
        flattened_conditions = [
            stock_condition_dict.get(cond, f"Unknown ({cond})")
            for cond in (self.trade_conditions if isinstance(self.trade_conditions, list) else [self.trade_conditions])
        ]
        self.trade_conditions_string = ', '.join(flattened_conditions)
        print(f"Trade Conditions: {self.trade_conditions_string}")

        self.trade_id = lastTrade.get('i')
        self.trade_price = float(lastTrade.get('p', 0))
        self.trade_size = float(lastTrade.get('s', 0))
        self.trade_timestamp = self.convert_timestamp(lastTrade.get('t'))
        self.trade_exchange = lastTrade.get('x')

        min_data = data.get('min', {})
        self.min_av = float(min_data.get('av', 0))
        self.min_timestamp = self.convert_timestamp(min_data.get('t'))
        self.min_trades = float(min_data.get('n', 0))
        self.min_o = float(min_data.get('o', 0))
        self.min_h = float(min_data.get('h', 0))
        self.min_l = float(min_data.get('l', 0))
        self.min_c = float(min_data.get('c', 0))
        self.min_v = float(min_data.get('v', 0))
        self.min_vw = float(min_data.get('vw', 0))

        prevDay = data.get('prevDay', {})
        self.o = float(prevDay.get('o', 0))
        self.h = float(prevDay.get('h', 0))
        self.l = float(prevDay.get('l', 0))
        self.c = float(prevDay.get('c', 0))
        self.v = float(prevDay.get('v', 0))
        self.vw = float(prevDay.get('vw', 0))

        print(self.trade_conditions_string)

        self.data_dict = {
            'ticker': ticker,
            'prev_open': self.o,
            'prev_high': self.h,
            'prev_low': self.l,
            'prev_close': self.c,
            'prev_volume': self.v,
            'prev_vwap': self.vw,
            'open': self.day_o,
            'high': self.day_h,
            'low': self.day_l,
            'close': self.day_c,
            'volume': self.day_v,
            'vwap': self.day_vw,
            'change': self.todaysChange,
            'change_pct': self.todaysChangePerc,
            'min_open': self.min_o,
            'min_high': self.min_h,
            'min_low': self.min_l,
            'min_close': self.min_c,
            'min_volume': self.min_v,
            'min_vwap': self.min_vw,
            'min_trades': self.min_trades,
            'min_timestamp': self.min_timestamp,
            'ask': self.ask,
            'ask_size': self.ask_size,
            'bid': self.bid,
            'bid_size': self.bid_size,
            'last_tradeprice': self.trade_price,
            'last_tradesize': self.trade_size,
            'last_tradeid': self.trade_id,
            'last_tadetime': self.trade_timestamp,
            'last_tradeconditions': self.trade_conditions_string,
            'last_tradeexchange': self.trade_exchange,
        }

        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])  # Create a single-row DataFrame




    def convert_timestamp(self, timestamp):
        if timestamp is None:
            return None
        # Convert nanoseconds to seconds
        timestamp_in_seconds = timestamp / 1_000_000_000
        # Convert to datetime and then to desired string format
        dt = datetime.fromtimestamp(timestamp_in_seconds, pytz.timezone('America/Chicago'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
