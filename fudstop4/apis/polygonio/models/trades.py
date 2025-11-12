from mapping import stock_condition_dict, STOCK_EXCHANGES, TAPES
from datetime import datetime
import pytz
from dataclasses import dataclass,asdict
import pandas as pd
class TradeData:
    def __init__(self, data, ticker):
        self.conditions = [i.get('conditions') for i in data]
        self.conditions = [item for sublist in (self.conditions or []) if sublist is not None for item in sublist]
        self.conditions = [stock_condition_dict.get(cond) for cond in self.conditions]
        
        self.exchange = [STOCK_EXCHANGES.get(i.get('exchange')) for i in data]
        try:
            self.id = [float(i.get('id', '-')) for i in data]
        except ValueError:
            self.id = None
        self.price = [float(i.get('price',0)) for i in data]
        self.sequence_number = [float(i.get('sequence_number')) for i in data]
        self.sip_timestamp = [self.convert_timestamp(i.get('sip_timestamp')) for i in data]
        self.size = [float(i.get('size',0)) for i in data]
        self.tape = [TAPES.get(i.get('tape')) for i in data]
        self.dollar_cost = [p * s for p, s in zip(self.price, self.size)]
        self.align_list_lengths()
        self.data_dict = { 
            'ticker': ticker,
            'sequence_number': self.sequence_number,
            'conditions': self.conditions,
            'exchange': self.exchange,
            'id': self.id,
            'trade_size': self.size,
            'trade_price': self.price,
            'tape': self.tape,
            'dollar_cost': self.dollar_cost,
            'timestamp': self.sip_timestamp

        }

        self.df = pd.DataFrame(self.data_dict)
    
    @staticmethod
    def convert_timestamp(ts):
        try:
            # Check if ts is already a datetime object
            if isinstance(ts, datetime):
                # If it's timezone-aware, convert to UTC
                if ts.tzinfo is not None and ts.tzinfo.utcoffset(ts) is not None:
                    ts = ts.astimezone(pytz.utc)
                timestamp_in_seconds = ts.timestamp()
            else:
                # Assuming ts is in nanoseconds and is a UTC timestamp
                timestamp_in_seconds = ts / 1e9

            # Convert to datetime object in UTC
            dt_utc = datetime.utcfromtimestamp(timestamp_in_seconds)

            # Convert to Eastern Time
            eastern = pytz.timezone('US/Eastern')
            dt_eastern = dt_utc.replace(tzinfo=pytz.utc).astimezone(eastern)

            return dt_eastern

        except Exception as e:
            print(f"Error in convert_timestamp: {e}")
            return None

    @staticmethod
    def flatten(lst):
        return [item for sublist in lst for item in (sublist if isinstance(sublist, list) else [sublist])]
    


    def align_list_lengths(self):
        for attr in ['sequence_number', 'conditions', 'exchange', 'id', 'size', 'price', 'tape', 'dollar_cost', 'sip_timestamp']:
            if getattr(self, attr) is None:
                setattr(self, attr, [])
        # Find the maximum length among all attributes
        max_length = max(len(getattr(self, attr)) for attr in ['sequence_number', 'conditions', 'exchange', 'id', 'size', 'price', 'tape', 'dollar_cost', 'sip_timestamp'])

        # Extend shorter lists to match the maximum length
        for attr in ['sequence_number', 'conditions', 'exchange', 'id', 'size', 'price', 'tape', 'dollar_cost', 'sip_timestamp']:
            current_list = getattr(self, attr)
            current_list.extend([None] * (max_length - len(current_list)))
            setattr(self, attr, current_list)
    @staticmethod
    def ensure_list(value):
        if isinstance(value, list):
            return value
        else:
            return [value]
    def __repr__(self):
        return f"<TradeData id={self.id}, price={self.price}, size={self.size}>"
    
from typing import Optional,Any,Dict


class LastTradeData:


    def __init__(self, results):
        if results is not None:
            self.ticker = results.get('T')
            self.exchange = STOCK_EXCHANGES.get(results.get('x'))
            self.conditions = [stock_condition_dict.get(i) for i in results.get('c', [])]
            self.conditions=  ','.join(self.conditions)
            self.price = results.get('p')
            self.size = results.get('s')
            self.correction = results.get('e')
            self.sequence_number = results.get('q')
            self.tape = TAPES.get(results.get('z'))
            self.dollar_cost = self.price * self.size if self.price and self.size else 0
            self.timestamp = self.convert_timestamp(results.get('t'))


            self.data_dict = { 
                'ticker': self.ticker,
                'exchange': self.exchange,
                'conditions': self.conditions,
                'price': self.price,
                'size': self.size,
                'cost': self.dollar_cost,
                'tape': self.tape,
                'correction': self.correction,
                'sequence': self.sequence_number,
                'timestamp': self.timestamp
            }

            self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])


    

    @staticmethod
    def convert_timestamp(ts):
        # Convert nanoseconds to seconds
        timestamp_in_seconds = ts / 1e9
        # Convert to datetime object in UTC
        dt_utc = datetime.utcfromtimestamp(timestamp_in_seconds)
        # Convert to Eastern Time
        eastern = pytz.timezone('US/Eastern')
        dt_eastern = dt_utc.replace(tzinfo=pytz.utc).astimezone(eastern)
        # Return as datetime object
        return dt_eastern