import pandas as pd
import pytz
from datetime import datetime
import asyncpg
from asyncpg.exceptions import InvalidCatalogNameError
import os

from dataclasses import dataclass, asdict
from typing import List

class Aggregates:
    def __init__(self, data, ticker):
        self.ticker = ticker

        self.volume = [i.get('v') for i in data]
        self.vwap = [i.get('vw') for i in data]
        self.open = [i.get('o') for i in data]
        self.close = [i.get('c') for i in data]
        self.high = [i.get('h') for i in data]
        self.low = [i.get('l') for i in data]
        self.timestamp = [i.get('t') for i in data]
        self.num_trades = [i.get('n') for i in data]

        self.dollar_cost = [v * vw for v, vw in zip(self.volume, self.vwap)]
        
        # Convert timestamps to Eastern Time
        eastern = pytz.timezone('US/Eastern')
        self.timestamp = [
            datetime.fromtimestamp(t / 1000, tz=pytz.utc).astimezone(eastern).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            for t in self.timestamp
        ]

        self.data_dict = {
            'ticker': self.ticker,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'vwap': self.vwap,
            'timestamp': self.timestamp,
            'num_trades': self.num_trades,
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)
