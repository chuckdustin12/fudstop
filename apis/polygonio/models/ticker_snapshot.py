import sys
from pathlib import Path
import pytz
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


        lastTrade = [i.get('lastTrade') for i in data]
        self.trade_conditions = [i.get('c', 0) for i in lastTrade]
        flattened_conditions = [item for sublist in self.trade_conditions for item in (sublist if isinstance(sublist, list) else [sublist])] if self.trade_conditions else []
        self.trade_conditions = [stock_condition_dict.get(c) for c in flattened_conditions] if flattened_conditions is not None else []
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




    def convert_timestamp(self, timestamp):
        if timestamp is None:
            return None
        # Convert nanoseconds to seconds
        timestamp_in_seconds = timestamp / 1_000_000_000
        # Convert to datetime and then to desired string format
        dt = datetime.fromtimestamp(timestamp_in_seconds, pytz.timezone('America/Chicago'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
