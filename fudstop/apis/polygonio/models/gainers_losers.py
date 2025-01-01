import pandas as pd
from datetime import datetime

class GainersLosers:
    def __init__(self, data):


        self.ticker = [i.get('ticker') for i in data]
        self.todaysChangePerc = [float(i.get('todaysChangePerc')) for i in data]
        self.todaysChange = [float(i.get('todaysChange')) for i in data]
        self.updated = [i.get('updated') for i in data]

        day = [i.get('day') for i in data]
        self.day_open = [float(i.get('o')) for i in day]
        self.day_high = [float(i.get('h')) for i in day]
        self.day_low = [float(i.get('l')) for i in day]
        self.day_close = [float(i.get('c')) for i in day]
        self.day_vol = [float(i.get('v')) for i in day]
        self.day_vwap = [float(i.get('vw')) for i in day]





        lastQuote = [i.get('lastQuote') for i in data]
        self.ask = [float(i.get('P')) for i in lastQuote]
        self.ask_size = [float(i.get('S')) for i in lastQuote]
        self.bid = [float(i.get('p')) for i in lastQuote]
        self.bid_size = [float(i.get('s')) for i in lastQuote]
        self.quote_timestamp = [datetime.fromtimestamp(i.get('t') / 1e9).strftime('%Y-%m-%d %H:%M:%S') for i in lastQuote]

        lastTrade = [i.get('lastTrade') for i in data]
        self.conditions = [i.get('c') for i in lastTrade]
        self.trade_id = [i.get('i') for i in lastTrade]
        self.trade_price = [float(i.get('p')) for i in lastTrade]
        self.trade_size = [float(i.get('s')) for i in lastTrade]
        self.trade_timestamp = [datetime.fromtimestamp(i.get('t') / 1e9).strftime('%Y-%m-%d %H:%M:%S') for i in lastTrade]
        self.trade_exchange = [i.get('x') for i in lastTrade]

        min = [i.get('min') for i in data]
        self.day_vol = [float(i.get('av')) for i in min]
        self.min_vol = [float(i.get('v')) for i in min]
        self.min_vwap = [float(i.get('vw')) for i in min]
        self.min_open = [float(i.get('o')) for i in min]
        self.min_high = [float(i.get('h')) for i in min]
        self.min_low = [float(i.get('l')) for i in min]
        self.min_close = [float(i.get('c')) for i in min]
        self.min_trades = [float(i.get('n')) for i in min]
        self.min_timestamp = [datetime.fromtimestamp(i.get('t') / 1000).strftime('%Y-%m-%d %H:%M:%S') for i in min]

        prevDay = [i.get('prevDay') for i in data]
        self.prev_open = [float(i.get('o')) for i in prevDay]
        self.prev_high = [float(i.get('h')) for i in prevDay]
        self.prev_low = [float(i.get('l')) for i in prevDay]
        self.prev_close = [float(i.get('c')) for i in prevDay]
        self.prev_vol = [float(i.get('v')) for i in prevDay]
        self.prev_vwap = [float(i.get('vw')) for i in prevDay]



        self.data_dict = { 
            'ticker': self.ticker,
            'day_open': self.day_open,
            'day_high': self.day_high,
            'day_low': self.day_low,
            'day_close': self.day_close,
            'day_vol': self.day_vol,
            'day_vwap': self.day_vwap,
            'day_change': self.todaysChange,
            'day_changeperc': self.todaysChangePerc,
            'prev_open': self.prev_open,
            'prev_high': self.prev_high,
            'prev_low': self.prev_low,
            'prev_close': self.prev_close,
            'prev_vol': self.prev_vol,
            'prev_vwap': self.prev_vwap,
            'min_open': self.min_open,
            'min_high': self.min_high,
            'min_low': self.min_low,
            'min_close': self.min_close,
            'min_volume': self.min_vol,
            'min_vwap': self.min_vwap,
            'min_trades': self.min_trades,
            'min_time': self.min_timestamp,
            'trade_id': self.trade_id,
            'trade_conditions': self.conditions,
            'trade_exchange': self.trade_exchange,
            'trade_size': self.trade_size,
            'trade_price': self.trade_price,
            'trade_time': self.trade_timestamp,
            'ask': self.ask,
            'ask_size': self.ask_size,
            'bid': self.bid,
            'bid_size': self.bid_size,
            'quote_time': self.quote_timestamp
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)