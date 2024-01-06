import sys
from pathlib import Path
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

@dataclass
class Ticker:


    ticker: Optional[str] = None
    today_change: Optional[float] = None
    today_change_perc: Optional[float] = None


@dataclass
class Day:


    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    vwap: Optional[float] = None


@dataclass
class LastQuote:


    ask_price: Optional[float] = None
    ask_size: Optional[float] = None
    bid_price: Optional[float] = None
    bid_size: Optional[float] = None
    quote_timestamp: Optional[float] = None


@dataclass
class LastTrade:


    conditions: Optional[List[int]] = None
    trade_id: Optional[str] = None
    trade_price: Optional[float] = None
    trade_size: Optional[int] = None
    trade_timestamp: Optional[int] = None
    trade_exchange: Optional[int] = None


@dataclass
class Min:


    accumulated_volume: Optional[int] = None
    minute_timestamp: Optional[float] = None
    vwap: Optional[float] = None
    volume: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None


@dataclass
class PrevDay:


    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    vwap: Optional[float] = None


@dataclass
class StockSnapshot():  # Inheritance
    def __init__(self):
        super().__init__()

        
    ticker: Optional[str] = None
    today_changep: Optional[float] = None
    today_change: Optional[float] = 0.0
    stock_day: Optional[Day] = None
    stock_last_quote: Optional[LastQuote] = None
    last_trade: Optional[LastTrade] = None
    stock_minute_bar: Optional[Min] = None
    prev_day: Optional[PrevDay] = None

    
    def __init__(self, ticker_data):
        self.ticker = ticker_data.get('ticker', None)
        self.today_changep = ticker_data.get('todaysChangePerc', None)
        self.today_change = ticker_data.get('todaysChange', 0.0)

        day_data = ticker_data.get('day', {})
        if day_data:
            self.stock_day = Day(
                open=day_data.get('o'),
                high=day_data.get('h'),
                low=day_data.get('l'),
                close=day_data.get('c'),
                volume=day_data.get('v'),
                vwap=day_data.get('vw'),
            )

        quote_data = ticker_data.get('lastQuote', None)
        if quote_data:
            self.stock_last_quote = LastQuote(
                ask_price=quote_data.get('P'),
                ask_size=quote_data.get('S'),
                bid_price=quote_data.get('p'),
                bid_size=quote_data.get('s'),
                quote_timestamp=quote_data.get('t'),
            )
        else:
            quote_data = None
        trade_data = ticker_data.get('lastTrade', None)
        if trade_data:
            self.last_trade = LastTrade(
                conditions = [stock_condition_dict.get(i) for i in trade_data.get('c')],

                trade_id=trade_data.get('i'),
                trade_price=trade_data.get('p'),
                trade_size=trade_data.get('s'),
                timestamp = convert_datetime_list(trade_data.get('t'), unit='ns'),
                exchange = STOCK_EXCHANGES.get(trade_data.get('x'))

        
            )
        else:
            trade_data = None
        min_data = ticker_data.get('min', {})
        if min_data:
            self.stock_minute_bar = Min(
                accumulated_volume=min_data.get('av'),
                minute_timestamp=min_data.get('t'),
                vwap=min_data.get('vw'),
                volume=min_data.get('v'),
                open=min_data.get('o'),
                high=min_data.get('h'),
                low=min_data.get('l'),
                close=min_data.get('c'),
            )
        else:
            min_data = None
        prev_data = ticker_data.get('prevDay', None)
        if prev_data:
            self.prev_day = PrevDay(
                open=prev_data.get('o'),
                high=prev_data.get('h'),
                low=prev_data.get('l'),
                close=prev_data.get('c'),
                volume=prev_data.get('v'),
                vwap=prev_data.get('vw'),
            )
        else:
            prev_data = None


