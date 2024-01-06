import os
from dotenv import load_dotenv
load_dotenv()
from fudstop.apis.polygonio.polygon_options import PolygonOptions
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
import numpy as np
import pandas as pd
import asyncio
import requests
from datetime import datetime, timezone, timedelta
poly = Polygon(connection_string=os.environ.get('POLYGON_STRING'))

ticker='AMZN'
timespan='day'
window=50
limit=100
timeframe='quarterly'

multiplier = 1
date_from = '2023-12-01'
date_to = '2023-12-29'


class PolygonMaster:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        # Process each kind of results if it's provided
        if 'macd_results' in kwargs:
            self.process_macd_results(kwargs['macd_results'])
        if 'rsi_results' in kwargs:
            self.process_rsi_results(kwargs['rsi_results'])

        if 'ema_results' in kwargs:
            self.process_ema_results(kwargs['ema_results'])
        
        if 'sma_results' in kwargs:
            self.process_sma_results(kwargs['sma_results'])

        if 'ticker_news_results' in kwargs:
            self.process_ticker_news_results(kwargs['ticker_news_results'])

        if 'aggregates_results' in kwargs:
            self.process_aggregates_results(kwargs['aggregates_results'])

        if 'all_stock_snapshot_results' in kwargs:
            self.process_all_stock_snapshot_results(kwargs['all_stock_snapshot_results'])
        
        if 'financials_results' in kwargs:
            self.process_annual_financial_results(kwargs['financials_results'])

        self.process_api_results(kwargs)

    def format_large_number(self, value):
        """
        Format large numbers into a readable form, handling both positive and negative values.
        """
        if pd.isnull(value):
            return value
        value = float(value)
        is_negative = value < 0
        if is_negative:
            value = -value
        if value >= 1e12:
            formatted_value = f'{value / 1e12:.2f}T'
        elif value >= 1e9:
            formatted_value = f'{value / 1e9:.2f}B'
        elif value >= 1e6:
            formatted_value = f'{value / 1e6:.2f}M'
        elif value >= 1e3:
            formatted_value = f'{value / 1e3:.2f}K'
        else:
            formatted_value = str(value)
        return f'-{formatted_value}' if is_negative else formatted_value

    def format_large_numbers_in_dataframe(self, df):
        """
        Automatically formats all numeric columns in a DataFrame to readable large numbers.
        """
        formatted_df = df.copy()
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for column in numeric_columns:
            formatted_df[column] = formatted_df[column].apply(self.format_large_number)
        return formatted_df


    def process_api_results(self, api_results):
        for result_type, result_data in api_results.items():
            if result_data is not None:
                processing_method = getattr(self, f'process_{result_type}', None)
                if processing_method:
                    processing_method(result_data)


    def process_rsi_results(self, rsi_results):
        self.underlying = rsi_results.get('underlying')
        self.rsi_values = rsi_results.get('values')
                
        self.rsi_timestamp = []
        for i in self.rsi_values:
            timestamp = i.get('timestamp')

            if timestamp:
                try:
                    # Convert timestamp from milliseconds to seconds
                    timestamp = float(timestamp) / 1000

                    # Convert Unix timestamp to datetime and adjust timezone
                    dt_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    dt_object = dt_object.astimezone(timezone(timedelta(hours=-5)))
                    self.rsi_timestamp.append(dt_object.strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    self.rsi_timestamp.append(f'Error: {e}')
            else:
                self.rsi_timestamp.append('Timestamp Missing')
        self.rsi_value = [float(i.get('value')) if i.get('value') is not None else 'N/A' for i in self.rsi_values]
        self.rsi_data_dict = { 
            'rsi_value': self.rsi_value,
            'rsi_timestamp': self.rsi_timestamp,
            'rsi_timespan': timespan
        }

        self.rsi_as_dataframe = pd.DataFrame(self.rsi_data_dict)
        if 'ticker' in self.kwargs:
            # Add the ticker as a new column
            self.rsi_as_dataframe['ticker'] = self.kwargs['ticker']
            



    def process_macd_results(self, macd_results):
        self.underlying = macd_results.get('underlying')
        self.macd_values = macd_results.get('values')
        self.macd_timestamp = []
        for i in self.macd_values:
            timestamp = i.get('timestamp')
            if timestamp:
                try:
                    # Convert from milliseconds to seconds if needed
                    timestamp = float(timestamp)
                    if len(str(int(timestamp))) == 13:  # Check if timestamp is in milliseconds
                        timestamp /= 1000

                    # Convert Unix timestamp to datetime and adjust timezone
                    formatted_timestamp = (
                        datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        .astimezone(timezone(timedelta(hours=-5)))  # Eastern Time (ET) UTC-5
                        .strftime('%Y-%m-%d %H:%M:%S')
                    )
                    self.macd_timestamp.append(formatted_timestamp)
                except Exception as e:
                    self.macd_timestamp.append(f'Error: {e}')
            else:
                self.macd_timestamp.append('N/A')

        self.macd = [float(i.get('value')) if i.get('value') is not None else 'N/A' for i in self.macd_values]
        self.signal = [float(i.get('signal')) if i.get('signal') is not None else 'N/A' for i in self.macd_values]
        self.histogram = [float(i.get('histogram')) if i.get('histogram') is not None else 'N/A' for i in self.macd_values]

        self.macd_data_dict = { 
            'macd': self.macd,
            'histogram': self.histogram,
            'signal': self.signal,
            'timestamp': self.macd_timestamp
        }
        self.macd_as_dataframe = pd.DataFrame(self.macd_data_dict)

        if 'ticker' in self.kwargs:
            # Add the ticker as a new column
            self.macd_as_dataframe['ticker'] = self.kwargs['ticker']
            


    def process_all_stock_snapshot_results(self, all_stock_snapshot_results):
        if all_stock_snapshot_results is not None:
            self.ticker = [i.get('ticker', '') for i in all_stock_snapshot_results]
            self.todaysChangePerc = [float(i.get('todaysChangePerc', 0.0)) for i in all_stock_snapshot_results]
            self.todaysChange = [float(i.get('todaysChange', 0.0)) for i in all_stock_snapshot_results]
            self.updated = [i.get('updated', '') for i in all_stock_snapshot_results]
        else:
            # Handle the case where all_stock_snapshot_results is None
            self.ticker = []
            self.todaysChangePerc = []
            self.todaysChange = []
            self.updated = [] # Default to empty string if 'updated' is None


        self.day = [i.get('day') for i in all_stock_snapshot_results]
        self.day_open = [float(i.get('o')) for i in self.day]
        self.day_high = [float(i.get('h')) for i in self.day]
        self.day_low = [float(i.get('l')) for i in self.day]
        self.day_close = [float(i.get('c')) for i in self.day]
        self.day_volume = [float(i.get('v')) for i in self.day]
        self.day_vwap = [float(i.get('vw')) for i in self.day]




        self.lastQuote = [i.get('lastQuote') for i in all_stock_snapshot_results]
        self.ask  = [float(i.get('P')) for i in self.lastQuote]
        self.ask_size = [float(i.get('S')) for i in self.lastQuote]
        self.bid = [float(i.get('p')) for i in self.lastQuote]
        self.bid_size = [float(i.get('s')) for i in self.lastQuote]
        self.quote_timestamp = []
        for i in self.lastQuote:
            timestamp = i.get('t')
            if timestamp:
                try:
                    # Convert from nanoseconds to seconds
                    timestamp_seconds = int(timestamp) // 1_000_000_000
                    # Calculate remaining nanoseconds after converting to seconds
                    remaining_nanos = int(timestamp) % 1_000_000_000

                    # Create a datetime object for the Unix epoch
                    epoch = datetime(1970, 1, 1)
                    # Add the seconds and remaining nanoseconds to the epoch
                    converted_datetime = epoch + timedelta(seconds=timestamp_seconds, microseconds=remaining_nanos // 1000)

                    # Adjust for Eastern Time (ET) UTC-5
                    adjusted_datetime = converted_datetime - timedelta(hours=5)
                    self.quote_timestamp.append(adjusted_datetime.strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    self.quote_timestamp.append(f'Error: {e}')
            else:
                self.quote_timestamp.append('N/A')

        
        self.lastTrade = [i.get('lastTrade') for i in all_stock_snapshot_results]
        self.trade_conditions = [i.get('c') for i in self.lastTrade]
        self.trade_id = [i.get('i') for i in self.lastTrade]
        self.trade_price = [float(i.get('p')) for i in self.lastTrade]
        self.trade_size = [float(i.get('s')) for i in self.lastTrade]
        self.trade_timestamp = []
        for i in self.lastTrade:
            timestamp = i.get('t')
            if timestamp:
                try:
                    # Convert from nanoseconds to seconds
                    timestamp_seconds = int(timestamp) // 1_000_000_000
                    # Calculate remaining nanoseconds after converting to seconds
                    remaining_nanos = int(timestamp) % 1_000_000_000

                    # Create a datetime object for the Unix epoch
                    epoch = datetime(1970, 1, 1)
                    # Add the seconds and remaining nanoseconds to the epoch
                    converted_datetime = epoch + timedelta(seconds=timestamp_seconds, microseconds=remaining_nanos // 1000)

                    # Adjust for Eastern Time (ET) UTC-5
                    adjusted_datetime = converted_datetime - timedelta(hours=5)
                    self.trade_timestamp.append(adjusted_datetime.strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    self.trade_timestamp.append(f'Error: {e}')
            else:
                self.trade_timestamp.append('N/A')
        self.trade_exchange = [i.get('x') for i in self.lastTrade]


        self.min = [i.get('min') for i in all_stock_snapshot_results]
        self.min_accumulated_volume = [float(i.get('av')) if i.get('av') is not None else 'N/A' for i in all_stock_snapshot_results]
        self.min_timestamp = [
            (
                datetime.fromtimestamp(
                    float(i.get('t')) / 1000 if len(str(int(float(i.get('t'))))) == 13 else float(i.get('t')),
                    tz=timezone.utc
                ).astimezone(timezone(timedelta(hours=-5)))  # Eastern Time (ET) UTC-5
                .strftime('%Y-%m-%d %H:%M:%S') if i.get('t') is not None else 'N/A'
            )
            for i in self.min
        ]
        self.min_num_trades = [float(i.get('n')) if i.get('n') is not None else 'N/A' for i in self.min]
        self.min_open = [float(i.get('o')) if i.get('o') is not None else 'N/A' for i in self.min]
        self.min_high = [float(i.get('h')) if i.get('h') is not None else 'N/A' for i in self.min]
        self.min_low = [float(i.get('l')) if i.get('l') is not None else 'N/A' for i in self.min]
        self.min_close = [float(i.get('c')) if i.get('c') is not None else 'N/A' for i in self.min]
        self.min_vwap = [float(i.get('vw')) if i.get('vw') is not None else 'N/A' for i in self.min]


        self.prevDay = [i.get('prevDay') for i in all_stock_snapshot_results]
        self.prev_open = [float(i.get('o')) if i.get('o') is not None else 'N/A' for i in all_stock_snapshot_results]
        self.prev_high = [float(i.get('h')) if i.get('h') is not None else 'N/A' for i in self.prevDay]
        self.prev_low = [float(i.get('l')) if i.get('l') is not None else 'N/A' for i in self.prevDay]
        self.prev_close = [float(i.get('c')) if i.get('c') is not None else 'N/A' for i in self.prevDay]
        self.prev_volume = [float(i.get('v')) if i.get('v') is not None else 'N/A' for i in self.prevDay]
        self.prev_vwap = [float(i.get('vw')) if i.get('vw') is not None else 'N/A' for i in self.prevDay]


        self.all_stock_snapshot_data_dict = { 
            'ticker': self.ticker,
            'change_percent': self.todaysChangePerc,
            'change': self.todaysChange,
            'day_open': self.day_open,
            'day_high': self.day_high,
            'day_low': self.day_low,
            'day_close': self.day_close,
            'day_volume': self.day_volume,
            'day_vwap': self.day_vwap,
            'prev_open': self.prev_open,
            'prev_high': self.prev_high,
            'prev_low': self.prev_low,
            'prev_close': self.prev_close,
            'prev_volume': self.prev_volume,
            'prev_vwap': self.prev_vwap,
            'min_accumulated_volume': self.min_accumulated_volume,
            'min_open': self.min_open,
            'min_high': self.min_high,
            'min_low': self.min_low,
            'min_close': self.min_close,
            'min_vwap': self.min_vwap,
            'min_num_trades': self.min_num_trades,
            'min_timestamp': self.min_timestamp,
            'trade_conditions': self.trade_conditions,
            'trade_exchange': self.trade_exchange,
            'trade_price': self.trade_price,
            'trade_size': self.trade_size,
            'trade_id': self.trade_id,
            'trade_timestamp': self.trade_timestamp,
            'ask': self.ask,
            'ask_size': self.ask_size,
            'bid': self.bid,
            'bid_size': self.bid_size,
            'quote_timestamp': self.quote_timestamp
        }


        self.all_ticker_snapshots_as_dataframe = pd.DataFrame(self.all_stock_snapshot_data_dict)

    def process_aggregates_results(self, aggregates_results):
        self.agg_volume = [float(i.get('v')) if i.get('v') is not None else 'N/A' for i in aggregates_results]
        self.agg_vwap = [float(i.get('vw')) if i.get('vw') is not None else 'N/A' for i in aggregates_results]
        self.agg_open = [float(i.get('o')) if i.get('o') is not None else 'N/A' for i in aggregates_results]
        self.agg_close = [float(i.get('c')) if i.get('c') is not None else 'N/A' for i in aggregates_results]
        self.agg_high = [float(i.get('h')) if i.get('h') is not None else 'N/A' for i in aggregates_results]
        self.agg_low = [float(i.get('l')) if i.get('l') is not None else 'N/A' for i in aggregates_results]

        # Convert timestamps to Eastern Time

        eastern_timezone = timezone(timedelta(hours=-5))  # Eastern Time (ET) UTC-5
        self.agg_timestamp = [
            datetime.fromtimestamp(float(i.get('t')) / 1000, tz=timezone.utc).astimezone(eastern_timezone).strftime('%Y-%m-%d %H:%M:%S')
            if i.get('t') is not None
            else 'N/A'
            for i in aggregates_results
        ]
        self.agg_num_trades = [float(i.get('n')) if i.get('n') is not None else 'N/A' for i in aggregates_results]


        self.agg_data_dict = { 
            
            'agg_volume': self.agg_volume,
            'agg_vwap': self.agg_vwap,
            'agg_num_trades': self.agg_num_trades,
            'agg_open': self.agg_open,
            'agg_close': self.agg_close,
            'agg_high': self.agg_high,
            'agg_low': self.agg_low,
            'agg_timestamp': self.agg_timestamp,
            'agg_timespan': timespan
            
                                    }

        self.agg_as_dataframe = pd.DataFrame(self.agg_data_dict)

        if 'ticker' in self.kwargs:
            # Add the ticker as a new column
            self.agg_as_dataframe['ticker'] = self.kwargs['ticker']
            

    def process_ema_results(self, ema_results):
        self.underlying = ema_results.get('underlying')
        self.ema_values = ema_results.get('values')
        self.ema_timestamp = [
            (
                datetime.fromtimestamp(
                    float(i.get('timestamp')) / 1000 if len(str(int(float(i.get('timestamp'))))) == 13 else float(i.get('timestamp')),
                    tz=timezone.utc
                ).astimezone(timezone(timedelta(hours=-5)))  # Eastern Time (ET) UTC-5
                .strftime('%Y-%m-%d %H:%M:%S') if i.get('timestamp') is not None else 'N/A'
            )
            for i in self.ema_values
        ]

        self.ema_value = [float(i.get('value')) if i.get('value') is not None else 'N/A' for i in self.ema_values]

        self.ema_data_dict = { 
            'ema_timestamp': self.ema_timestamp,
            'ema_value': self.ema_value
        }
        self.ema_as_dataframe = pd.DataFrame(self.ema_data_dict)

        if 'ticker' in self.kwargs:
            # Add the ticker as a new column
            self.ema_as_dataframe['ticker'] = self.kwargs['ticker']
            

    def process_sma_results(self, sma_results):
        self.underlying = sma_results.get('underlying')
        self.sma_values = sma_results.get('values')
        self.sma_timestamp = [
            (
                datetime.fromtimestamp(
                    float(i.get('timestamp')) / 1000 if len(str(int(float(i.get('timestamp'))))) == 13 else float(i.get('timestamp')), 
                    tz=timezone.utc
                ).astimezone(timezone(timedelta(hours=-5)))  # Eastern Time (ET) UTC-5
                .strftime('%Y-%m-%d %H:%M:%S') if i.get('timestamp') is not None else 'N/A'
            )
            for i in self.sma_values
        ]

        self.sma_value = [float(i.get('value')) if i.get('value') is not None else 'N/A' for i in self.sma_values]


        self.sma_data_dict = { 
            'sma_value': self.sma_value,
            'sma_timestamp': self.sma_timestamp
        }

        self.sma_as_dataframe = pd.DataFrame(self.sma_data_dict)

        if 'ticker' in self.kwargs:
            # Add the ticker as a new column
            self.sma_as_dataframe['ticker'] = self.kwargs['ticker']

    # def process_single_snapshot_results(self, ticker_snapshot_results):
    #     self.ticker = ticker_snapshot_results['ticker']
    #     self.todaysChangePerc= float(ticker_snapshot_results['todaysChangePerc']) if ticker_snapshot_results['todaysChangePerc'] is not None else 'N/A'
    #     self.todaysChange= float(ticker_snapshot_results['todaysChange']) if ticker_snapshot_results['todaysChange'] is not None else 'N/A'
    #     self.updated= ticker_snapshot_results['updated']


    #     self.day= ticker_snapshot_results['day']
    #     self.day_open = float(self.day['o']) if self.day['o'] is not None else 'N/A'
    #     self.day_high = float(self.day['h']) if self.day['h'] is not None else 'N/A'
    #     self.day_low = float(self.day['l']) if self.day['l'] is not None else 'N/A'
    #     self.day_close = float(self.day['c']) if self.day['c'] is not None else 'N/A'
    #     self.day_volume = float(self.day['v']) if self.day['v'] is not None else 'N/A'
    #     self.day_vwap = float(self.day['v']) if self.day['v'] is not None else 'N/A'

    #     self.lastQuote= ticker_snapshot_results['lastQuote']
    #     self.ask = float(self.lastQuote['P']) if self.lastQuote['P'] is not None else 'N/A'
    #     self.ask_size = float(self.lastQuote['S']) if self.lastQuote['S'] is not None else 'N/A'
    #     self.bid = float(self.lastQuote['p']) if self.lastQuote['p'] is not None else 'N/A'
    #     self.bid_size = float(self.lastQuote['s']) if self.lastQuote['s'] is not None else 'N/A'
    #     timestamp = self.lastQuote.get('t')
    #     if timestamp:
    #         try:
    #             timestamp_seconds = int(timestamp) // 1_000_000_000
    #             remaining_nanos = int(timestamp) % 1_000_000_000

    #             # Create a datetime object for the Unix epoch
    #             epoch = datetime(1970, 1, 1)
    #             # Add the seconds and remaining nanoseconds to the epoch
    #             converted_datetime = epoch + timedelta(seconds=timestamp_seconds, microseconds=remaining_nanos // 1000)

    #             # Adjust for Eastern Time (ET) UTC-5
    #             adjusted_datetime = converted_datetime - timedelta(hours=5)
    #             self.quote_timestamp = adjusted_datetime.strftime('%Y-%m-%d %H:%M:%S')
    #         except Exception as e:
    #             self.quote_timestamp = f'Error: {e}'
    #     else:
    #         self.quote_timestamp = 'N/A'

    #     self.lastTrade= ticker_snapshot_results['lastTrade']
    #     self.trade_conditions = self.lastTrade['c']
    #     self.trade_id = self.lastTrade['i']
    #     self.trade_price = float(self.lastTrade['p']) if self.lastTrade['p'] is not None else 'N/A'
    #     self.trade_size = float(self.lastTrade['s']) if self.lastTrade['s'] is not None else 'N/A'
    #     timestamp = self.lastTrade.get('t')
    #     if timestamp:
    #         try:
    #             # Convert from nanoseconds to seconds
    #             timestamp_seconds = int(timestamp) // 1_000_000_000
    #             # Calculate remaining nanoseconds after converting to seconds
    #             remaining_nanos = int(timestamp) % 1_000_000_000

    #             # Create a datetime object for the Unix epoch
    #             epoch = datetime(1970, 1, 1)
    #             # Add the seconds and remaining nanoseconds to the epoch
    #             converted_datetime = epoch + timedelta(seconds=timestamp_seconds, microseconds=remaining_nanos // 1000)

    #             # Adjust for Eastern Time (ET) UTC-5
    #             adjusted_datetime = converted_datetime - timedelta(hours=5)
    #             self.trade_timestamp = adjusted_datetime.strftime('%Y-%m-%d %H:%M:%S')
    #         except Exception as e:
    #             self.trade_timestamp = f'Error: {e}'
    #     else:
    #         self.trade_timestamp = 'N/A'
    #     self.trade_exchange = float(self.lastTrade['x']) if self.lastTrade['x'] is not None else 'N/A'

    #     self.min= ticker_snapshot_results['min']
    #     self.min_accumulated_volume = float(self.min['av']) if self.min['av'] is not None else 'N/A'
    #     self.min_timestamp = []
    #     timestamp = self.min.get('t')
    #     if timestamp:
    #         try:
    #             # Convert from nanoseconds to seconds
    #             timestamp_seconds = int(timestamp) // 1_000_000_000
    #             # Calculate remaining nanoseconds after converting to seconds
    #             remaining_nanos = int(timestamp) % 1_000_000_000

    #             # Create a datetime object for the Unix epoch
    #             epoch = datetime(1970, 1, 1)
    #             # Add the seconds and remaining nanoseconds to the epoch
    #             converted_datetime = epoch + timedelta(seconds=timestamp_seconds, microseconds=remaining_nanos // 1000)

    #             # Adjust for Eastern Time (ET) UTC-5
    #             adjusted_datetime = converted_datetime - timedelta(hours=5)
    #             self.min_timestamp.append(adjusted_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    #         except Exception as e:
    #             self.min_timestamp.append(f'Error: {e}')
    #     else:
    #         self.min_timestamp.append('N/A')

    #     self.min_num_trades = float(self.min['n']) if self.min['n'] is not None else 'N/A'
    #     self.min_open = float(self.min['o']) if self.min['o'] is not None else 'N/A'
    #     self.min_high = float(self.min['h']) if self.min['h'] is not None else 'N/A'
    #     self.min_low = float(self.min['l']) if self.min['l'] is not None else 'N/A'
    #     self.min_close = float(self.min['c']) if self.min['c'] is not None else 'N/A'
    #     self.min_vwap = float(self.min['vw']) if self.min['vw'] is not None else 'N/A'
                    

    #     self.prevDay= ticker_snapshot_results['prevDay']
    #     self.prev_open = float(self.prevDay['o']) if self.prevDay['o'] is not None else 'N/A'
    #     self.prev_high = float(self.prevDay['h']) if self.prevDay['h'] is not None else 'N/A'
    #     self.prev_low = float(self.prevDay['l']) if self.prevDay['l'] is not None else 'N/A'
    #     self.prev_close = float(self.prevDay['c']) if self.prevDay['c'] is not None else 'N/A'
    #     self.prev_volume = float(self.prevDay['v']) if self.prevDay['v'] is not None else 'N/A'
    #     self.prev_vwap = float(self.prevDay['vw']) if self.prevDay['vw'] is not None else 'N/A'


    #     self.single_ticker_snapshot_data_dict = { 
    #         'ticker': self.ticker,
    #         'change_percent': self.todaysChangePerc,
    #         'change': self.todaysChange,
    #         'day_open': self.day_open,
    #         'day_high': self.day_high,
    #         'day_low': self.day_low,
    #         'day_close': self.day_close,
    #         'day_volume': self.day_volume,
    #         'day_vwap': self.day_vwap,
    #         'prev_open': self.prev_open,
    #         'prev_high': self.prev_high,
    #         'prev_low': self.prev_low,
    #         'prev_close': self.prev_close,
    #         'prev_volume': self.prev_volume,
    #         'prev_vwap': self.prev_vwap,
    #         'min_accumulated_volume': self.min_accumulated_volume,
    #         'min_open': self.min_open,
    #         'min_high': self.min_high,
    #         'min_low': self.min_low,
    #         'min_close': self.min_close,
    #         'min_vwap': self.min_vwap,
    #         'min_num_trades': self.min_num_trades,
    #         'min_timestamp': self.min_timestamp,
    #         'trade_conditions': self.trade_conditions,
    #         'trade_exchange': self.trade_exchange,
    #         'trade_id': self.trade_id,
    #         'trade_price': self.trade_price,
    #         'trade_size': self.trade_size,
    #         'ask': self.ask,
    #         'ask_size': self.ask_size,
    #         'bid': self.bid,
    #         'bid_size': self.bid_size,
    #         'quote_timestamp': self.quote_timestamp
    #     }


    #     self.single_ticker_snapshot_as_dataframe = pd.DataFrame(self.single_ticker_snapshot_data_dict)


    def process_ticker_news_results(self, ticker_news_results):
        publisher = [i.get('publisher') for i in ticker_news_results]
        self.name = [i.get('name') for i in publisher]
        self.logo_url = [i.get('logo_url') for i in publisher]
        self.homepage_url = [i.get('homepage_url') for i in publisher]




        self.title = [i.get('title') for i in ticker_news_results]
        self.author = [i.get('author') for i in ticker_news_results]
        self.published_utc = [i.get('published_utc') for i in ticker_news_results]
        self.article_url = [i.get('article_url') for i in ticker_news_results]
        self.tickers = [i.get('tickers') for i in ticker_news_results]
        self.amp_url = [i.get('amp_url') for i in ticker_news_results]
        self.image_url = [i.get('image_url') for i in ticker_news_results]
        self.description = [i.get('description') for i in ticker_news_results]


        self.ticker_news_data_dict = { 
            'name': self.name,
            'logo_url': self.logo_url,
            'homepage_url': self.homepage_url,
            'title': self.title,
            'author': self.author,
            'article_url': self.article_url,
            'tickers': self.tickers,
            'amp_url': self.amp_url,
            'image_url': self.image_url,
            'description': self.description
        }


        self.ticker_news_as_dataframe = pd.DataFrame(self.ticker_news_data_dict)



    def process_annual_financial_results(self, financials_results):
        self.id = []
        self.sic = []
        self.start_date = []
        self.end_date = []

        self.id = [i.get('id') for i in financials_results]
        self.start_date = [i.get('start_date') for i in financials_results]
        self.end_date = [i.get('end_date') for i in financials_results]
        self.timeframe = [i.get('timeframe') for i in financials_results]
        self.fiscal_period = [i.get('fiscal_period') for i in financials_results]
        self.fiscal_year = [i.get('fiscal_year') for i in financials_results]
        self.cik = [i.get('cik') for i in financials_results]
        self.sic = [i.get('sic') for i in financials_results]
        self.tickers = [i.get('tickers') for i in financials_results]
        self.company_name = [i.get('company_name') for i in financials_results]
        self.financials = [i.get('financials') for i in financials_results]
        balance_sheet = [i.get('balance_sheet') for i in self.financials]
        comprehensive_income = [i.get('comprehensive_income') for i in self.financials]
        for i in comprehensive_income[0]:
            print(i)



        #income statement
        income_statement = [i.get('income_statement') for i in self.financials]
        # Replace None with 0.0 and convert to float for all lists
        net_income_loss = [i.get('net_income_loss', 0.0) for i in income_statement]
        income_loss_from_continuing_operations_before_tax = [i.get('income_loss_from_continuing_operations_before_tax') if i is not None else 0.0 for i in income_statement]
        basic_earnings_per_share = [i.get('basic_earnings_per_share') if i is not None else 0.0 for i in income_statement]
        net_income_loss_attributable_to_parent = [i.get('net_income_loss_attributable_to_parent') if i is not None else 0.0 for i in income_statement]
        revenues = [i.get('revenues') if i is not None else 0.0 for i in income_statement]
        net_income_loss_attributable_to_noncontrolling_interest = [i.get('net_income_loss_attributable_to_noncontrolling_interest') if i is not None else 0.0 for i in income_statement]
        net_income_loss_available_to_common_stockholders_basic = [i.get('net_income_loss_available_to_common_stockholders_basic') if i is not None else 0.0 for i in income_statement]
        gross_profit = [i.get('gross_profit') if i is not None else 0.0 for i in income_statement]
        operating_expenses = [i.get('operating_expenses') if i is not None else 0.0 for i in income_statement]
        preferred_stock_dividends_and_other_adjustments = [i.get('preferred_stock_dividends_and_other_adjustments') if i is not None else 0.0 for i in income_statement]
        cost_of_revenue = [i.get('cost_of_revenue') if i is not None else 0.0 for i in income_statement]
        costs_and_expenses = [i.get('costs_and_expenses') if i is not None else 0.0 for i in income_statement]
        basic_average_shares = [i.get('basic_average_shares') if i is not None else 0.0 for i in income_statement]
        interest_expense_operating = [i.get('interest_expense_operating') if i is not None else 0.0 for i in income_statement]
        income_loss_before_equity_method_investments = [i.get('income_loss_before_equity_method_investments') if i is not None else 0.0 for i in income_statement]
        nonoperating_income_loss = [i.get('nonoperating_income_loss') if i is not None else 0.0 for i in income_statement]
        benefits_costs_expenses = [i.get('benefits_costs_expenses') if i is not None else 0.0 for i in income_statement]
        income_loss_from_equity_method_investments = [i.get('income_loss_from_equity_method_investments') if i is not None else 0.0 for i in income_statement]
        income_tax_expense_benefit = [i.get('income_tax_expense_benefit') if i is not None else 0.0 for i in income_statement]
        income_tax_expense_benefit_deferred = [i.get('income_tax_expense_benefit_deferred') if i is not None else 0.0 for i in income_statement]
        diluted_earnings_per_share = [i.get('diluted_earnings_per_share') if i is not None else 0.0 for i in income_statement]
        operating_income_loss = [i.get('operating_income_loss') if i is not None else 0.0 for i in income_statement]
        income_loss_from_continuing_operations_after_tax = [i.get('income_loss_from_continuing_operations_after_tax') if i is not None else 0.0 for i in income_statement]
        diluted_average_shares = [i.get('diluted_average_shares') if i is not None else 0.0 for i in income_statement]



        #balance sheet
        equity_attributable_to_parent = [i.get('equity_attributable_to_parent') if i else None for i in balance_sheet]
        assets = [i.get('assets') if i else None for i in balance_sheet]
        noncurrent_liabilities = [i.get('noncurrent_liabilities') if i else None for i in balance_sheet]
        other_noncurrent_assets = [i.get('other_noncurrent_assets') if i else None for i in balance_sheet]
        inventory = [i.get('inventory') if i else None for i in balance_sheet]
        equity = [i.get('equity') if i else None for i in balance_sheet]
        accounts_payable = [i.get('accounts_payable') if i else None for i in balance_sheet]
        long_term_debt = [i.get('long_term_debt') if i else None for i in balance_sheet]
        equity_attributable_to_noncontrolling_interest = [i.get('equity_attributable_to_noncontrolling_interest') if i else None for i in balance_sheet]
        fixed_assets = [i.get('fixed_assets') if i else None for i in balance_sheet]
        liabilities = [i.get('liabilities') if i else None for i in balance_sheet]
        noncurrent_assets = [i.get('noncurrent_assets') if i else None for i in balance_sheet]
        current_liabilities = [i.get('current_liabilities') if i else None for i in balance_sheet]
        other_current_liabilities = [i.get('other_current_liabilities') if i else None for i in balance_sheet]
        other_current_assets = [i.get('other_current_assets') if i else None for i in balance_sheet]
        current_assets = [i.get('current_assets') if i else None for i in balance_sheet]
        liabilities_and_equity = [i.get('liabilities_and_equity') if i else None for i in balance_sheet]
        other_noncurrent_liabilities = [i.get('other_noncurrent_liabilities') if i else None for i in balance_sheet]


        #comprehensive income
        comprehensive_income_loss_attributable_to_parent= [i.get('comprehensive_income_loss_attributable_to_parent') if i else None for i in balance_sheet]
        comprehensive_income_loss_attributable_to_noncontrolling_interest= [i.get('comprehensive_income_loss_attributable_to_noncontrolling_interest') if i else None for i in balance_sheet]
        comprehensive_income_loss= [i.get('comprehensive_income_loss') if i else None for i in balance_sheet]
        other_comprehensive_income_loss= [i.get('other_comprehensive_income_loss') if i else None for i in balance_sheet]


        #cashflow statement
        cash_flow_statement = [i.get('cash_flow_statement') for i in self.financials]
        net_cash_flow_from_financing_activities = [i.get('net_cash_flow_from_financing_activities', 0.0) for i in cash_flow_statement]
        net_cash_flow_from_investing_activities_continuing = [i.get('net_cash_flow_from_investing_activities_continuing', 0.0) for i in cash_flow_statement]
        net_cash_flow_continuing = [i.get('net_cash_flow_continuing', 0.0) for i in cash_flow_statement]
        net_cash_flow_from_operating_activities = [i.get('net_cash_flow_from_operating_activities', 0.0) for i in cash_flow_statement]
        net_cash_flow_from_investing_activities = [i.get('net_cash_flow_from_investing_activities', 0.0) for i in cash_flow_statement]
        net_cash_flow_from_financing_activities_continuing = [i.get('net_cash_flow_from_financing_activities_continuing', 0.0) for i in cash_flow_statement]
        net_cash_flow_from_operating_activities_continuing = [i.get('net_cash_flow_from_operating_activities_continuing', 0.0) for i in cash_flow_statement]
        net_cash_flow = [i.get('net_cash_flow', 0.0) for i in cash_flow_statement]


        
        self.value = [
            item.get('value') if isinstance(item, dict) else None
            for sublist in [
                equity_attributable_to_parent, assets, noncurrent_liabilities, other_current_assets, inventory, equity, accounts_payable, long_term_debt, equity_attributable_to_noncontrolling_interest, fixed_assets, liabilities, noncurrent_assets, current_liabilities, other_current_liabilities, other_current_assets, current_assets, liabilities_and_equity, other_noncurrent_liabilities, other_noncurrent_assets,
                # Add the new attributes here
                income_loss_from_continuing_operations_before_tax, basic_earnings_per_share, net_income_loss_attributable_to_parent, revenues, net_income_loss_attributable_to_noncontrolling_interest, net_income_loss_available_to_common_stockholders_basic, gross_profit, operating_expenses, preferred_stock_dividends_and_other_adjustments, cost_of_revenue, costs_and_expenses, basic_average_shares, interest_expense_operating, income_loss_before_equity_method_investments, nonoperating_income_loss, benefits_costs_expenses, income_loss_from_equity_method_investments, income_tax_expense_benefit, income_tax_expense_benefit_deferred, diluted_earnings_per_share, operating_income_loss, income_loss_from_continuing_operations_after_tax, diluted_average_shares, net_income_loss,
                #comp income
                comprehensive_income_loss_attributable_to_parent, comprehensive_income_loss_attributable_to_noncontrolling_interest, comprehensive_income_loss, other_comprehensive_income_loss,
                #netcashflow
                net_cash_flow_from_financing_activities, net_cash_flow_continuing, net_cash_flow_from_investing_activities_continuing, net_cash_flow_from_operating_activities, net_cash_flow_from_investing_activities, net_cash_flow_from_investing_activities_continuing, net_cash_flow_from_financing_activities_continuing, net_cash_flow_from_operating_activities_continuing, net_cash_flow
            ]
            for item in sublist
        ]
        self.unit = [
            item.get('unit') if isinstance(item, dict) else None
            for sublist in [
                equity_attributable_to_parent, assets, noncurrent_liabilities, other_current_assets, inventory, equity, accounts_payable, long_term_debt, equity_attributable_to_noncontrolling_interest, fixed_assets, liabilities, noncurrent_assets, current_liabilities, other_current_liabilities, other_current_assets, current_assets, liabilities_and_equity, other_noncurrent_liabilities, other_noncurrent_assets,
                # Add the new attributes here
                income_loss_from_continuing_operations_before_tax, basic_earnings_per_share, net_income_loss_attributable_to_parent, revenues, net_income_loss_attributable_to_noncontrolling_interest, net_income_loss_available_to_common_stockholders_basic, gross_profit, operating_expenses, preferred_stock_dividends_and_other_adjustments, cost_of_revenue, costs_and_expenses, basic_average_shares, interest_expense_operating, income_loss_before_equity_method_investments, nonoperating_income_loss, benefits_costs_expenses, income_loss_from_equity_method_investments, income_tax_expense_benefit, income_tax_expense_benefit_deferred, diluted_earnings_per_share, operating_income_loss, income_loss_from_continuing_operations_after_tax, diluted_average_shares, net_income_loss,
                #comp income
                comprehensive_income_loss_attributable_to_parent, comprehensive_income_loss_attributable_to_noncontrolling_interest, comprehensive_income_loss, other_comprehensive_income_loss,
                #netcashflow
                net_cash_flow_from_financing_activities, net_cash_flow_continuing, net_cash_flow_from_investing_activities_continuing, net_cash_flow_from_operating_activities, net_cash_flow_from_investing_activities, net_cash_flow_from_investing_activities_continuing, net_cash_flow_from_financing_activities_continuing, net_cash_flow_from_operating_activities_continuing, net_cash_flow
            ]
            for item in sublist
        ]

        self.label = [
            item.get('label') if isinstance(item, dict) else None
            for sublist in [
                equity_attributable_to_parent, assets, noncurrent_liabilities, other_current_assets, inventory, equity, accounts_payable, long_term_debt, equity_attributable_to_noncontrolling_interest, fixed_assets, liabilities, noncurrent_assets, current_liabilities, other_current_liabilities, other_current_assets, current_assets, liabilities_and_equity, other_noncurrent_liabilities, other_noncurrent_assets,
                # Add the new attributes here
                income_loss_from_continuing_operations_before_tax, basic_earnings_per_share, net_income_loss_attributable_to_parent, revenues, net_income_loss_attributable_to_noncontrolling_interest, net_income_loss_available_to_common_stockholders_basic, gross_profit, operating_expenses, preferred_stock_dividends_and_other_adjustments, cost_of_revenue, costs_and_expenses, basic_average_shares, interest_expense_operating, income_loss_before_equity_method_investments, nonoperating_income_loss, benefits_costs_expenses, income_loss_from_equity_method_investments, income_tax_expense_benefit, income_tax_expense_benefit_deferred, diluted_earnings_per_share, operating_income_loss, income_loss_from_continuing_operations_after_tax, diluted_average_shares, net_income_loss,
                #comp income
                comprehensive_income_loss_attributable_to_parent, comprehensive_income_loss_attributable_to_noncontrolling_interest, comprehensive_income_loss, other_comprehensive_income_loss,
                #netcashflow
                net_cash_flow_from_financing_activities, net_cash_flow_continuing, net_cash_flow_from_investing_activities_continuing, net_cash_flow_from_operating_activities, net_cash_flow_from_investing_activities, net_cash_flow_from_investing_activities_continuing, net_cash_flow_from_financing_activities_continuing, net_cash_flow_from_operating_activities_continuing, net_cash_flow
            ]
            for item in sublist
        ]

        self.order = [
            item.get('order') if isinstance(item, dict) else None
            for sublist in [
                equity_attributable_to_parent, assets, noncurrent_liabilities, other_current_assets, inventory, equity, accounts_payable, long_term_debt, equity_attributable_to_noncontrolling_interest, fixed_assets, liabilities, noncurrent_assets, current_liabilities, other_current_liabilities, other_current_assets, current_assets, liabilities_and_equity, other_noncurrent_liabilities, other_noncurrent_assets,
                # Add the new attributes here
                income_loss_from_continuing_operations_before_tax, basic_earnings_per_share, net_income_loss_attributable_to_parent, revenues, net_income_loss_attributable_to_noncontrolling_interest, net_income_loss_available_to_common_stockholders_basic, gross_profit, operating_expenses, preferred_stock_dividends_and_other_adjustments, cost_of_revenue, costs_and_expenses, basic_average_shares, interest_expense_operating, income_loss_before_equity_method_investments, nonoperating_income_loss, benefits_costs_expenses, income_loss_from_equity_method_investments, income_tax_expense_benefit, income_tax_expense_benefit_deferred, diluted_earnings_per_share, operating_income_loss, income_loss_from_continuing_operations_after_tax, diluted_average_shares, net_income_loss,
                #comp income
                comprehensive_income_loss_attributable_to_parent, comprehensive_income_loss_attributable_to_noncontrolling_interest, comprehensive_income_loss, other_comprehensive_income_loss,
                #netcashflow
                net_cash_flow_from_financing_activities, net_cash_flow_continuing, net_cash_flow_from_investing_activities_continuing, net_cash_flow_from_operating_activities, net_cash_flow_from_investing_activities, net_cash_flow_from_investing_activities_continuing, net_cash_flow_from_financing_activities_continuing, net_cash_flow_from_operating_activities_continuing, net_cash_flow
            ]
            for item in sublist
        ]

        fiscal_years = []
        fiscal_periods = []
        start_dates = []
        end_dates = []
        timeframes = []

        
        for i in range(len(self.label)):
            fiscal_year = self.fiscal_year[i % 10]  # Use modulo to cycle through the 10 fiscal years
            fiscal_period = self.fiscal_period[i % 10]
            end_date = self.end_date[i % 10]
            start_date = self.start_date[i % 10]
            timeframe = self.timeframe[i % 10]

            fiscal_years.append(fiscal_year)
            fiscal_periods.append(fiscal_period)
            start_dates.append(start_date)
            end_dates.append(end_date)
            timeframes.append(timeframe)

        print(len(fiscal_years), len(fiscal_periods), len(start_dates), len(end_dates))
        self.financials_data_dict = { 
            'fiscal_year': fiscal_years,
            'fiscal_period': fiscal_periods,
            'start_date': start_dates,
            'end_date': end_dates,
            'metric': self.label,
            'value': self.value,
            'unit': self.unit

        }


        self.as_financials_df = pd.DataFrame(self.financials_data_dict)
        self.as_financials_df = self.format_large_numbers_in_dataframe(self.as_financials_df).sort_values('end_date', ascending=False)
        if 'ticker' in self.kwargs:
            # Add the ticker as a new column
            self.as_financials_df['ticker'] = self.kwargs['ticker']
            



    def convert_timestamp(self, timestamp, from_milliseconds=True):
        try:
            if from_milliseconds:
                timestamp = float(timestamp) / 1000
            dt_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            return dt_object.astimezone(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            return f'Error: {e}'
        
    def make_polygon_request(self, endpoint, params):
        base_url = "https://api.polygon.io"
        api_key = os.environ.get('YOUR_POLYGON_KEY')
        full_url = f"{base_url}{endpoint}?apiKey={api_key}"
        for key, value in params.items():
            full_url += f"&{key}={value}"
        response = requests.get(full_url)
        return response.json().get('results')
        

    def get_rsi(self, ticker, timespan, limit):
        endpoint = f"/v1/indicators/rsi/{ticker}"
        params = {"timespan": timespan, "adjusted": "true", "window": 14, "order": "desc", "limit": limit}
        rsi_response = self.make_polygon_request(endpoint, params)
        return PolygonMaster(rsi_results=rsi_response, ticker=ticker)
    
    def get_macd(self, ticker, timespan, limit):
        endpoint = f"/v1/indicators/macd/{ticker}"
        params = {"timespan": timespan, "adjusted": "true", "series_type": "close", "order": "desc", "limit": limit}
        sma_response = self.make_polygon_request(endpoint, params)
        return PolygonMaster(macd_results=sma_response, ticker=ticker)
    
    def get_sma(self, ticker, timespan, window, limit):
        endpoint = f"/v1/indicators/sma/{ticker}"
        params = {"timespan": timespan, "adjusted": "true", "window": window, "series_type": "close", "order": "desc", "limit": limit}
        sma_response = self.make_polygon_request(endpoint, params)
        return PolygonMaster(sma_results=sma_response, ticker=ticker)

    def get_ema(self, ticker, timespan, window, limit):
        endpoint = f"/v1/indicators/ema/{ticker}"
        params = {"timespan": timespan, "adjusted": "true", "window": window, "series_type": "close", "order": "desc", "limit": limit}
        ema_response = self.make_polygon_request(endpoint, params)
        return PolygonMaster(ema_results=ema_response, ticker=ticker)

    def get_all_tickers_snapshot(self):
        endpoint = "/v2/snapshot/locale/us/markets/stocks/tickers"
        all_tickers_snapshot_response = self.make_polygon_request(endpoint, {})
        return PolygonMaster(all_stock_snapshot_results=all_tickers_snapshot_response)

    def get_aggregates(self,ticker, multiplier, timespan, date_from, date_to, limit):
        endpoint = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}"
        params = {"adjusted": "true", "sort": "asc", "limit": limit}
        aggregates_response = self.make_polygon_request(endpoint, params)
        return PolygonMaster(aggregates_results=aggregates_response, ticker=ticker)

    def get_ticker_news(self, ticker):
        endpoint = f"/v2/reference/news"
        params = {"ticker": ticker}
        ticker_news_response = self.make_polygon_request(endpoint, params)
        return PolygonMaster(ticker_news_results=ticker_news_response, ticker=ticker)

    def get_financials(self, ticker, timeframe):
        endpoint = f"/vX/reference/financials"
        params = {"ticker": ticker, "timeframe": timeframe}
        financials_response = self.make_polygon_request(endpoint, params)
        return PolygonMaster(financials_results=financials_response, ticker=ticker)
    

master = PolygonMaster()
opts = PolygonOptions(user='chuck', database='poly')
async def main():
    ticker='AAPL'
    timespan='day'
    multiplier=1
    date_to = '2023-12-31'
    date_from = '2023-12-01'
    limit=100
    rsi = master.get_rsi(ticker=ticker,timespan=timespan, limit=limit)
    
    aggs = master.get_aggregates(ticker,multiplier,timespan,date_from,date_to,limit)
    financials = master.get_financials(ticker,timeframe='quarterly')
    ema = master.get_ema(ticker,timespan,window,limit)
    sma = master.get_sma(ticker,timespan,window,limit)
    macd = master.get_macd(ticker,timespan,limit)
    news = master.get_ticker_news(ticker)
 

    await opts.connect()

    await opts.batch_insert_dataframe(rsi.rsi_as_dataframe,table_name='rsi', unique_columns='rsi_timespan, rsi_timestamp, ticker')
    await opts.batch_insert_dataframe(aggs.agg_as_dataframe, table_name='aggs', unique_columns='agg_timespan, agg_timestamp, ticker')
    await opts.batch_insert_dataframe(financials.as_financials_df, table_name='financials', unique_columns='ticker')
    await opts.batch_insert_dataframe(ema.ema_as_dataframe, table_name='ema', unique_columns='ticker')
    await opts.batch_insert_dataframe(sma.sma_as_dataframe, table_name='sma', unique_columns='ticker')
    await opts.batch_insert_dataframe(macd.macd_as_dataframe, table_name='macd', unique_columns='ticker')
    await opts.batch_insert_dataframe(news.ticker_news_as_dataframe, table_name='news', unique_columns='insertion_timestamp')
    #await opts.batch_insert_dataframe(snapshots.all_ticker_snapshots_as_dataframe, table_name='snapshot', unique_columns='insertion_timestamp')


asyncio.run(main())