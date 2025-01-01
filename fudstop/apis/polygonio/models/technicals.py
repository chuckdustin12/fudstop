from datetime import datetime, timezone
import pytz

import pandas as pd

class RSI:
    def __init__(self, data, ticker):

        if 'results' in data:
            results = data['results']
            values = results.get('values', None)
            self.ticker=ticker
            self.rsi_value = [i.get('value', None) for i in values] if values and isinstance(values, list) and any('value' in i for i in values) else None
            self.rsi_timestamp = [i.get('timestamp', None) for i in values] if values and isinstance(values, list) and any('value' in i for i in values) else None
            if self.rsi_timestamp is not None:
                # Convert timestamps to human-readable Eastern Time
                self.rsi_timestamp = self.convert_to_human_readable(self.rsi_timestamp)

                self.data_dict = { 

                    'ticker': ticker,

                    'rsi_value': self.rsi_value,

                    'rsi_timestamp': self.rsi_timestamp
                }


                self.as_dataframe = pd.DataFrame(self.data_dict)

            underlying = results.get('underlying', None)
            aggregates = underlying['aggregates']

            ticker = [i.get("T") for i in aggregates]
            v = [i.get("v") for i in aggregates]
            vw = [i.get("vw") for i in aggregates]
            o = [i.get("o") for i in aggregates]
            c = [i.get("c") for i in aggregates]
            h = [i.get("h") for i in aggregates]
            l = [i.get("l") for i in aggregates]
            t = [i.get("t") for i in aggregates]
            n = [i.get("n") for i in aggregates]
            self.underlying_timestamp = self.convert_to_human_readable(t)

            self.underlying_dict = { 
                'ticker': ticker,
                'volume': v,
                'vwap': vw,
                'open': o,
                'high': h,
                'low': l,
                'close': c,
                'timestamp': self.underlying_timestamp,
                'num_trades': n
            }


            self.underlying_df = pd.DataFrame(self.underlying_dict)

            
    def convert_to_human_readable(self, timestamps):
        human_readable_times = []
        eastern = pytz.timezone('US/Eastern')
        if timestamps is not None:
            for ts in timestamps:
                if ts is not None:
                    try:
                        ts /= 1000  # Convert from milliseconds to seconds
                        dt_utc = datetime.fromtimestamp(ts, timezone.utc)
                        dt_eastern = dt_utc.astimezone(eastern)
                        human_readable_times.append(dt_eastern.strftime('%Y-%m-%d %H:%M:%S'))
                    except Exception as e:
                        print(f"Failed to convert timestamp {ts}: {e}")
                else:
                    human_readable_times.append(None)  # or some default value
            return human_readable_times
    

    
    



from datetime import datetime, timezone
import pytz

import pandas as pd

class EMA:
    def __init__(self, data, ticker):
        if data is not None:
            results = data['results'] if 'results' in data else None
            if results is not None:
                values = results.get('values')

                self.ema_value = [i.get('value') for i in values] if values is not None else []
                self.ema_timestamp = [i.get('timestamp') for i in values] if values is not None else []

                # Convert timestamps to human-readable Eastern Time
                self.ema_timestamp = self.convert_to_human_readable(self.ema_timestamp)

                self.data_dict = { 
                    'ticker': ticker,

                    'ema_value': self.ema_value,

                    'ema_timestamp': self.ema_timestamp
                }


                self.as_dataframe = pd.DataFrame(self.data_dict)
                
    def convert_to_human_readable(self, timestamps):
        human_readable_times = []
        eastern = pytz.timezone('US/Eastern')
        for ts in timestamps:
            if ts is not None:
                try:
                    ts /= 1000  # Convert from milliseconds to seconds
                    dt_utc = datetime.fromtimestamp(ts, timezone.utc)
                    dt_eastern = dt_utc.astimezone(eastern)
                    human_readable_times.append(dt_eastern.strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    print(f"Failed to convert timestamp {ts}: {e}")
            else:
                human_readable_times.append(None)  # or some default value
        return human_readable_times
    


class SMA:
    def __init__(self, data, ticker):
        if data is not None:
            results = data['results']
            values = results.get('values')

            self.sma_value = [i.get('value') for i in values] if values is not None else []
            self.sma_timestamp = [i.get('timestamp') for i in values] if values is not None else []

            # Convert timestamps to human-readable Eastern Time
            self.sma_timestamp = self.convert_to_human_readable(self.sma_timestamp)

            self.data_dict = { 
                'ticker': ticker,
                'sma_value': self.sma_value,

                'sma_timestamp': self.sma_timestamp
            }


            self.as_dataframe = pd.DataFrame(self.data_dict)
        
    def convert_to_human_readable(self, timestamps):
        human_readable_times = []
        eastern = pytz.timezone('US/Eastern')
        for ts in timestamps:
            if ts is not None:
                try:
                    ts /= 1000  # Convert from milliseconds to seconds
                    dt_utc = datetime.fromtimestamp(ts, timezone.utc)
                    dt_eastern = dt_utc.astimezone(eastern)
                    human_readable_times.append(dt_eastern.strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    print(f"Failed to convert timestamp {ts}: {e}")
            else:
                human_readable_times.append(None)  # or some default value
        return human_readable_times
    


class MACD:
    def __init__(self, data, ticker):
        results = data.get('results', {})
        values = results.get('values', [])

        # Extract MACD values and timestamps
        self.macd_value = [i.get('value') for i in values] if values is not None else []
        self.macd_timestamp = [i.get('timestamp') for i in values] if values is not None else []
        self.macd_histogram = [i.get('histogram') for i in values] if values is not None else []
        self.macd_signal = [i.get('signal') for i in values] if values is not None else []

        # Convert timestamps to human-readable Eastern Time
        self.macd_timestamp = self.convert_to_human_readable(self.macd_timestamp)

        # Prepare data dictionary for DataFrame
        self.data_dict = {
            'ticker': ticker,
            'value': self.macd_value,
            'time': self.macd_timestamp,
            'signal': self.macd_signal,
            'hist': self.macd_histogram
        }

        # Create DataFrame
        self.as_dataframe = pd.DataFrame(self.data_dict)
        
    def convert_to_human_readable(self, timestamps):
        human_readable_times = []
        eastern = pytz.timezone('US/Eastern')
        for ts in timestamps:
            if ts is not None:
                try:
                    ts /= 1000  # Convert from milliseconds to seconds
                    dt_utc = datetime.fromtimestamp(ts, timezone.utc)
                    dt_eastern = dt_utc.astimezone(eastern)
                    human_readable_times.append(dt_eastern.strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    print(f"Failed to convert timestamp {ts}: {e}")
            else:
                human_readable_times.append(None)  # or some default value
        return human_readable_times