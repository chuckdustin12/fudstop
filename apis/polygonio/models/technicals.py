from datetime import datetime, timezone
import pytz

import pandas as pd

class RSI:
    def __init__(self, data, ticker):
        self.as_dataframe = None
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
        results = data['results']
        values = results.get('values')

        self.ema_value = [i.get('value') for i in values]
        self.ema_timestamp = [i.get('timestamp') for i in values]

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
        results = data['results']
        values = results.get('values')

        self.sma_value = [i.get('value') for i in values]
        self.sma_timestamp = [i.get('timestamp') for i in values]

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