import pandas as pd
from datetime import datetime

import pytz

class Gex:
    def __init__(self, data):
        self.timestamp = data.get('timestamp')
        self.ticker = data.get('ticker')
        self.spot = data.get('spot')
        self.zero_gamma = data.get('zero_gamma')
        self.major_pos_vol = data.get('major_pos_vol')
        self.major_pos_oi = data.get('major_pos_oi')
        self.major_neg_vol = data.get('major_neg_vol')
        self.major_neg_oi = data.get('major_neg_oi')
        self.sum_gex_vol = data.get('sum_gex_vol')
        self.sum_gex_oi = data.get('sum_gex_oi')
        self.delta_risk_reversal = data.get('delta_risk_reversal')
        self.max_priors = data.get('max_priors')


        self.data_dict = { 

            'timestamp': self.timestamp,
            'ticker': self.ticker,
            'spot': self.spot,
            'zero_gamma': self.zero_gamma,
            'major_pos_vol': self.major_pos_vol,
            'major_neg_vol': self.major_neg_vol,
            'majoir_pos_oi': self.major_pos_oi,
            'major_neg_oi': self.major_neg_oi,
            'sum_gex_vol': self.sum_gex_vol,
            'sum_gex_oi': self.sum_gex_oi,
            'delta_risk_reversal': self.delta_risk_reversal,

        }


        self.as_dataframe = pd.DataFrame(self.data_dict, index=[self.ticker]).transpose()


from datetime import timedelta
class GexMajorLevels:
    def __init__(self, data):
        try:
            self.timestamp = data.get('timestamp')
            # Parse the string to a datetime object, removing the timezone information
            timestamp = datetime.strptime(self.timestamp.split('+')[0].strip(), '%Y-%m-%d %H:%M:%S.%f')

            # Subtract 5 hours to convert to Eastern Time (not considering Daylight Saving Time)
            self.eastern_timestamp = timestamp - timedelta(hours=5)
            self.ticker = data.get('ticker')
            self.spot = data.get('spot')
            self.mpos_vol = data.get('mpos_vol')
            self.mneg_vol = data.get('mneg_vol')
            self.mpos_oi = data.get('mpos_oi')
            self.mneg_oi = data.get('mneg_oi')
            self.zero_gamma = data.get('zero_gamma')
            self.net_gex_vol = data.get('net_gex_vol')
            self.net_gex_oi = data.get('net_gex_oi')

            self.data_dict = { 

                'timestamp': self.eastern_timestamp,
                'ticker': self.ticker,
                'spot': self.spot,
                'major_pos_vol': self.mpos_vol,
                'major_pos_oi': self.mpos_oi,
                'major_neg_vol': self.mneg_vol,
                'major_neg_oi': self.mneg_oi,
                'zero_gamma': self.zero_gamma,
                'net_gex_vol': self.net_gex_vol,
                'net_gex_oi': self.net_gex_oi
            }


            self.as_dataframe = pd.DataFrame(self.data_dict, index=[self.ticker]).transpose()

        except AttributeError:
            print(f'Error')



    def convert_to_datetime(self,date_string):
        # Assuming date_string is in the format 'YYYY-MM-DD HH:MM:SS.SSS +00:00'
        # Parse the string to a datetime object
        dt = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f %z')
        # Convert to a timezone-aware datetime object (UTC)
        dt = dt.astimezone(pytz.utc)
        # Get the Unix timestamp
        unix_timestamp = int(dt.timestamp())
        return unix_timestamp



class MaxGex:
    def __init__(self, data):
        self.timestamp = data.get('timestamp')
        self.ticker = data.get('ticker')
        self.current = data.get('current')
        self.one = data.get('one')
        self.five = data.get('five')
        self.ten = data.get('ten')
        self.fifteen = data.get('fifteen')
        self.thirty = data.get('thirty')


        self.data_dict = { 

            'timestamp': self.timestamp,
            'ticker': self.ticker,
            'current': self.current,
            'one': self.one,
            'five': self.five,
            'ten': self.ten,
            'fifteen': self.fifteen,
            'thirty': self.thirty
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)