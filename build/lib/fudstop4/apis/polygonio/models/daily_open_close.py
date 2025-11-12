import pandas as pd
from fudstop4.apis.helpers import get_human_readable_string


class DailyOpenClose:
    def __init__(self, data):
        try:
            self.ah = data.get('afterHours', 0)
            self.pm = data.get('preMarket', 0)
            self.close = data.get('close', 0)
            self.low = data.get('low', 0)
            self.high = data.get('high', 0)
            self.open = data.get('open', 0)
            self.date = data.get('from', 'N/A')
            self.symbol = data.get('symbol')
            components = get_human_readable_string(self.symbol)
            self.ticker = components.get('underlying_symbol')
            self.strike = components.get('strike_price')
            self.expiry = components.get('expiry_date')
            self.call_put = components.get('call_put')
            self.volume = data.get('volume', 0)

            self.data_dict = { 
                'option_symbol': self.symbol,
                'ticker': self.ticker,
                'strike': self.strike,
                'call_put': self.call_put,
                'expiry': self.expiry,
                'pm': self.pm,
                'open': self.open,
                'high': self.high,
                'low': self.low,
                'close': self.close,
                'ah': self.ah,
                'volume': self.volume,
                'date': self.date

            }
            
            self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])
            self.as_dataframe = self.as_dataframe.drop_duplicates()
        except Exception as e:
            print(e)