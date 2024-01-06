import requests
import pandas as pd
session = requests.session()
class WebullVolAnalysis:
    
    def __init__(self, response):

    
        
        # Check if the response JSON contains the required keys
        if all(key in response for key in ('avePrice', 'buyVolume', 'sellVolume', 'nVolume', 'totalVolume')):
            self.avePrice = response.get('avePrice', None)
            self.buyVolume = response.get('buyVolume', None)
            self.sellVolume = response.get('sellVolume', None)
            self.nVolume = response.get('nVolume', None)
            self.totalVolume = response.get('totalVolume', None)
            self.buyPct = (self.buyVolume / self.totalVolume) * 100
            self.sellPct = (self.sellVolume / self.totalVolume) * 100
            self.nPct = (self.nVolume / self.totalVolume) * 100

            self.data_dict = { 

                'avg_traded_price': self.avePrice,
                'buy_volume': self.buyVolume,
                'sell_volume': self.sellVolume,
                'neutral_volume': self.nVolume,
                'total_volume': self.totalVolume,
                'buy_pct': self.buyPct,
                'neutral_pct': self.nPct,
                'sell_pct': self.sellPct
            }

            self.df = pd.DataFrame(self.data_dict, index=[0]).transpose()
        else:
            # If the required keys are not present, set the attribute values to None
            self.avePrice = 0
            self.buyVolume = 0
            self.sellVolume = 0
            self.nVolume = 0
            self.totalVolume = 0
            self.buyPct = 0
            self.sellPct = 0
            self.nPct = 0

