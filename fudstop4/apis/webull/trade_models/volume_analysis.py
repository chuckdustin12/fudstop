import requests
import pandas as pd
session = requests.session()
class WebullVolAnalysis:
    
    def __init__(self, r, ticker):
        try:
            self.ticker=ticker
            self.totalNum = r['totalNum'] if 'totalNum' in r else 0
            self.totalVolume = r['totalVolume'] if 'totalVolume' in r else 0
            self.avePrice = r['avePrice'] if 'avePrice' in r else 0
            self.buyVolume = r['buyVolume'] if 'buyVolume' in r else 0
            self.sellVolume = r['sellVolume'] if 'sellVolume' in r else 0
            self.nVolume = r['nVolume'] if 'nVolume' in r else 0


            if self.buyVolume != 0 and self.sellVolume != 0 and self.nVolume != 0:
                self.buyPct = (self.buyVolume / self.totalVolume) * 100
                self.sellPct = (self.sellVolume / self.totalVolume) * 100
                self.nPct = (self.nVolume / self.totalVolume) * 100

                self.data_dict = { 
                    'ticker': self.ticker,
                    'avg_price': self.avePrice,
                    'total_num': self.totalNum,
                    'total_volume': self.totalVolume,
                    'buy_volume': self.buyVolume,
                    'sell_volume': self.sellVolume,
                    'neut_volume': self.nVolume,
                    'buy_pct': self.buyPct,
                    'sell_pct': self.sellPct,
                    'neut_pct': self.nPct
                }


                self.df = pd.DataFrame(self.data_dict, index=[0])
        except Exception as e:
            print(e)

