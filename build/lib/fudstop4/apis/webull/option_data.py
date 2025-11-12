import pandas as pd



class VolumeAnalysisDatas:
    def __init__(self, datas):
        try:
            self.price = [i.get('price') for i in datas]
            self.buy = [i.get('buy') for i in datas]
            self.sell = [i.get('sell') for i in datas]
            self.volume = [i.get('volume') for i in datas]
            self.ratio = [i.get('ratio') for i in datas]

            self.data_dict = { 
                'price': self.price,
                'buy': self.buy,
                'sell': self.sell,
                'volume': self.volume,
                'ratio': self.ratio
            }


            self.as_dataframe = pd.DataFrame(self.data_dict)
        except Exception as e:
            print(e)



class OptionDataFromIDs:
    def __init__(self, data):
        try:

            self.open = [float(i.get('open',0)) for i in data]
            self.high = [float(i.get('high',0)) for i in data]
            self.low = [float(i.get('low',0)) for i in data]
            self.strikePrice = [float(i.get('strikePrice',0)) for i in data]
            self.openInterest = [float(i.get('openInterest',0)) for i in data]
            self.volume = [float(i.get('volume',0)) for i in data]
            self.latestPriceVol = [float(i.get('latestPriceVol',0)) for i in data]
            self.delta = [float(i.get('delta',0)) for i in data]
            self.vega = [float(i.get('vega',0)) for i in data]
            self.impVol = [round(float(i.get('impVol',0))*100,2) for i in data]
            self.gamma = [float(i.get('gamma',0)) for i in data]
            self.theta = [float(i.get('theta',0)) for i in data]
            self.rho = [float(i.get('rho',0)) for i in data]
            self.close = [float(i.get('close',0)) for i in data]
            self.change = [float(i.get('change',0)) for i in data]
            self.changeRatio = [round(float(i.get('changeRatio',0))*100,2) for i in data]
            self.expireDate = [i.get('expireDate') for i in data]
            self.tickerId = [float(i.get('tickerId',0)) for i in data]
            self.belongTickerId = [float(i.get('belongTickerId',0)) for i in data]
            self.openIntChange = [i.get('openIntChange',0) for i in data]
            self.activeLevel = [float(i.get('activeLevel',0)) for i in data]
            self.cycle = [i.get('cycle') for i in data]
            self.weekly = [i.get('weekly') for i in data]
            self.direction = [i.get('direction') for i in data]
            self.symbol = ["O:" + i.get('symbol') for i in data]
            self.unSymbol = [i.get('unSymbol') for i in data]
            askList = [i.get('askList') for i in data]
            askList = [item for sublist in askList for item in sublist]
            bidList = [i.get('bidList') for i in data]
            bidList = [item for sublist in bidList for item in sublist]


            self.askPrice = [i.get('price') for i in askList]
            self.askVolume = [i.get('volume') for i in askList]
            self.askEx = [i.get('quoteEx') for i in askList]

            self.bidPrice = [i.get('price') for i in bidList]
            self.bidVolume = [i.get('volume') for i in bidList]
            self.bidEx = [i.get('quoteEx') for i in bidList]

            self.data_dict = { 
                'option_id': self.tickerId[0],
                'ticker_id': self.belongTickerId[0],
                'option_symbol': self.symbol[0],
                'ticker': self.unSymbol[0],
                'strike': self.strikePrice[0],
                'call_put': self.direction[0],
                'expiry': self.expireDate[0],
                'open': self.open[0],
                'high': self.high[0],
                'low': self.low[0],
                'close': self.close[0],
                'volume': self.volume[0],
                'oi': self.openInterest[0],
                'oi_change': self.openIntChange[0],
                'delta': self.delta[0],
                'gamma': self.gamma[0],
                'rho': self.rho[0],
                'theta': self.theta[0],
                'vega': self.vega[0],
                'iv': self.impVol[0],
                'last_vol': self.latestPriceVol[0],
                'bid_price': self.bidPrice[0],
                'bid_vol': self.bidVolume[0],
                'bid_ex': self.bidEx[0],
                'ask_price': self.askPrice[0],
                'ask_vol': self.askVolume[0],
                'ask_ex': self.askEx[0],
                'change': self.change[0],
                'change_pct': self.changeRatio[0],
                'cycle': self.cycle[0],
                'weekly': self.weekly[0],
                'activity_level': self.activeLevel[0]
                


            }

            self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])
        except Exception as e:
            print(e)