import pandas as pd



class WebullContractData:
    def __init__(self, data):
        try:
            self.open = [float(i.get('open')) for i in data]
            self.high = [float(i.get('high')) for i in data]
            self.low = [float(i.get('low')) for i in data]
            self.strikePrice = [float(i.get('strikePrice')) for i in data]
            self.preClose = [float(i.get('preClose')) for i in data]
            self.openInterest = [float(i.get('openInterest')) for i in data]
            self.volume = [float(i.get('volume')) for i in data]
            self.latestPriceVol = [float(i.get('latestPriceVol')) for i in data]
            self.delta = [float(i.get('delta')) for i in data]
            self.vega = [float(i.get('vega')) for i in data]
            self.impVol = [round(float(i.get('impVol')*100,2)) for i in data]
            self.gamma = [float(i.get('gamma')) for i in data]
            self.theta = [i.get(float('theta')) for i in data]
            self.rho = [float(i.get('rho')) for i in data]
            self.close = [float(i.get('close')) for i in data]
            self.change = [float(i.get('change')) for i in data]
            self.changeRatio = [round(float(i.get('changeRatio')*100,3)) for i in data]
            self.expireDate = [i.get('expireDate') for i in data]
            self.tickerId = [float(i.get('tickerId')) for i in data]
            self.belongTickerId = [int(i.get('belongTickerId')) for i in data]
            self.openIntChange = [float(i.get('openIntChange')) for i in data]
            self.activeLevel = [float(i.get('activeLevel')) for i in data]
            self.direction = [i.get('direction') for i in data]
            self.symbol = [i.get('symbol') for i in data]
            self.unSymbol = [i.get('unSymbol') for i in data]


            self.data_dict = { 
                'option_id': self.tickerId,
                'stock_id': self.belongTickerId,
                'ticker': self.unSymbol,
                'strike': self.strikePrice,
                'call_put': self.direction,
                'expiry': self.expireDate,
                'volume': self.volume,
                'latest_vol': self.latestPriceVol,
                'activity_level': self.activeLevel,
                'oi': self.openInterest,
                'oi_change': self.openIntChange,
                'open': self.open,
                'high': self.high,
                'low': self.low,
                'close': self.close,
                'pre_close': self.preClose,
                'change': self.change,
                'change_ratio': self.changeRatio,
                'delta': self.delta,
                'gamma': self.gamma,
                'theta': self.theta,
                'vega': self.vega,
                'rho': self.rho,
                'iv': self.impVol,

                
            }
            self.as_dataframe = pd.DataFrame(self.data_dict)
        except Exception as e:
            print(e)
        