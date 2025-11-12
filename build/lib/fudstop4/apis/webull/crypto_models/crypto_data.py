import pandas as pd




class WebullCryptoData:
    def __init__(self, data):


        self.tickerId = [i.get('tickerId') for i in data]
        self.name = [i.get('name') for i in data]
        self.symbol = [i.get('symbol') for i in data]

        self.tradeTime = [i.get('tradeTime') for i in data]
        self.close = [float(i.get('close')) for i in data]
        self.change = [float(i.get('change')) for i in data]
        self.changeRatio = [i.get('changeRatio') for i in data]
        self._yield = [float(i['yield']) if i.get('yield') is not None else 0.0 for i in data]
        self.preClose = [float(i.get('preClose')) for i in data]
        self.fiftyTwoWkHigh = [float(i.get('fiftyTwoWkHigh')) for i in data]
        self.fiftyTwoWkLow = [float(i.get('fiftyTwoWkLow')) for i in data]
        self.open = [float(i.get('open')) for i in data]
        self.high = [float(i.get('high')) for i in data]
        self.low = [float(i.get('low')) for i in data]
        self.pe = [float(i['pe']) if i.get('pe') is not None else 0.0 for i in data]

        self.vibrateRatio = [float(i.get('vibrateRatio')) for i in data]

        self.data_dict = { 
            'ticker_id': self.tickerId,
            'name': self.name,
            'symbol': self.symbol,
            'trade_time': self.tradeTime,
            'close': self.close,
            'change': self.change,
            'change_ratio': self.changeRatio,
            'yield': self._yield,
            'pre_close': self.preClose,
            'fifty_two_wk_high': self.fiftyTwoWkHigh,
            'fifty_two_wk_low': self.fiftyTwoWkLow,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'pe': self.pe,
            'vibrate_ratio': self.vibrateRatio
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)