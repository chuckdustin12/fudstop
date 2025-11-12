import pandas as pd




class IndexQuote:
    def __init__(self, data):


        self.name = [i.get('name') for i in data]
        self.symbol = [i.get('symbol') for i in data]
        self.close = [float(i.get('close')) for i in data]
        self.change = [float(i.get('change')) for i in data]
        self.changeRatio = [float(i.get('changeRatio')) for i in data]
        self.volume = [float(i.get('volume')) for i in data]
        self.netAsset = [float(i.get('netAsset')) for i in data]
        self.totalAsset = [float(i.get('totalAsset')) for i in data]
        self.dividend = [float(i.get('dividend')) for i in data]
        self._yield = [float(i.get('yield')) for i in data]



        self.data_dict = { 
            'name': self.name,
            'symbol': self.symbol,
            'close': self.close,
            'change': self.change,
            'change_pct': self.changeRatio,
            'volume': self.volume,
            'net_asset': self.netAsset,
            'total_asset': self.totalAsset,
            'dividend': self.dividend,
            'yield': self._yield,
            
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)