import pandas as pd




class PriceStreamer:
    def __init__(self, data):
        self.totalNum = data.get('totalNum')
        self.totalVolume = data.get('totalVolume')
        self.avePrice = data.get('avePrice')
        self.buyVolume = data.get('selfbuyVolume')
        self.sellVolume = data.get('sellVolume')
        self.nVolume = data.get('nVolume')
        self.preClose = data.get('preClose')
        self.dates = data.get('dates')
        self.maxT = data.get('maxT')
        stats = data.get('stats')
        #self.tradeTimeTs = to_datetime_eastern(data.get('tradeTimeTs'))

        self.price = [i.get('price') for i in stats]
        self.b = [i.get('b') for i in stats]
        self.s = [i.get('s') for i in stats]
        self.n = [i.get('n') for i in stats]
        self.t = [i.get('t') for i in stats]
        self.ratio = [i.get('ratio') for i in stats]




        self.data_dict = { 
            'price': self.price,
            'buy': self.b,
            'sell': self.s,
            'neutral': self.n,
            'total': self.t,
            'ratio': self.ratio,
        }



        self.as_df = pd.DataFrame(data)







