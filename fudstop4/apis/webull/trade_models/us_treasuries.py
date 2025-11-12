import pandas as pd



class US_TREASURIES:
    def __init__(self, data):
        self.tickerId = float(data.get('tickerId', 0))
        self.symbol = data.get('symbol', 0)
        self.name = data.get('name', 0)
        self.fullName = data.get('fullName', 0)
        self.close = float(data.get('close', 0))
        self.change = float(data.get('change', 0))
        self.changeRatio = float(data.get('changeRatio', 0))
        self.open = float(data.get('open', 0))
        self.high = float(data.get('high', 0))
        self.low = float(data.get('low', 0))
        self.bondYield = float(data.get('bondYield', 0))
        self.askList = data.get('askList', 0)
        self.bidList = data.get('bidList', 0)
        self.tradeTime = data.get('tradeTime', 0)