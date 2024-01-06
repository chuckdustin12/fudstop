import pandas as pd


class TickerInfo:
    def __init__(self, data):
        self.tickerId = [i['tickerId'] for i in data]
        self.exchangeId = [i['exchangeId'] for i in data]
        self.regionId = [i['regionId'] for i in data]
        self.symbol = [i['symbol'] for i in data]
        self.unSymbol = [i['unSymbol'] for i in data]
        self.tickerType = [i['tickerType'] for i in data]
        self.belongTickerId = [i['belongTickerId'] for i in data]
        self.direction = [i['direction'] for i in data]
        self.quoteLotSize = [i['quoteLotSize'] for i in data]
        self.expireDate = [i['expireDate'] for i in data]
        self.strikePrice = [i['strikePrice'] for i in data]
        self.change = [i['change'] for i in data]
        self.changeRatio = [i['changeRatio'] for i in data]
        self.quoteMultiplier = [i['quoteMultiplier'] for i in data]
        self.cycle = [i['cycle'] for i in data]

        self.data_dict = { 
            'tickerId': self.tickerId,
            'exchangeId': self.exchangeId,
            'regionId': self.regionId,
            'symbol': self.symbol,
            'unSymbol': self.unSymbol,
            'tickerType': self.tickerType,
            'belongTickerId': self.belongTickerId,
            'direction': self.direction,
            'quoteLotSize': self.quoteLotSize,
            'expireDate': self.expireDate,
            'strikePrice': self.strikePrice,
            'change': self.change,
            'changeRatio': self.changeRatio,
            'quoteMultiplier': self.quoteMultiplier,
            'cycle': self.cycle
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)



class ScreenerRule:
    def __init__(self, data):
        self.change = [i['options.screener.rule.change'] for i in data]
        self.expireDate = [i['options.screener.rule.expireDate'] for i in data]
        self.ask = [i['options.screener.rule.ask'] for i in data]
        self.openInterest = [i['options.screener.rule.openInterest'] for i in data]
        self.otm = [i['options.screener.rule.otm'] for i in data]
        self.tobep = [i['options.screener.rule.tobep'] for i in data]
        self.changeRatio = [i['options.screener.rule.changeRatio'] for i in data]
        self.volume = [i['options.screener.rule.volume'] for i in data]
        self.itm = [i['options.screener.rule.itm'] for i in data]
        self.implVol = [i['options.screener.rule.implVol'] for i in data]
        self.close = [i['options.screener.rule.close'] for i in data]
        self.bid = [i['options.screener.rule.bid'] for i in data]

        self.data_dict = { 
            'options.screener.rule.change': self.change,
            'options.screener.rule.expireDate': self.expireDate,
            'options.screener.rule.ask': self.ask,
            'options.screener.rule.openInterest': self.openInterest,
            'options.screener.rule.otm': self.otm,
            'options.screener.rule.tobep': self.tobep,
            'options.screener.rule.changeRatio': self.changeRatio,
            'options.screener.rule.volume': self.volume,
            'options.screener.rule.itm': self.itm,
            'options.screener.rule.implVol': self.implVol,
            'options.screener.rule.close': self.close,
            'options.screener.rule.bid': self.bid
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)