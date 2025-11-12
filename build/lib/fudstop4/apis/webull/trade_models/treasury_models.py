import pandas as pd



class TreasuryData:
    def __init__(self, data):

        self.tickerId = [i.get('tickerId') for i in data]
        self.symbol = [i.get('symbol') for i in data]
        self.isin = [i.get('isin') for i in data]
        self.disSymbol = [i.get('disSymbol') for i in data]
        self.fullName = [i.get('fullName') for i in data]
        self.name = [i.get('name') for i in data]
        self.expDate = [i.get('expDate') for i in data]
        self.parValue = [i.get('parValue') for i in data]
        self.coupon = [i.get('coupon') for i in data]
        self.accruedInterest = [i.get('accruedInterest') for i in data]
        self.couponFrequency = [i.get('couponFrequency') for i in data]
        self.couponFreqDesc = [i.get('couponFreqDesc') for i in data]
        self.term = [i.get('term') for i in data]
        self.close = [i.get('close') for i in data]
        self.preClose = [i.get('preClose') for i in data]
        self.change = [i.get('change') for i in data]
        self.changeRatio = [i.get('changeRatio') for i in data]
        self.bondYield = [i.get('bondYield') for i in data]
        self.askPrice = [i.get('askPrice') for i in data]
        self.bidPrice = [i.get('bidPrice') for i in data]
        self.askMinSize = [i.get('askMinSize') for i in data]
        self.bidMinSize = [i.get('bidMinSize') for i in data]
        self.askVolume = [i.get('askVolume') for i in data]
        self.bidVolume = [i.get('bidVolume') for i in data]
        self.askYield = [i.get('askYield') for i in data]

        self.data_dict = { 
            'symbol': self.symbol,
            'isin':self.isin,
            'full_name': self.fullName,
            'name': self.name,
            'expiry': self.expDate,
            'par_value': self.parValue,
            'coupon': self.coupon,
            'accrued_interest': self.accruedInterest,
            'coupon_frequency': self.couponFrequency,
            'coupon_frequency_desc': self.couponFreqDesc,
            'term': self.term,
            'close': self.close,
            'pre_close': self.preClose,
            'change': self.change,
            'change_ratio': self.changeRatio,
            'bond_yield': self.bondYield,
            'ask_price': self.askPrice,
            'bid_price': self.bidPrice,
            'ask_minsize': self.askMinSize,
            'bid_minsize': self.bidMinSize,
            'ask_volume': self.askVolume,
            'bid_volume': self.bidVolume,
            'ask_yield': self.askYield
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)