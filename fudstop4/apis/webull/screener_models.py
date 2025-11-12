import pandas as pd


import pandas as pd
import asyncio


class ScreenerResults:
    def __init__(self, data):
        self.total = data.get('total')
        self.fetch = data.get('fetch')
        self.nextFetch = data.get('nextFetch')
        self.rules = data.get('rules')
        items = data.get('items')
        self.tickerIdList = data.get('tickerIdList')

        ticker = [i.get('ticker') for i in items]
        self.newly = [i.get('newly') for i in items]


        self.tickerId = [self.safe_float(i.get('tickerId')) for i in ticker]
        self.name = [i.get('name') for i in ticker]  # Assuming 'name' should not be converted to float
        self.symbol = [i.get('symbol') for i in ticker]  # Assuming 'symbol' should not be converted to float
        self.close = [self.safe_float(i.get('close')) for i in ticker]
        self.change = [self.safe_float(i.get('change')) for i in ticker]
        self.changeRatio = [round(self.safe_float(i.get('changeRatio')) * 100, 2) if i.get('changeRatio') is not None else None for i in ticker]
        self.marketValue = [self.safe_float(i.get('marketValue')) for i in ticker]
        self.volume = [self.safe_float(i.get('volume')) for i in ticker]
        self.turnoverRate = [self.safe_float(i.get('turnoverRate')) for i in ticker]
        self.peTtm = [self.safe_float(i.get('peTtm')) for i in ticker]
        self.dividend = [self.safe_float(i.get('dividend')) for i in ticker]
        self.fiftyTwoWkHigh = [self.safe_float(i.get('fiftyTwoWkHigh')) for i in ticker]
        self.fiftyTwoWkLow = [self.safe_float(i.get('fiftyTwoWkLow')) for i in ticker]
        self.open = [self.safe_float(i.get('open')) for i in ticker]
        self.high = [self.safe_float(i.get('high')) for i in ticker]
        self.low = [self.safe_float(i.get('low')) for i in ticker]
        self.vibrateRatio = [self.safe_float(i.get('vibrateRatio')) for i in ticker]

        self.data_dict = {
                    'tickerId': self.tickerId,
                    'name': self.name,
                    'symbol': self.symbol,
                    'close': self.close,
                    'change': self.change,
                    'changeRatio': self.changeRatio,
                    'marketValue': self.marketValue,
                    'volume': self.volume,
                    'turnoverRate': self.turnoverRate,
                    'peTtm': self.peTtm,
                    'dividend': self.dividend,
                    'fiftyTwoWkHigh': self.fiftyTwoWkHigh,
                    'fiftyTwoWkLow': self.fiftyTwoWkLow,
                    'open': self.open,
                    'high': self.high,
                    'low': self.low,
                    'vibrateRatio': self.vibrateRatio
                }

        self.as_dataframe = pd.DataFrame(self.data_dict)




    def safe_float(self, value):
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None



class OptionScreenerResults:
    def __init__(self, data):
        derivative = [i.get('derivative') for i in data]

        self.tickerId = [i.get('tickerId') for i in derivative]
        self.symbol = [i.get('symbol') for i in derivative]
        self.unSymbol = [i.get('unSymbol') for i in derivative]
        self.tickerType = [i.get('tickerType') for i in derivative]
        self.belongTickerId = [i.get('belongTickerId') for i in derivative]
        self.direction = [i.get('direction') for i in derivative]
        self.quoteLotSize = [i.get('quoteLotSize') for i in derivative]
        self.expireDate = [i.get('expireDate') for i in derivative]
        self.strikePrice = [i.get('strikePrice') for i in derivative]
        self.change = [i.get('change') for i in derivative]
        self.changeRatio = [i.get('changeRatio') for i in derivative]
        self.quoteMultiplier = [i.get('quoteMultiplier') for i in derivative]
        self.cycle = [i.get('cycle') for i in derivative]
        values = [i.get('values') for i in data]
        self.screener_change = [i.get('options.screener.rule.change') for i in values]
        self.screener_expireDate = [i.get('options.screener.rule.expireDate') for i in values]
        self.screener_ask = [i.get('options.screener.rule.ask') for i in values]
        self.screener_openInterest = [i.get('options.screener.rule.openInterest') for i in values]
        self.screener_otm = [i.get('options.screener.rule.otm') for i in values]
        self.screener_tobep = [i.get('options.screener.rule.tobep') for i in values]
        self.screener_changeRatio = [i.get('options.screener.rule.changeRatio') for i in values]
        self.screener_volume = [i.get('options.screener.rule.volume') for i in values]
        self.screener_itm = [i.get('options.screener.rule.itm') for i in values]
        self.screener_implVol = [i.get('options.screener.rule.implVol') for i in values]
        self.screener_close = [i.get('options.screener.rule.close') for i in values]
        self.screener_bid = [i.get('options.screener.rule.bid') for i in values]

        self.data_dict = {
            'tickerId': self.tickerId,
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
            'cycle': self.cycle,
            'ask': self.screener_ask,
            'openInterest': self.screener_openInterest,
            'otm': self.screener_otm,
            'tobep': self.screener_tobep,
            'volume': self.screener_volume,
            'itm': self.screener_itm,
            'implVol': self.screener_implVol,
            'close': self.screener_close,
            'bid': self.screener_bid
        }
        self.as_dataframe = pd.DataFrame(self.data_dict)

        # Convert necessary fields to float and datetime
        float_columns = [
            'quoteLotSize', 'strikePrice', 'change', 'changeRatio',
            'quoteMultiplier', 'ask', 'openInterest', 'otm', 'tobep',
            'volume', 'itm', 'implVol', 'close', 'bid'
        ]
        date_columns = ['expireDate']

        for col in float_columns:
            self.as_dataframe[col] = pd.to_numeric(self.as_dataframe[col], errors='coerce')

        for col in date_columns:
            self.as_dataframe[col] = pd.to_datetime(self.as_dataframe[col], errors='coerce')
