import pandas as pd



class EarningSurprise:
    def __init__(self, ticker, values):

        self.surpriseRatio = [i.get('surpriseRatio') for i in values]
        self.afterLast = [i.get('afterLast') for i in values]
        self.fiscalYear = [i.get('fiscalYear') for i in values]
        self.quarter = [i.get('quarter') for i in values]
        self.releaseDate = [i.get('releaseDate') for i in values]
        self.eps = [i.get('eps') for i in values]
          
        self.name = [i.get('name') for i in ticker]
        self.symbol = [i.get('symbol') for i in ticker]
        self.close = [i.get('close') for i in ticker]
        self.change = [i.get('change') for i in ticker]
        self.changeRatio = [i.get('changeRatio') for i in ticker]
        self.marketValue = [i.get('marketValue') for i in ticker]
        self.volume = [i.get('volume') for i in ticker]
        self.turnoverRate = [i.get('turnoverRate') for i in ticker]
        self.peTtm = [i.get('peTtm') for i in ticker]
        self.dividend = [i.get('dividend') for i in ticker]
        self.preClose = [i.get('preClose') for i in ticker]
        self.fiftyTwoWkHigh = [i.get('fiftyTwoWkHigh') for i in ticker]
        self.fiftyTwoWkLow = [i.get('fiftyTwoWkLow') for i in ticker]
        self.open = [i.get('open') for i in ticker]
        self.high = [i.get('high') for i in ticker]
        self.low = [i.get('low') for i in ticker]
        self.vibrateRatio = [i.get('vibrateRatio') for i in ticker]

        self.data_dict = { 
            'ticker': self.symbol,
            'name': self.name,
            'earnings_release_date': self.releaseDate,
            'surprise_ratio': self.surpriseRatio,
            'after_last': self.afterLast,
            'fiscal_year': self.fiscalYear,
            'quarter': self.quarter,
            'eps': self.eps,
            'pe_ttm': self.peTtm,
            'close_price': self.close,
            'price_change': self.change,
            'change_ratio': self.changeRatio,
            'market_value': self.marketValue,
            'volume': self.volume,
            'turnover_rate': self.turnoverRate,
            'dividend': self.dividend,
            'pre_close': self.preClose,
            'fifty_high': self.fiftyTwoWkHigh,
            'fifty_low': self.fiftyTwoWkLow,
            'open_price': self.open,
            'high_price': self.high,
            'low_price': self.low,
            'vibrate_ratio': self.vibrateRatio
        }


        self.df = pd.DataFrame(self.data_dict)



class Dividend:
    def __init__(self, ticker, values):


        self._yield = [i.get('yield') for i in values]
        self.dividend = [i.get('dividend') for i in values]
        self.ex_date = [i.get('exDate') for i in values]
          
        self.name = [i.get('name') for i in ticker]
        self.symbol = [i.get('symbol') for i in ticker]
        self.close = [float(i.get('close')) for i in ticker]
        self.change = [float(i.get('change')) for i in ticker]
        self.changeRatio = [float(i.get('changeRatio')) for i in ticker]
        self.marketValue = [float(i.get('marketValue')) for i in ticker]
        self.volume = [float(i.get('volume')) for i in ticker]
        self.turnoverRate = [float(i.get('turnoverRate')) for i in ticker]
        self.peTtm = [float(i.get('peTtm')) for i in ticker]
        self.dividend = [float(i.get('dividend')) for i in ticker]
        self.preClose = [float(i.get('preClose')) for i in ticker]
        self.open = [float(i.get('open')) for i in ticker]
        self.high = [float(i.get('high')) for i in ticker]
        self.low = [float(i.get('low')) for i in ticker]
        self.vibrateRatio = [float(i.get('vibrateRatio')) for i in ticker]

        self.data_dict = { 
            'ticker': self.symbol,
            'name': self.name,
            'dividend': self.dividend,
            'yield': self._yield,
            'ex_date': self.ex_date,
            'pe_ttm': self.peTtm,
            'close_price': self.close,
            'price_change': self.change,
            'change_ratio': self.changeRatio,
            'market_value': self.marketValue,
            'volume': self.volume,
            'turnover_rate': self.turnoverRate,
            'dividend': self.dividend,
            'pre_close': self.preClose,
            'open_price': self.open,
            'high_price': self.high,
            'low_price': self.low,
            'vibrate_ratio': self.vibrateRatio,
            
        }


        self.df = pd.DataFrame(self.data_dict)



class MicroFutures:
    def __init__(self, futures, values):

        self.symbol = [i.get('symbol') for i in futures]
        self.name = [i.get('name') for i in futures]
        self.type = [i.get('type') for i in futures]
        self.month = [i.get('month') for i in futures]
        self.year = [i.get('year') for i in futures]
        self.lastTradingDate = [i.get('lastTradingDate') for i in futures]
        self.firstTradingDate = [i.get('firstTradingDate') for i in futures]
        self.settlementDate = [i.get('settlementDate') for i in futures]
        self.expDate = [i.get('expDate') for i in futures]
        self.contractSize = [float(i.get('contractSize')) for i in futures]
        self.contractUnit = [i.get('contractUnit') for i in futures]
        self.minPriceFluctuation = [float(i.get('minPriceFluctuation')) for i in futures]
        self.contractType = [i.get('contractType') for i in futures]
        self.marginInitialCash = [float(i.get('marginInitialCash')) for i in futures]


        self.contractId = [i.get('contractId') for i in values]
        self.close = [float(i.get('close') or 0) if i.get('close') is not None else None for i in values]
        self.change = [float(i.get('change') or 0) if i.get('change') is not None else None for i in values]
        self.changeRatio = [float(i.get('changeRatio') or 0) if i.get('changeRatio') is not None else None for i in values]
        self.volume = [float(i.get('volume') or 0) if i.get('volume') is not None else None for i in values]
        self.high = [float(i.get('high') or 0) if i.get('high') is not None else None for i in values]
        self.low = [float(i.get('low') or 0) if i.get('low') is not None else None for i in values]
        self.open = [i.get('open') if i.get('open') is not None else None for i in values]
        self.priorSettle = [float(i.get('priorSettle') or 0) if i.get('priorSettle') is not None else None for i in values]
        self.openInterest = [float(i.get('openInterest') or 0) if i.get('openInterest') is not None else None for i in values]

                # Create a consolidated data dictionary
        self.data_dict = {
            'symbol': self.symbol,
            'name': self.name,
            'type': self.type,
            'month': self.month,
            'year': self.year,
            'last_trading_date': self.lastTradingDate,
            'first_trading_date': self.firstTradingDate,
            'settlement_date': self.settlementDate,
            'exp_date': self.expDate,
            'contract_size': self.contractSize,
            'contract_unit': self.contractUnit,
            'min_price_fluctuation': self.minPriceFluctuation,
            'contract_type': self.contractType,
            'margin_initial_cash': self.marginInitialCash,
            'contract_id': self.contractId,
            'close_price': self.close,
            'price_change': self.change,
            'change_ratio': self.changeRatio,
            'volume': self.volume,
            'high_price': self.high,
            'low_price': self.low,
            'open_price': self.open,
            'prior_settle': self.priorSettle,
            'open_interest': self.openInterest
        }


        self.df = pd.DataFrame(self.data_dict)