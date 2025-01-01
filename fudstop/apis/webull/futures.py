import pandas as pd
import datetime
import pytz



class WBFutures:
    def __init__(self, values, futures):


        self.dt = [i.get('dt') for i in values]
        self.tickerId = [i.get('tickerId') for i in values]
        self.contractId = [i.get('contractId') for i in values]
        self.tradeTime = [i.get('tradeTime') for i in values]
        self.status = [i.get('status') for i in values]
        self.close = [i.get('close') for i in values]
        self.change = [i.get('change') for i in values]
        self.changeRatio = [i.get('changeRatio') for i in values]
        self.volume = [i.get('volume') for i in values]
        self.high = [i.get('high') for i in values]
        self.low = [i.get('low') for i in values]
        self.preClose = [i.get('preClose') for i in values]
        self.open = [i.get('open') for i in values]
        self.priorSettle = [i.get('priorSettle') for i in values]
        self.openInterest = [i.get('openInterest') for i in values]

        self.tickerId = [i.get('tickerId') for i in futures]
        self.relatedContractId = [i.get('relatedContractId') for i in futures]
        self.symbol = [i.get('symbol') for i in futures]
        self.name = [i.get('name') for i in futures]
        self.securityType = [i.get('securityType') for i in futures]
        self.securitySubType = [i.get('securitySubType') for i in futures]
        self.type = [i.get('type') for i in futures]
        self.month = [i.get('month') for i in futures]
        self.year = [i.get('year') for i in futures]
        self.lastTradingDate = [i.get('lastTradingDate') for i in futures]
        self.firstTradingDate = [i.get('firstTradingDate') for i in futures]
        self.settlementDate = [i.get('settlementDate') for i in futures]
        self.expDate = [i.get('expDate') for i in futures]
        self.contractSize = [i.get('contractSize') for i in futures]
        self.minPriceFluctuation = [i.get('minPriceFluctuation') for i in futures]
        self.marginInitialCash = [i.get('marginInitialCash') for i in futures]

        self.data = {
            'dt': [i.get('dt', None) for i in values],
            'ticker_id': [i.get('tickerId', None) for i in values],
            'contract_id': [i.get('contractId', None) for i in values],
            'trade_time': [self.convert_to_eastern_time(i.get('tradeTime', None)) for i in values],
            'status': [i.get('status', None) for i in values],
            'close': [self.to_float(i.get('close')) for i in values],
            'change': [self.to_float(i.get('change')) for i in values],
            'change_ratio': [self.to_float(i.get('changeRatio')) for i in values],
            'volume': [self.to_float(i.get('volume')) for i in values],
            'high': [self.to_float(i.get('high')) for i in values],
            'low': [self.to_float(i.get('low')) for i in values],
            'pre_close': [self.to_float(i.get('preClose')) for i in values],
            'open': [self.to_float(i.get('open')) for i in values],
            'prior_settle': [self.to_float(i.get('priorSettle')) for i in values],
            'open_interest': [self.to_float(i.get('openInterest')) for i in values],
            'ticker_id_futures': [i.get('tickerId', None) for i in futures],
            'contract_specs_id': [i.get('contractSpecsId', None) for i in futures],
            'related_contract_id': [i.get('relatedContractId', None) for i in futures],
            'symbol': [i.get('symbol', None) for i in futures],
            'exchange_id': [i.get('exchangeId', None) for i in futures],
            'name': [i.get('name', None) for i in futures],
            'security_type': [i.get('securityType', None) for i in futures],
            'security_sub_type': [i.get('securitySubType', None) for i in futures],
            'type': [i.get('type', None) for i in futures],
            'currency_id': [i.get('currencyId', None) for i in futures],
            'region_id': [i.get('regionId', None) for i in futures],
            'month': [i.get('month', None) for i in futures],
            'year': [i.get('year', None) for i in futures],
            'last_trading_date': [i.get('lastTradingDate', None) for i in futures],
            'first_trading_date': [i.get('firstTradingDate', None) for i in futures],
            'settlement_date': [i.get('settlementDate', None) for i in futures],
            'exp_date': [i.get('expDate', None) for i in futures],
            'contract_size': [self.to_float(i.get('contractSize')) for i in futures],
            'min_price_fluctuation': [self.to_float(i.get('minPriceFluctuation')) for i in futures],
            'margin_initial_cash': [self.to_float(i.get('marginInitialCash')) for i in futures],
        }


        self.df = pd.DataFrame(self.data)

    def to_float(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    def convert_to_eastern_time(self, timestamp):
        if timestamp is None:
            return None
        utc_time = datetime.datetime.utcfromtimestamp(int(timestamp) / 1000)
        eastern = pytz.timezone('US/Eastern')
        eastern_time = utc_time.astimezone(eastern)
        # Return the time in 'YYYY-MM-DD HH:MM:SS' format without timezone information
        return eastern_time.strftime('%Y-%m-%d %H:%M:%S')
    
    def group_by_abbreviation(self):
        grouped_df = self.df.groupby('symbol').sum()
        return grouped_df
    


class IndividualFutures:
    def __init__(self, data):
        self.ticker_id = [i.get('tickerId') for i in data]
        self.name = [i.get('name') for i in data]
        self.symbol = [i.get('symbol') for i in data]
        self.trade_time = [self.process_trade_time(i.get('tradeTime', None)) for i in data]
        
        self.close = [self.to_float(i.get('close')) for i in data]
        self.change = [self.to_float(i.get('change')) for i in data]
        self.change_ratio = [self.to_float(i.get('changeRatio')) for i in data]
        self.volume = [self.to_float(i.get('volume')) for i in data]
        self.pre_close = [self.to_float(i.get('preClose')) for i in data]
        self.open = [self.to_float(i.get('open')) for i in data]
        self.high = [self.to_float(i.get('high')) for i in data]
        self.low = [self.to_float(i.get('low')) for i in data]
        self.trade_stamp = [i.get('tradeStamp') for i in data]
        self.settlement_date = [i.get('settlementDate') for i in data]
        self.to_expiry_date = [i.get('toExpiryDate') for i in data]
        self.multiplier = [self.to_float(i.get('multiplier')) for i in data]
        self.open_interest = [self.to_float(i.get('openInterest')) for i in data]
        self.open_interest_change = [self.to_float(i.get('openInterestChange')) for i in data]
        self.prior_settle = [self.to_float(i.get('priorSettle')) for i in data]
        self.settle_price = [self.to_float(i.get('settlePrice')) for i in data]
        self.exp_date = [i.get('expDate') for i in data]
        self.data_dict = { 'ticker_id': self.ticker_id,
                'name': self.name,
                'symbol': self.symbol,
                'trade_time': self.trade_time,
                'close': self.close,
                'change': self.change,
                'change_ratio': self.change_ratio,
                'volume': self.volume,
                'pre_close': self.pre_close,
                'open': self.open,
                'high': self.high,
                'low': self.low,
                'trade_stamp': self.trade_stamp,
                'settlement_date': self.settlement_date,
                'to_expiry_date': self.to_expiry_date,
                'multiplier': self.multiplier,
                'open_interest': self.open_interest,
                'open_interest_change': self.open_interest_change,
                'prior_settle': self.prior_settle,
                'settle_price': self.settle_price,
                'exp_date': self.exp_date
            }
        

        self.df = pd.DataFrame(self.data_dict)
    def to_float(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def convert_to_eastern_time(self, timestamp):
        if timestamp is None:
            return None
        utc_time = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
        eastern = pytz.timezone('US/Eastern')
        eastern_time = utc_time.astimezone(eastern)
        return eastern_time.strftime('%Y-%m-%d %H:%M:%S')

    def process_trade_time(self, trade_time):
        if trade_time is None:
            return None
        # Remove everything after the '.' and split by 'T'
        processed_time = trade_time.split('.')[0]
        return self.convert_to_eastern_time(processed_time)