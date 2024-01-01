
import pandas as pd

class OptionSnapshotData:
    def __init__(self, data):
        self.implied_volatility = [float(i['implied_volatility']) if 'implied_volatility' in i else None for i in data]
        self.open_interest = [float(i['open_interest']) if 'open_interest' in i else None for i in data]
        self.break_even_price = [float(i['break_even_price']) if 'break_even_price' in i else None for i in data]

        day = [i['day'] if i['day'] is not None else None for i in data]
        self.day_close = [float(i['close']) if 'close' in i else None for i in day]
        self.day_high = [float(i['high']) if 'high' in i else None for i in day]
        self.last_updated  = [i['last_updated'] if 'last_updated' in i else None for i in day]
        self.day_low  = [float(i['low']) if 'low' in i else None for i in day]
        self.day_open  = [float(i['open']) if 'open' in i else None for i in day]
        self.day_change_percent  = [float(i['change_percent']) if 'change_percent' in i else None for i in day]
        self.day_change  = [float(i['change']) if 'change' in i else None for i in day]
        self.previous_close = [float(i['previous_close']) if 'previous_close' in i else None for i in day]
        self.day_volume = [float(i['volume']) if 'volume' in i else None for i in day]
        self.day_vwap  = [float(i['vwap']) if 'vwap' in i else None for i in day]

        details = [i['details'] if i['details'] is not None else None for i in data]
        self.contract_type = [i['contract_type'] if 'contract_type' in i else None for i in details]
        self.exercise_style = [i['exercise_style'] if 'exercise_style' in i else None for i in details]
        self.expiration_date = [i['expiration_date'] if 'expiration_date' in i else None for i in details]
        self.shares_per_contract= [i['shares_per_contract'] if 'shares_per_contract' in i else None for i in details]
        self.strike_price = [float(i['strike_price']) if 'strike_price' in i else None for i in details]
        self.option_symbol = [i['ticker'] if 'ticker' in i else None for i in details]

        greeks = [i['greeks'] if i['greeks'] is not None else None for i in data]
        self.delta = [float(i['delta']) if 'delta' in i else None for i in greeks]
        self.gamma= [float(i['gamma']) if 'gamma' in i else None for i in greeks]
        self.theta= [float(i['theta']) if 'theta' in i else None for i in greeks]
        self.vega = [float(i['vega']) if 'vega' in i else None for i in greeks]

        lastquote = [i['last_quote'] if i['last_quote'] is not None else None for i in data]
        self.ask = [float(i['ask']) if 'ask' in i else None for i in lastquote]
        self.ask_size = [float(i['ask_size']) if 'ask_size' in i else None for i in lastquote]
        self.bid= [float(i['bid']) if 'bid' in i else None for i in lastquote]
        self.bid_size= [float(i['bid_size']) if 'bid_size' in i else None for i in lastquote]
        self.quote_last_updated= [i['quote_last_updated'] if 'quote_last_updated' in i else None for i in lastquote]
        self.midpoint = [float(i['midpoint']) if 'midpoint' in i else None for i in lastquote]


        lasttrade = [i['last_trade'] if i['last_trade'] is not None else None for i in data]
        self.conditions = [i['conditions'] if 'conditions' in i else None for i in lasttrade]
        self.exchange = [i['exchange'] if 'exchange' in i else None for i in lasttrade]
        self.price= [float(i['price']) if 'price' in i else None for i in lasttrade]
        self.sip_timestamp= [i['sip_timestamp'] if 'sip_timestamp' in i else None for i in lasttrade]
        self.size= [float(['size']) if 'size' in i else None for i in lasttrade]

        underlying = [i['underlying_asset'] if i['underlying_asset'] is not None else None for i in data]
        self.change_to_break_even = [i['change_to_break_even'] if 'change_to_break_even' in i else None for i in underlying]
        self.underlying_last_updated = [i['underlying_last_updated'] if 'underlying_last_updated' in i else None for i in underlying]
        self.underlying_price = [float(i['price']) if 'price' in i else None for i in underlying]
        self.underlying_ticker = [i['ticker'] if 'ticker' in i else None for i in underlying]


        self.data_dict = {
        "implied_volatility": self.implied_volatility,
        "open_interest": self.open_interest,
        "break_even_price": self.break_even_price,
        "close": self.day_close,
        "high": self.day_high,
        "last_updated": self.last_updated,
        "low": self.day_low,
        "open": self.day_open,
        "change_percent": self.day_change_percent,
        "change": self.day_change,
        "previous_close": self.previous_close,
        "vol": self.day_volume,
        "vwap": self.day_vwap,
        "call_put": self.contract_type,
        "exercise_style": self.exercise_style,
        "exp": self.expiration_date,
        "shares_per_contract": self.shares_per_contract,
        "strike": self.strike_price,
        "ticker": self.option_symbol,

        "delta": self.delta,
        "gamma": self.gamma,
        "theta": self.theta,
        "vega": self.vega,
        "ask": self.ask,
        "ask_size": self.ask_size,
        "bid": self.bid,
        "bid_size": self.bid_size,
        "quote_last_updated": self.quote_last_updated,
        "midpoint": self.midpoint,
        "conditions": self.conditions,
        "exchange": self.exchange,
        "cost": self.price,
        "sip_timestamp": self.sip_timestamp,
        "size": self.size,
        "change_to_break_even": self.change_to_break_even,
        "underlying_last_updated": self.underlying_last_updated,
        "price": self.underlying_price,
        "symbol": self.underlying_ticker
    }


        self.df = pd.DataFrame(self.data_dict)
