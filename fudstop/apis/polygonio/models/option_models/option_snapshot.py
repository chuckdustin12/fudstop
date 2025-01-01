
import pandas as pd
import numpy as np
import math
import datetime
from fudstop.apis.polygonio.mapping import option_condition_dict
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

        details = [i.get('details', None) for i in data]
        self.contract_type = [i['contract_type'] if 'contract_type' in i else None for i in details]
        self.exercise_style = [i['exercise_style'] if 'exercise_style' in i else None for i in details]
        self.expiration_date = [i['expiration_date'] if 'expiration_date' in i else None for i in details]
        self.shares_per_contract= [i['shares_per_contract'] if 'shares_per_contract' in i else None for i in details]
        self.strike_price = [float(i['strike_price']) if 'strike_price' in i else None for i in details]
        self.option_symbol = [i['ticker'] if 'ticker' in i else None for i in details]

        greeks = [i.get('greeks', None) for i in data]
        self.delta = [float(i['delta']) if 'delta' in i else None for i in greeks]
        self.gamma= [float(i['gamma']) if 'gamma' in i else None for i in greeks]
        self.theta= [float(i['theta']) if 'theta' in i else None for i in greeks]
        self.vega = [float(i['vega']) if 'vega' in i else None for i in greeks]

        lastquote = [i.get('last_quote',None) for i in data]
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


 # Calculate time to maturity for each option
        self.time_to_maturity = [
            self.years_to_maturity(exp_date) for exp_date in self.expiration_date
        ]

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


def years_to_maturity(expiration_date, pricing_date=None):
    """
    Calculate the time to maturity (T) in years based on an expiration_date.
      - expiration_date: a string 'YYYY-MM-DD' or a datetime object
      - pricing_date:    the current date (or the date for which we want T).
                        Defaults to datetime.now() if not provided.

    Returns:
      T (float): Time to expiration in years. If expiration_date is in the past,
                 this will return 0.
    """
    # If no pricing_date is given, use "now"
    if pricing_date is None:
        pricing_date = datetime.datetime.now()
    
    # Convert string expiration_date into a datetime object if needed
    if isinstance(expiration_date, str):
        expiration_date = datetime.datetime.fromisoformat(expiration_date)
    
    # Calculate the difference in days
    day_diff = (expiration_date - pricing_date).days
    
    # If the expiration_date is already past, clamp T at 0
    if day_diff < 0:
        return 0.0
    
    # Convert days to years (approx. 365 days in a year)
    T = day_diff / 365.0
    return T


class WorkingUniversal:
    def __init__(self, data):
        print(data)
        self.risk_free_rate = 4.25
        self.break_even_price = [i.get('break_even_price') for i in data]
        session = [i.get('session') for i in data]
     
        self.change = [i.get('change', 0) for i in session if i is not None]
        self.change_percent = [i.get('change_percent') for i in session if i is not None]
        self.close = [i.get('close') for i in session if i is not None]
        self.high = [i.get('high') for i in session if i is not None]
        self.low = [i.get('low') for i in session if i is not None]
        self.open = [i.get('open') for i in session if i is not None]
        self.volume = [i.get('volume') for i in session if i is not None]
        self.previous_close = [i.get('previous_close') for i in session if i is not None]
        
        details = [i.get('details') for i in data]
        self.contract_type = [i.get('contract_type') for i in details if i is not None]
        self.exercise_style = [i.get('exercise_style') for i in details if i is not None]
        self.expiration_date = [i.get('expiration_date') for i in details if i is not None]
        self.shares_per_contract = [i.get('shares_per_contract') for i in details if i is not None]
        self.strike_price = [i.get('strike_price') for i in details if i is not None]
        
        greeks = [i.get('greeks') for i in data]
        self.delta = [i.get('delta') for i in greeks if i is not None]
        self.gamma = [i.get('gamma') for i in greeks if i is not None]
        self.theta = [i.get('theta') for i in greeks if i is not None]
        self.vega = [i.get('vega') for i in greeks if i is not None]
        
        self.implied_volatility = [i.get('implied_volatility') for i in data if i is not None]
        
        last_quote = [i.get('last_quote') for i in data if i is not None]
        self.ask = [i.get('ask') for i in last_quote if i is not None]
        self.ask_size = [i.get('ask_size') for i in last_quote if i is not None]
        self.ask_exchange = [i.get('ask_exchange') for i in last_quote if i is not None]
        self.bid = [i.get('bid') for i in last_quote if i is not None]
        self.bid_size = [i.get('bid_size') for i in last_quote if i is not None]
        self.bid_exchange = [i.get('bid_exchange') for i in last_quote if i is not None]
        self.midpoint = [i.get('midpoint') for i in last_quote if i is not None]
        
        last_trade = [i.get('last_trade') for i in data]
        self.sip_timestamp = [i.get('sip_timestamp') for i in last_trade if i is not None]
        self.trade_conditions = [','.join(map(str, i.get('conditions', []))) for i in last_trade if i is not None]
        self.trade_conditions = option_condition_dict.get(int(self.trade_conditions[0]))

    
        self.trade_price = [i.get('price') for i in last_trade if i is not None]
        self.trade_size = [i.get('size') for i in last_trade if i is not None]
        self.trade_exchange = [i.get('exchange') for i in last_trade if i is not None]
        
        self.open_interest = [i.get('open_interest') for i in data if i is not None]
        
        underlying_asset = [i.get('underlying_asset') for i in data ]
        self.change_to_break_even = [i.get('change_to_break_even') for i in underlying_asset if i is not None]
        self.underlying_price = [i.get('price') for i in underlying_asset if i is not None]
        self.underlying_ticker = [i.get('ticker') for i in underlying_asset if i is not None]
        
        self.name = [i.get('name') for i in data ]
        self.market_status = [i.get('market_status') for i in data if i is not None]
        self.ticker = [i.get('ticker') for i in data if i is not None]
        self.type = [i.get('type') for i in data if i is not None]


        self.volume_oi_ratio = []
        for vol, oi in zip(self.volume, self.open_interest):
            if vol is None or oi is None or oi == 0:
                self.volume_oi_ratio.append(None)
            else:
                self.volume_oi_ratio.append(vol / oi)





        self.dte = [
            max((datetime.datetime.fromisoformat(exp_date) - datetime.datetime.now()).days, 0) 
            if exp_date else None 
            for exp_date in self.expiration_date
        ]

        self.intrinsic_value = [
            max(S - K, 0) if S is not None and K is not None and ct == 'call' else 
            max(K - S, 0) if S is not None and K is not None and ct == 'put' else None
            for S, K, ct in zip(self.underlying_price, self.strike_price, self.contract_type)
        ]

 
        self.extrinsic_value = [
            max(midpoint - intrinsic, 0) if midpoint is not None and intrinsic is not None else None
            for midpoint, intrinsic in zip(self.midpoint, self.intrinsic_value)
        ]


        self.dte = [
            max((datetime.datetime.fromisoformat(exp_date) - datetime.datetime.now()).days, 0)
            if exp_date is not None else None
            for exp_date in self.expiration_date
        ]

       
        self.time_to_maturity = [
            d / 365.0 if d is not None else None
            for d in self.dte
        ]

    
        self.spread = [
            (ask - bid) if ask is not None and bid is not None else None
            for ask, bid in zip(self.ask, self.bid)
        ]

       
        self.spread_percent = [
            ((ask - bid) / midpoint * 100) if ask is not None and bid is not None and midpoint not in (None, 0) else None
            for ask, bid, midpoint in zip(self.ask, self.bid, self.midpoint)
        ]

     
        self.premium_percent = [
            (midpoint / S * 100) if midpoint is not None and S is not None and S > 0 else None
            for midpoint, S in zip(self.midpoint, self.underlying_price)
        ]


        self.time_to_maturity = [
            years_to_maturity(exp_date) for exp_date in self.expiration_date
        ]

      
        self.d1_d2 = [
            self.compute_d1_d2(S, K, T, r, sigma)
            for S, K, T, r, sigma in zip(
                self.underlying_price,
                self.strike_price,
                self.time_to_maturity,
                [self.risk_free_rate / 100.0] * len(self.strike_price),
                self.implied_volatility,
            )
        ]
     
        self.greeks_dict = [
            self.compute_additional_greeks(S, T, sigma, gamma, r=self.risk_free_rate / 100.0)
            for S, T, sigma, gamma in zip(
                self.underlying_price,
                self.time_to_maturity,
                self.implied_volatility,
                self.gamma,
            )
        ]
      
        valid_iv_strike_pairs = [
            (strike, iv) 
            for strike, iv in zip(self.strike_price, self.implied_volatility) 
            if (strike is not None and iv is not None)
        ]

        if valid_iv_strike_pairs:
   
            lowest_iv_strike, lowest_iv = min(valid_iv_strike_pairs, key=lambda x: x[1])
            self.lowest_iv_strike_price = lowest_iv_strike
        else:
            self.lowest_iv_strike_price = None
        valid_iv = [iv for iv in self.implied_volatility if iv is not None]
        if valid_iv:
            self.average_implied_volatility = sum(valid_iv) / len(valid_iv)
        else:
            self.average_implied_volatility = None
        if self.lowest_iv_strike_price is not None and self.underlying_price:
 
            current_price_list = [p for p in self.underlying_price if p is not None]
            if current_price_list:
                current_price = current_price_list[0]
                if self.lowest_iv_strike_price > current_price:
                    self.iv_skew = "call_skew"
                else:
                    self.iv_skew = "put_skew"
            else:
                self.iv_skew = None
        else:
            self.iv_skew = None
        # Dictionary to organize all data
        self.data_dict = {
            'break_even_price': self.break_even_price,
            'change': self.change,
            'change_percent': self.change_percent,
            'close': self.close,
            'high': self.high,
            'low': self.low,
            'open': self.open,
            'volume': self.volume,
            'previous_close': self.previous_close,
            'call_put': self.contract_type,
            'exercise_style': self.exercise_style,
            'expiry': self.expiration_date,
            'shares_per_contract': self.shares_per_contract,
            'strike': self.strike_price,
            'delta': self.delta,
            'gamma': self.gamma,
            'theta': self.theta,
            'vega': self.vega,
            'iv': self.implied_volatility,
            'ask': self.ask,
            'ask_size': self.ask_size,
            'ask_exchange': self.ask_exchange,
            'bid': self.bid,
            'bid_size': self.bid_size,
            'bid_exchange': self.bid_exchange,
            'midpoint': self.midpoint,
            'sip_timestamp': self.sip_timestamp,
            'trade_conditions': self.trade_conditions,
            'trade_price': self.trade_price,
            'trade_size': self.trade_size,
            'trade_exchange': self.trade_exchange,
            'oi': self.open_interest,
            'change_to_break_even': self.change_to_break_even,
            'underlying_price': self.underlying_price,
            'ticker': self.underlying_ticker,
            'name': self.name,
            'market_status': self.market_status,
            'option_symbol': self.ticker,
            'type': self.type,
            'intrinsic_value': self.intrinsic_value,
            'extrinsic_value': self.extrinsic_value,
            'premium_percent': self.premium_percent,
            'dte': self.dte,
            'spread': self.spread,
            'spread_pct': self.spread_percent,
            'vol_oi_ratio': self.volume_oi_ratio,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
        self.as_dataframe['risk_free_rate'] =self.risk_free_rate
        self.as_dataframe['iv_skew'] = self.iv_skew
        self.as_dataframe['avg_iv'] = self.average_implied_volatility
        self._add_moneyness()
        self.add_additional_greeks()

    @staticmethod
    def compute_additional_greeks(S, T, sigma, gamma, *, r=None):
        """
        Compute additional Greeks numerically, handling partial None values.
        
        Parameters:
            S (float): Underlying price
            T (float): Time to maturity in years
            sigma (float): Implied volatility
            gamma (float): Gamma of the option
            r (float, optional): Risk-free rate
        Returns:
            dict: Additional Greeks with None for any invalid calculations.
        """
        # Default result structure with None for all Greeks
        result = {
            'vanna': None,
            'vomma': None,
            'veta': None,
            'vera': None,
            'speed': None,
            'zomma': None,
            'color': None,
            'ultima': None,
            'charm': None,
        }

        try:
            # Vanna: ∂Δ/∂σ
            if sigma and gamma:
                result['vanna'] = gamma / sigma

            # Vomma: ∂²V/∂σ²
            if sigma and gamma:
                result['vomma'] = gamma / sigma**2

            # Veta: ∂V/∂T
            if S and sigma and gamma and T:
                result['veta'] = -S * sigma * gamma * T

            # Vera: ∂²V/∂σ∂r
            if r and gamma:
                result['vera'] = gamma / r

            # Speed: ∂³V/∂S³
            if S and gamma:
                result['speed'] = -gamma / S

            # Zomma: ∂Γ/∂σ
            if gamma:
                result['zomma'] = 2 * gamma

            # Color: ∂Γ/∂T
            if T and gamma:
                result['color'] = -gamma / T

            # Ultima: ∂³V/∂σ³
            if sigma and gamma:
                result['ultima'] = gamma / sigma**3

            # Charm: ∂Δ/∂T
            if T and gamma and S and sigma:
                result['charm'] = -gamma * (2 * T**(-1))

        except Exception as e:
            print(f"Error computing additional Greeks: {e}")

        return result

    @staticmethod
    def compute_d1_d2(S, K, T, r, sigma, q=0.0):
        """
        Compute the Black–Scholes d1 and d2 terms:
        d1 = [ln(S/K) + (r - q + sigma^2/2)*T] / (sigma * sqrt(T))
        d2 = d1 - sigma * sqrt(T)

        Parameters:
        S (float):     Underlying price
        K (float):     Strike price
        T (float):     Time to maturity in years
        r (float):     Risk-free interest rate (annualized, decimal)
        sigma (float): Implied volatility (decimal, e.g., 0.20 for 20%)
        q (float):     Continuous dividend yield (decimal). Defaults to 0.

        Returns:
        (d1, d2) as a tuple of floats, or (None, None) if inputs are invalid.
        """
        # Validate inputs: None or non-positive values should return (None, None)
        if S is None or K is None or T is None or sigma is None:
            return (None, None)
        if (S <= 0) or (K <= 0) or (T <= 0) or (sigma <= 0):
            return (None, None)

        try:
            d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
            d2 = d1 - sigma * np.sqrt(T)
        except Exception as e:
            # Optional: Log the exception if needed
            print(f"Error computing d1 and d2: {e}")
            return (None, None)

        return (d1, d2)


    def _add_moneyness(self):
        """
        Adds a 'moneyness' column to self.as_dataframe: atm, itm, or otm.
        For calls:
         - ITM if underlying_price > strike_price
         - OTM if underlying_price < strike_price
         - ATM if equal
        For puts:
         - ITM if underlying_price < strike_price
         - OTM if underlying_price > strike_price
         - ATM if equal
        """
        def compute_moneyness(row):
            contract_type = row['call_put']
            strike = row['strike']
            und_price = row['underlying_price']

            # Handle None values
            if (contract_type is None or
                strike is None or
                und_price is None or
                np.isnan(strike) or
                np.isnan(und_price)):
                return None

            # Identify moneyness by type
            if contract_type.lower() == 'call':
                if und_price > strike:
                    return 'itm'
                elif und_price < strike:
                    return 'otm'
                else:
                    return 'atm'
            elif contract_type.lower() == 'put':
                if und_price < strike:
                    return 'itm'
                elif und_price > strike:
                    return 'otm'
                else:
                    return 'atm'
            else:
                return None

        self.as_dataframe['moneyness'] = self.as_dataframe.apply(compute_moneyness, axis=1)


    def add_additional_greeks(self):
        """
        Compute and add additional Greeks to self.as_dataframe.
        """
        additional_greeks = [
            self.compute_additional_greeks(S, T, sigma, gamma, r=self.risk_free_rate / 100.0)
            for S, T, sigma, gamma in zip(
                self.underlying_price,
                self.time_to_maturity,
                self.implied_volatility,
                self.gamma,
            )
        ]

        # Add each Greek to the DataFrame
        for greek in ['vanna', 'vomma', 'veta', 'vera', 'speed', 'zomma', 'color', 'ultima', 'charm']:
            self.as_dataframe[greek] = [g[greek] for g in additional_greeks]