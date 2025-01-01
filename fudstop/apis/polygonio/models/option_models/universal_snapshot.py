import sys
from pathlib import Path
import scipy.stats as stats
import json
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from ...mapping import OPTIONS_EXCHANGES
indices_list = ["SPX", "SPXW", "NDX", "VIX", "VVIX"]
from scipy.stats import norm



import numpy as np
import pandas as pd
from datetime import datetime
from scipy.stats import norm, stats
import json
import logging

import numpy as np
import pandas as pd
from datetime import datetime
from scipy.stats import norm, stats
import json
import logging
def safe_log(x):
    try:
        return np.log(float(x))  # Convert to float and apply log
    except (ValueError, TypeError) as e:
        print(f"Error computing log for value {x}: {e}")
        return None
import numpy as np
import pandas as pd
from datetime import datetime
from scipy.stats import norm, stats
import json
import logging

import numpy as np
import pandas as pd
from datetime import datetime
from scipy.stats import norm, stats
import json
import logging

class UniversalOptionSnapshot:
    def __init__(self, results):
        # Initialize attributes as empty lists

        fixed_risk_free_rate = 4.87  # Assuming a fixed risk-free rate
        self.break_even = []

        self.implied_volatility = []
        self.open_interest = []
        self.risk_free_rate = []
        self.volume = []
        self.high = []
        self.low = []
        self.vwap = []
        self.open = []
        self.close = []
        self.change_percent = []
        self.strike = []
        self.expiry = []
        self.cp = []
        self.exercise_style = []
        self.option_symbol = []
        self.theta = []
        self.delta = []
        self.gamma = []
        self.vega = []
        self.sip_timestamp = []
        self.conditions = []
        self.price = []
        self.trade_size = []
        self.exchange = []
        self.ask = []
        self.bid = []
        self.bid_size = []
        self.ask_size = []
        self.mid = []
        self.change_to_breakeven = []
        self.underlying_price = []
        self.ticker = []
        self.dte = []
        self.time_value = []
        self.moneyness = []
        self.liquidity_score = []
        self.spread = []
        self.intrinsic_value = []
        self.extrinsic_value = []
        self.leverage_ratio = []
        self.spread_pct = []
        self.return_on_risk = []
        self.velocity = []
        self.gamma_risk = []
        self.theta_decay_rate = []
        self.vega_impact = []
        self.delta_theta_ratio = []
        self.sensitivity = []
        self.ltr = []
        self.rrs = []
        self.gbs = []
        self.opp = []

        # Set the fixed risk-free rate
        fixed_risk_free_rate = 4.87  # Assuming a fixed risk-free rate

        # Loop over each item in results
        for idx, i in enumerate(results):
            # Extract basic information
            self.break_even.append(self.to_float(i.get('break_even_price')))
            self.implied_volatility.append(self.to_float(i.get('implied_volatility')))
            self.open_interest.append(self.to_float(i.get('open_interest')))
            self.risk_free_rate.append(fixed_risk_free_rate)

            # Safely extract 'day' data
            day = i.get('day', {})
            self.volume.append(self.to_float(day.get('volume')))
            self.high.append(self.to_float(day.get('high')))
            self.low.append(self.to_float(day.get('low')))
            self.vwap.append(self.to_float(day.get('vwap')))
            self.open.append(self.to_float(day.get('open')))
            self.close.append(self.to_float(day.get('close')))
            self.change_percent.append(self.safe_round(self.to_float(day.get('change_percent', 0)), 0))

            # Safely extract 'details' data
            details = i.get('details', {})
            self.strike.append(self.to_float(details.get('strike_price')))
            self.expiry.append(details.get('expiration_date'))
            self.cp.append(details.get('contract_type'))
            self.exercise_style.append(details.get('exercise_style'))
            self.option_symbol.append(details.get('ticker'))

            # Safely extract 'greeks' data
            greeks = i.get('greeks', {})
            self.theta.append(self.safe_round(self.to_float(greeks.get('theta')), 4))
            self.delta.append(self.safe_round(self.to_float(greeks.get('delta')), 4))
            self.gamma.append(self.safe_round(self.to_float(greeks.get('gamma')), 4))
            self.vega.append(self.safe_round(self.to_float(greeks.get('vega')), 4))

            # Safely extract 'last_trade' data
            last_trade = i.get('last_trade', {})
            self.sip_timestamp.append(last_trade.get('sip_timestamp'))
            conditions = last_trade.get('conditions')
            self.conditions.append(conditions[0] if isinstance(conditions, list) else conditions)
            self.price.append(self.to_float(last_trade.get('price')))
            self.trade_size.append(self.to_float(last_trade.get('size')))
            self.exchange.append(last_trade.get('exchange'))

            # Safely extract 'last_quote' data
            last_quote = i.get('last_quote', {})
            self.ask.append(self.to_float(last_quote.get('ask')))
            self.bid.append(self.to_float(last_quote.get('bid')))
            self.bid_size.append(self.to_float(last_quote.get('bid_size')))
            self.ask_size.append(self.to_float(last_quote.get('ask_size')))
            self.mid.append(self.to_float(last_quote.get('midpoint')))

            # Safely extract 'underlying_asset' data
            underlying_asset = i.get('underlying_asset', {})
            self.change_to_breakeven.append(self.to_float(underlying_asset.get('change_to_break_even')))
            self.underlying_price.append(self.to_float(underlying_asset.get('price')))
            self.ticker.append(underlying_asset.get('ticker'))

            # Calculations that depend on extracted data
            today = pd.Timestamp(datetime.today())

            # Convert expiry to datetime
            expiry_date = pd.to_datetime(details.get('expiration_date'), errors='coerce')
            if pd.notnull(expiry_date):
                dte_value = (expiry_date - today).days
            else:
                dte_value = None
            self.dte.append(dte_value)

            # Time value
            price = self.price[-1]
            underlying_price = self.underlying_price[-1]
            strike = self.strike[-1]
            if price is not None and underlying_price is not None and strike is not None:
                time_value = self.safe_round(price - underlying_price + strike, 3)
            else:
                time_value = None
            self.time_value.append(time_value)

            # Moneyness
            cp = self.cp[-1]
            moneyness = self.calculate_moneyness(cp, strike, underlying_price)
            self.moneyness.append(moneyness)

            # Liquidity indicator
            ask_size = self.ask_size[-1]
            bid_size = self.bid_size[-1]
            if ask_size is not None and bid_size is not None:
                liquidity_score = self.safe_round(ask_size + bid_size, 3)
            else:
                liquidity_score = None
            self.liquidity_score.append(liquidity_score)

            # Spread calculation
            ask = self.ask[-1]
            bid = self.bid[-1]
            if ask is not None and bid is not None:
                spread = ask - bid
            else:
                spread = None
            self.spread.append(spread)

            # Intrinsic value
            intrinsic_value = self.calculate_intrinsic_value(cp, strike, underlying_price)
            self.intrinsic_value.append(intrinsic_value)

            # Extrinsic value
            if price is not None and intrinsic_value is not None:
                extrinsic_value = self.safe_round(price - intrinsic_value, 3)
            else:
                extrinsic_value = None
            self.extrinsic_value.append(extrinsic_value)

            # Leverage ratio
            delta = self.delta[-1]
            if delta is not None and strike is not None and underlying_price not in [None, 0]:
                leverage_ratio = self.safe_round(delta / (strike / underlying_price), 3)
            else:
                leverage_ratio = None
            self.leverage_ratio.append(leverage_ratio)

            # Spread percentage
            mid = self.mid[-1]
            if ask is not None and bid is not None and mid not in [None, 0]:
                spread_pct = self.safe_round((ask - bid) / mid * 100.0, 3)
            else:
                spread_pct = None
            self.spread_pct.append(spread_pct)

            # Return on risk
            return_on_risk = self.calculate_return_on_risk(cp, price, strike, underlying_price)
            self.return_on_risk.append(return_on_risk)

            # Option velocity
            if delta is not None and price not in [None, 0]:
                velocity = self.safe_round(delta / price, 3)
            else:
                velocity = None
            self.velocity.append(velocity)

            # Gamma risk
            gamma = self.gamma[-1]
            if gamma is not None and underlying_price is not None:
                gamma_risk = self.safe_round(gamma * underlying_price, 3)
            else:
                gamma_risk = None
            self.gamma_risk.append(gamma_risk)

            # Theta decay rate
            theta = self.theta[-1]
            if theta is not None and price not in [None, 0]:
                theta_decay_rate = self.safe_round(theta / price, 3)
            else:
                theta_decay_rate = None
            self.theta_decay_rate.append(theta_decay_rate)

            # Vega impact
            vega = self.vega[-1]
            if vega is not None and price not in [None, 0]:
                vega_impact = self.safe_round(vega / price, 3)
            else:
                vega_impact = None
            self.vega_impact.append(vega_impact)

            # Delta to theta ratio
            if delta is not None and theta not in [None, 0]:
                delta_theta_ratio = self.safe_round(delta / theta, 3)
            else:
                delta_theta_ratio = None
            self.delta_theta_ratio.append(delta_theta_ratio)

            # Option sensitivity score
            delta_val = delta if delta is not None else 0
            gamma_val = gamma if gamma is not None else 0
            vega_val = vega if vega is not None else 0
            theta_val = theta if theta is not None else 0
            sensitivity = self.safe_round(delta_val + 0.5 * gamma_val + 0.1 * vega_val - 0.5 * theta_val, 3)
            self.sensitivity.append(sensitivity)

            # Liquidity-theta ratio
            if liquidity_score is not None and theta not in [None, 0]:
                ltr = liquidity_score / abs(theta)
            else:
                ltr = None
            self.ltr.append(ltr)

            # Risk-reward score
            implied_volatility = self.implied_volatility[-1]
            if intrinsic_value is not None and extrinsic_value is not None and implied_volatility not in [None, 0]:
                rrs = (intrinsic_value + extrinsic_value) / (implied_volatility + 1e-4)
            else:
                rrs = None
            self.rrs.append(rrs)

            # Greeks-balance score
            gbs = self.safe_round(abs(delta_val) + abs(gamma_val) - abs(vega_val) - abs(theta_val), 3)
            self.gbs.append(gbs)

            # Options profit potential
            opp = self.calculate_opp(moneyness, sensitivity, ltr, rrs)
            self.opp.append(opp)


        
            self.time_to_maturity = [
                d / 365.0 if d is not None else None
                for d in self.dte
            ]

            # Safety checks: make sure these lists are not empty and have the same length
            if not self.strike or not self.implied_volatility or not self.underlying_price:
                self.iv_skew = None
                self.skew_depth = None
            else:
                # If you potentially have multiple underlyings, decide which underlying price you care about
                # For simplicity, assume we use the first underlying price
                current_underlying_price = self.underlying_price[0]

                # Zip together strike + IV and filter out None
                strike_iv_pairs = [
                    (s, iv)
                    for s, iv in zip(self.strike, self.implied_volatility)
                    if s is not None and iv is not None
                ]
                
                if not strike_iv_pairs:
                    self.iv_skew = None
                    self.skew_depth = None
                else:
                    # Sort by IV ascending
                    strike_iv_pairs.sort(key=lambda x: x[1])  # (strike, implied_vol) -> sort by implied_vol

                    # Take the lowest-IV pair
                    lowest_iv_strike, lowest_iv_value = strike_iv_pairs[0]

                    # 1) Determine iv_skew
                    if lowest_iv_strike < current_underlying_price:
                        self.iv_skew = "put skew"
                    else:
                        self.iv_skew = "call skew"

        # 2) Determine skew_depth

        # a) Identify an ATM strike (the strike closest to underlying)
        #    Sort all strikes numerically, pick the one that is nearest
        unique_strikes = sorted(set(s for s in self.strike if s is not None))
        
        # Find the ATM strike by absolute difference
        atm_strike = min(unique_strikes, key=lambda x: abs(x - current_underlying_price))

        # b) Now get the indices of 'lowest_iv_strike' and 'atm_strike'
        atm_index = unique_strikes.index(atm_strike)
        lowest_iv_index = unique_strikes.index(lowest_iv_strike)

        # c) skew_depth is the difference in indices (how many intervals away)
        self.skew_depth = abs(lowest_iv_index - atm_index)

        greeks_dict = [
            self.compute_additional_greeks(S, T, sigma, gamma, r=self.risk_free_rate[0] / 100.0)
            for S, T, sigma, gamma in zip(
                self.underlying_price,
                self.time_to_maturity,
                self.implied_volatility,
                self.gamma,
            )
        ]

        self.zomma = [i.get('zomma') for i in greeks_dict]
        self.charm = [i.get('charm') for i in greeks_dict]
        self.vera = [i.get('vera') for i in greeks_dict]
        self.vanna = [i.get('vanna') for i in greeks_dict]
        self.speed = [i.get('speed') for i in greeks_dict]
        self.color = [i.get('color') for i in greeks_dict]
        self.ultima = [i.get('ultima') for i in greeks_dict]
        self.vomma = [i.get('vomma') for i in greeks_dict]
        self.veta = [i.get('veta') for i in greeks_dict]
        self.epsilon = [i.get('epsilon') for i in greeks_dict]
        self.lambda_ = [i.get('lambda') for i in greeks_dict]
        self.avg_iv = self.average_implied_volatility()


        # Create a dictionary of all attributes
        data_dict = {
            'break_even': self.break_even,
            'iv': self.implied_volatility,
            'iv_skew': self.iv_skew,
            'avg_iv': self.avg_iv,
            'oi': self.open_interest,
            'risk_free_rate': self.risk_free_rate,
            'volume': self.volume,
            'high': self.high,
            'low': self.low,
            'vwap': self.vwap,
            'open': self.open,
            'close': self.close,
            'change_percent': self.change_percent,
            'strike': self.strike,
            'expiry': self.expiry,
            'call_put': self.cp,
            'exercise_style': self.exercise_style,
            'option_symbol': self.option_symbol,
            'theta': self.theta,
            'theta_decay_rate': self.theta_decay_rate,
            'delta': self.delta,
            'delta_theta_ratio': self.delta_theta_ratio,
            'charm': self.charm,
            'gamma': self.gamma,
            'color': self.color,
            'speed': self.speed,
            'vega': self.vega,
            'vega_impact': self.vega_impact,
            'vera': self.vera,
            'veta': self.veta,
            'epsilon': self.epsilon,
            'lambda': self.lambda_,
            'vomma': self.vomma,
            'vanna': self.vanna,
            'ultima': self.ultima,
            'timestamp': self.sip_timestamp,
            'conditions': self.conditions,
            'price': self.price,
            'trade_size': self.trade_size,
            'exchange': self.exchange,
            'ask': self.ask,
            'mid': self.mid,
            'bid': self.bid,
            'bid_size': self.bid_size,
            'ask_size': self.ask_size,
            'change_to_breakeven': self.change_to_breakeven,
            'underlying_price': self.underlying_price,
            'ticker': self.ticker,
            'dte': self.dte,
            'time_value': self.time_value,
            'moneyness': self.moneyness,
            'liquidity_score': self.liquidity_score,
            'spread': self.spread,
            'spread_pct': self.spread_pct,
            'intrinsic_value': self.intrinsic_value,
            'extrinsic_value': self.extrinsic_value,
            'leverage_ratio': self.leverage_ratio,
            'return_on_risk': self.return_on_risk,
            'velocity': self.velocity,
            'gamma_risk': self.gamma_risk,
            'sensitivity': self.sensitivity,
            'ltr': self.ltr,
            'rrs': self.rrs,
            'gbs': self.gbs,
            'opp': self.opp,
            
        }

        # Convert data_dict to DataFrame
        self.df = pd.DataFrame(data_dict)
        pd.set_option('display.float_format', lambda x: f'{x:.6f}')
        # Now proceed with calculations that can be vectorized over the DataFrame
        # Handle cases where the DataFrame might be empty
        if self.df.empty:
            logging.warning("The DataFrame is empty. No data to process.")
            return

        # Convert relevant columns to numeric data types
        numeric_cols = [
            'break_even', 'iv', 'oi', 'risk_free_rate', 'volume',
            'high', 'low', 'vwap', 'open', 'close', 'change_percent', 'strike', 'theta', 'delta',
            'gamma', 'vega', 'price', 'trade_size', 'ask', 'bid', 'bid_size', 'ask_size', 'mid',
            'change_to_breakeven', 'underlying_price', 'dte', 'time_value', 'liquidity_score',
            'spread', 'intrinsic_value', 'extrinsic_value', 'leverage_ratio', 'spread_pct',
            'return_on_risk', 'velocity', 'gamma_risk', 'theta_decay_rate', 'vega_impact',
            'delta_theta_ratio', 'sensitivity', 'ltr', 'rrs', 'gbs', 'opp'
        ]

        self.df[numeric_cols] = self.df[numeric_cols].apply(pd.to_numeric, errors='coerce')

   
        self.df['iv_percentile'] = self.df['iv'].rank(pct=True)
        self.iv_percentile = self.df['iv_percentile']

        self.df['t_years'] = self.df['dte'] / 365.0

    def average_implied_volatility(self):
        # Ensure inputs are lists and filter out None values
        ivs = np.array([iv if iv is not None else 0 for iv in self.implied_volatility])
        ois = np.array([oi if oi is not None else 0 for oi in self.open_interest])

        # Only consider entries where both implied volatility and open interest are non-zero
        valid_indices = (ivs > 0) & (ois > 0)
        ivs = ivs[valid_indices]
        ois = ois[valid_indices]

        # Calculate the total implied volatility weighted by open interest
        total_iv = np.dot(ivs, ois)
        # Calculate the total open interest
        total_oi = np.sum(ois)
        # Calculate the average implied volatility
        return total_iv / total_oi if total_oi > 0 else 0
    def to_float(self, value, default=None):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def safe_round(self, value, ndigits):
        if value is not None:
            return round(value, ndigits)
        else:
            return None

    def calculate_moneyness(self, cp, strike, underlying_price):
        if cp is None or strike is None or underlying_price is None:
            return 'Unknown'
        elif cp == 'call':
            if underlying_price > strike:
                return 'ITM'
            elif underlying_price < strike:
                return 'OTM'
            else:
                return 'ATM'
        elif cp == 'put':
            if underlying_price < strike:
                return 'ITM'
            elif underlying_price > strike:
                return 'OTM'
            else:
                return 'ATM'
        else:
            return 'Unknown'

    def calculate_intrinsic_value(self, cp, strike, underlying_price):
        if cp == 'call' and underlying_price is not None and strike is not None:
            return self.safe_round(max(0.0, underlying_price - strike), 3)
        elif cp == 'put' and underlying_price is not None and strike is not None:
            return self.safe_round(max(0.0, strike - underlying_price), 3)
        else:
            return None

    def calculate_return_on_risk(self, cp, price, strike, underlying_price):
        if cp == 'call' and price and strike and underlying_price and strike > underlying_price:
            return self.safe_round(price / (strike - underlying_price), 3)
        elif cp == 'put' and price and strike and underlying_price and strike < underlying_price:
            return self.safe_round(price / (underlying_price - strike), 3)
        else:
            return 0.0

    def calculate_opp(self, moneyness, sensitivity, ltr, rrs):
        moneyness_scores = {'ITM': 1, 'ATM': 0.5, 'OTM': 0.2}
        ms = moneyness_scores.get(moneyness, 0)
        oss = sensitivity
        if ms is not None and oss is not None and ltr is not None and rrs is not None:
            return self.safe_round(ms * oss * ltr * rrs, 3)
        else:
            return None

    def normalize_column(self, column_name, scaling_factor=1):
        col_vals = self.df[column_name].dropna()
        if not col_vals.empty:
            min_val = col_vals.min()
            max_val = col_vals.max()
            if max_val != min_val:
                self.df[column_name] = ((self.df[column_name] - min_val) / (max_val - min_val)) * scaling_factor

    def calculate_d1_row(self, row):
        u = row['underlying_price']
        s = row['strike']
        r = row['risk_free_rate'] / 100  # Convert to decimal
        iv = row['iv']
        t = row['t_years']
        epsilon = 1e-10
        try:
            if u is None or s is None or r is None or iv is None or t is None:
                return None
            if u <= epsilon or s <= epsilon or iv <= epsilon or t <= epsilon:
                return None
            ln_us = np.log(u / s)
            term = (r + 0.5 * iv ** 2) * t
            denominator = iv * np.sqrt(t)
            d1 = (ln_us + term) / denominator
            return d1
        except Exception:
            return None


 

    def __repr__(self) -> str:
        return f"UniversalOptionSnapshot(df={self.df})"

    def __getitem__(self, index):
        return self.df[index]

    def __setitem__(self, index, value):
        self.df[index] = value

    def __iter__(self):
        self.iter = self.df.itertuples()
        return self

    def __next__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopIteration
        


    @staticmethod
    def compute_additional_greeks(S, T, sigma, gamma, *, r=None, option_price=None, q=None, delta=None):
        """
        Compute second- and third-order Greeks numerically, with fallback to None.

        Extended Greek definitions (summary):
            - Delta (Δ): Price sensitivity to changes in the underlying asset.
            - Gamma (Γ): Rate of change of Delta.
            - Theta (θ): Sensitivity to time decay.
            - Vega (ν): Sensitivity to implied volatility.
            - Rho (ρ): Sensitivity to interest rate changes.

            Additional, higher-order Greeks:
            - Vanna (∂Delta/∂σ or DvegaDspot):
              Sensitivity of Delta w.r.t. volatility. Also interpreted as the rate of change of Vega w.r.t. the underlying price.
            - Vomma (∂Vega/∂σ or Vega Convexity, Volga):
              Measures the rate of change of Vega as volatility changes.
            - Veta (∂Vega/∂t):
              Sensitivity of Vega to the passage of time.
            - Vera (DrhoDvol or Rhova):
              Sensitivity of Rho to volatility changes.
            - Speed (DgammaDspot):
              The rate of change of Gamma with respect to changes in the underlying price.
            - Zomma (DgammaDvol):
              The rate of change of Gamma with respect to volatility.
            - Color (DgammaDtime):
              The rate of change of Gamma with respect to the passage of time.
            - Ultima (DvommaDvol):
              The sensitivity of Vomma to changes in volatility.
            - Charm (DdeltaDtime):
              The rate of change of Delta with respect to time.

            Additional Greeks:
            - Lambda (λ) or Omega (Ω) or Elasticity:
              The percentage change in an option's price for a 1% change in the underlying price.
              Often computed as: Lambda = Delta * (S / OptionPrice).
            - Epsilon (ε) or Psi (ψ):
              The percentage change in the option’s price for a 1% change in the dividend yield.

        Parameters:
            S (float):     Underlying price.
            T (float):     Time to maturity in years.
            sigma (float): Implied volatility (e.g. 0.20 for 20%).
            gamma (float): Gamma of the option.
            r (float):     Risk-free rate (decimal, e.g. 0.0425 for 4.25%).
            
            option_price (float, optional): 
                The actual price of the option (call/put). Needed to compute Lambda precisely.
            q (float, optional): 
                Continuous dividend yield (decimal). Needed for Epsilon/Psi calculations.
            delta (float, optional): 
                Option’s Delta, which may be used in the formulas for Lambda or Epsilon.

        Returns:
            dict: Additional Greeks, with None for any invalid calculations or
                  if insufficient data is provided for the formula.
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
            'lambda': None,
            'epsilon': None,
        }

        try:
            # ---- Existing Greeks ----
            # Vanna: ∂Delta/∂σ (DvegaDspot)
            if sigma and gamma:
                result['vanna'] = gamma / sigma

            # Vomma: ∂²V/∂σ²
            if sigma and gamma:
                result['vomma'] = gamma / sigma**2

            # Veta: ∂Vega/∂t
            if S and sigma and gamma and T:
                result['veta'] = -S * sigma * gamma * T

            # Vera (Rhova): ∂²V/∂σ∂r
            if r and gamma:
                # Placeholder approximate definition
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

            # Charm: ∂Delta/∂T
            if T and gamma and S and sigma:
                result['charm'] = -gamma * (2 / T)

            # ---- New Greeks ----
            # Lambda (λ) or Omega (Ω) or Elasticity
            # Usually: Lambda = Delta * (S / OptionPrice).
            # If we don't have the actual option price, we can't compute it precisely.
            if delta is not None and option_price is not None and option_price != 0:
                # Convert to a percentage measure: for a 1% change in S
                # (This is a commonly cited formula.)
                result['lambda'] = delta * (S / option_price)

            # Epsilon (ε) or Psi (ψ):
            # The partial derivative of the option price w.r.t. dividend yield (as a %) 
            # can be option-type dependent (e.g., calls differ from puts).
            # Common formula for a non-dividend-protected call: Epsilon ~ -T * S * e^(-q T) * N(d1)
            # For a put, sign might differ, etc. We treat it generically here.
            #
            # If you want a simpler approach for "percentage change in the option price 
            # for a 1% change in dividend yield," you might do something like:
            if q is not None and delta is not None and T and S:
                # Placeholder approach or approximate formula:
                # Let's assume a call-like payoff (sign can differ for puts).
                # We need d1 if we want a more precise formula, but we will keep it simple:
                #    Epsilon = -T * (S * delta)  (very rough placeholder)
                #
                # In reality you might:
                # dC/dq = -T * S * e^(-q*T) * N(d1) [BSM call-specific formula]
                # Epsilon = (dC/dq) * (1 / option_price) * 100  (if you want "per 1% change")
                #
                # We’ll do a minimal placeholder to show how to incorporate it:
                result['epsilon'] = -T * S * delta

        except Exception as e:
            print(f"Error computing additional Greeks: {e}")

        return result

    @staticmethod
    def compute_d1_d2(S, K, T, r, sigma, q=0.0):
        """
        Compute the Black–Scholes d1 and d2 terms:

            d1 = [ln(S/K) + (r - q + sigma^2/2)*T] / (sigma * sqrt(T))
            d2 = d1 - sigma * sqrt(T)

        Where:
            S (float):     Underlying price
            K (float):     Strike price
            T (float):     Time to maturity (years)
            r (float):     Risk-free interest rate (annualized, decimal)
            sigma (float): Implied volatility (decimal)
            q (float):     Continuous dividend yield (decimal). Defaults to 0.

        Returns:
            (d1, d2) as a tuple of floats, or (None, None) if inputs are invalid.
        """
        if S is None or K is None or T is None or sigma is None:
            return (None, None)
        if (S <= 0) or (K <= 0) or (T <= 0) or (sigma <= 0):
            return (None, None)

        try:
            d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
            d2 = d1 - sigma * np.sqrt(T)
        except Exception as e:
            print(f"Error computing d1 and d2: {e}")
            return (None, None)

        return (d1, d2)

    def add_additional_greeks(self):
        """
        Compute and add additional Greeks (vanna, vomma, veta, vera, speed, zomma,
        color, ultima, charm, lambda, epsilon) to self.as_dataframe for each row.

        Note: If you actually have 'option_price', 'q', 'delta', etc. in your data,
        you can provide them here in the loop and pass them to compute_additional_greeks.
        Otherwise, placeholders or partial results will be returned.
        """
        additional_greeks = []
        for idx, row in self.as_dataframe.iterrows():
            S      = row.get('underlying_price')
            T      = self.time_to_maturity[idx]
            sigma  = row.get('iv')
            gamma  = row.get('gamma')
            # Optionally retrieve the following if you have them in your data:
            # option_price = row.get('option_price')
            # q           = row.get('continuous_dividend_yield')
            # delta       = row.get('delta')

            # Convert stored risk-free rate from percentage to decimal
            result = self.compute_additional_greeks(
                S=S, T=T, sigma=sigma, gamma=gamma,
                r=self.risk_free_rate / 100.0,
                # option_price=option_price,
                # q=q,
                # delta=delta
            )
            additional_greeks.append(result)

        # Append each Greek to the DataFrame
        for greek in [
            'vanna', 'vomma', 'veta', 'vera', 'speed', 'zomma', 'color',
            'ultima', 'charm', 'lambda'
        ]:
            self.df[greek] = [g[greek] for g in additional_greeks]







class UniversalOptionSnapshot2:
    def __init__(self, results):

        session = [i['session'] if 'session' in i else 0 for i in results]


#         self.break_even = [float(i['break_even_price']) if 'break_even_price' is not None and 'break_even_price' in i else None for i in results]
#         self.implied_volatility = [float(i['implied_volatility']) if 'implied_volatility' in i else None for i in results] 
#         self.open_interest = [float(i['open_interest']) if 'open_interest' in i else None for i in results]

#         day = [i['day'] if 'day' in i else 0 for i in results]
#         self.volume = [float(i.get('volume',0)) for i in day]
#         self.high = [float(i.get('high',0)) for i in day]
#         self.low = [float(i.get('low',0)) for i in day]
#         self.vwap = [float(i.get('vwap',0)) for i in day]
#         self.open = [float(i.get('open',0)) for i in day]
#         self.close = [float(i.get('close',0)) for i in day]
#         self.change_percent= [round(float(i.get('change_percent',0))*100,2) for i in day]



#         details = [i['details'] for i in results]
#         self.strike = [float(i['strike_price']) if 'strike_price' in i else None for i in details]
#         self.expiry = [i['expiration_date'] if 'expiration_date' in i else None for i in details]
#         # Convert the expiration dates into a pandas Series
#         expiry_series = pd.Series(self.expiry)
#         expiry_series = pd.to_datetime(expiry_series)

#         self.contract_type = [i['contract_type'] if 'contract_type' in i else None for i in details]
#         self.exercise_style = [i['exercise_style'] if 'exercise_style' in i else None for i in details]
#         self.ticker = [i['ticker'] if 'ticker' in i else None for i in details]

#         greeks = [i.get('greeks') for i in results]
#         self.theta = [round(float(i['theta']),4) if 'theta' in i else None for i in greeks]
#         self.delta = [round(float(i['delta']),4) if 'delta' in i else None for i in greeks]
#         self.gamma = [round(float(i['gamma']),4) if 'gamma' in i else None for i in greeks]
#         self.vega = [round(float(i['vega']),4) if 'vega' in i else None for i in greeks]


#         last_trade = [i['last_trade'] if i['last_trade'] is not None else None for i in results]
#         self.sip_timestamp = [i['sip_timestamp'] if 'sip_timestamp' in i else None for i in last_trade]
#         self.conditions = [i['conditions'] if 'conditions' in i else None for i in last_trade]
#         #self.conditions = [condition for sublist in self.conditions for condition in (sublist if isinstance(sublist, list) else [sublist])]
#         self.trade_price = [float(i['price']) if 'price' in i else None for i in last_trade]
#         self.trade_size = [float(i['size']) if 'size' in i else None for i in last_trade]
#         self.exchange = [i['exchange'] if 'exchange' in i else None for i in last_trade]
#         #self.exchange = [OPTIONS_EXCHANGES.get(i) for i in self.exchange]

#         last_quote = [i['last_quote'] if i['last_quote'] is not None else None for i in results]
#         self.ask = [float(i['ask']) if 'ask' in i and i['ask'] is not None else None for i in last_quote]
#         self.bid = [float(i['bid']) if 'bid' in i and i['bid'] is not None else None for i in last_quote]
#         self.bid_size = [float(i['bid_size']) if 'bid_size' in i and i['bid_size'] is not None else None for i in last_quote]
#         self.ask_size = [float(i['ask_size']) if 'ask_size' in i and i['ask_size'] is not None else None for i in last_quote]
#         self.midpoint = [float(i['midpoint']) if 'midpoint' in i and i['midpoint'] is not None else None for i in last_quote]



#         underlying_asset = [i['underlying_asset'] if i['underlying_asset'] is not None else None for i in results]
#         self.change_to_breakeven = [float(i['change_to_break_even']) if 'change_to_break_even' in i else None for i in underlying_asset]
#         self.underlying_price = [float(i.get('price')) if i.get('price') is not None else None for i in underlying_asset]

#         self.underlying_ticker = [i['ticker'] if 'ticker' in i else None for i in underlying_asset]
#         today = pd.Timestamp(datetime.today())
        
        
#         expiry_series = pd.to_datetime(self.expiry)

#         # Today's date
#         today = pd.to_datetime(datetime.now())

#         # Calculate days to expiry for each date in the series
#         self.days_to_expiry_series = (expiry_series - today).days
#         self.time_value = [float(p) - float(s) + float(k) if p and s and k else None for p, s, k in zip(self.trade_price, self.underlying_price, self.strike)]
#         self.time_value = [round(item, 3) if item is not None else None for item in self.time_value]

#         self.moneyness = [
#             'Unknown' if u is None else (
#                 'ITM' if (ct == 'call' and s < u) or (ct == 'put' and s > u) else (
#                     'OTM' if (ct == 'call' and s > u) or (ct == 'put' and s < u) else 'ATM'
#                 )
#             ) for ct, s, u in zip(self.contract_type, self.strike, self.underlying_price)
#         ]

#         self.liquidity_indicator = [float(a_size) + float(b_size) if a_size is not None and b_size is not None else None for a_size, b_size in zip(self.ask_size, self.bid_size)]
#         self.liquidity_indicator = [round(item, 3) if item is not None else None for item in self.liquidity_indicator]

#         self.spread = [float(a) - float(b) if a is not None and b is not None else None for a, b in zip(self.ask, self.bid)]
#         self.intrinsic_value = [float(u) - float(s) if ct == 'call' and u is not None and s is not None and u > s else float(s) - float(u) if ct == 'put' and u is not None and s is not None and s > u else 0.0 for ct, u, s in zip(self.contract_type, self.underlying_price, self.strike)]
#         self.intrinsic_value =[round(item, 3) if item is not None else None for item in self.intrinsic_value]
#         self.extrinsic_value = [float(p) - float(iv) if p is not None and iv is not None else None for p, iv in zip(self.trade_price, self.intrinsic_value)]
#         self.extrinsic_value =[round(item, 3) if item is not None else None for item in self.extrinsic_value]
#         self.leverage_ratio = [float(d) / (float(s) / float(u)) if d is not None and s is not None and u is not None else None for d, s, u in zip(self.delta, self.strike, self.underlying_price)]
#         self.leverage_ratio = [round(item, 3) if item is not None else None for item in self.leverage_ratio]
#         self.spread_pct = [(float(a) - float(b)) / float(m) * 100.0 if a is not None and b is not None and m is not None and m != 0 else None for a, b, m in zip(self.ask, self.bid, self.midpoint)]

#         self.spread_pct = [round(item, 3) if item is not None else None for item in self.spread_pct]
#         self.return_on_risk = [float(p) / (float(s) - float(u)) if ct == 'call' and p is not None and s is not None and u is not None and s > u else float(p) / (float(u) - float(s)) if ct == 'put' and p is not None and s is not None and u is not None and s < u else 0.0 for ct, p, s, u in zip(self.contract_type, self.trade_price, self.strike, self.underlying_price)]
#         self.return_on_risk = [round(item, 3) if item is not None else None for item in self.return_on_risk]
#         self.option_velocity = [float(delta) / float(p) if delta is not None and p is not None else 0.0 for delta, p in zip(self.delta, self.trade_price)]
#         self.option_velocity =[round(item, 3) if item is not None else None for item in self.option_velocity]
#         self.gamma_risk = [float(g) * float(u) if g is not None and u is not None else None for g, u in zip(self.gamma, self.underlying_price)]
#         self.gamma_risk =[round(item, 3) if item is not None else None for item in self.gamma_risk]
#         self.theta_decay_rate = [float(t) / float(p) if t is not None and p is not None else None for t, p in zip(self.theta, self.trade_price)]
#         self.theta_decay_rate = [round(item, 3) if item is not None else None for item in self.theta_decay_rate]
#         self.vega_impact = [float(v) / float(p) if v is not None and p is not None else None for v, p in zip(self.vega, self.trade_price)]
#         self.vega_impact =[round(item, 3) if item is not None else None for item in self.vega_impact]
#         self.delta_to_theta_ratio = [float(d) / float(t) if d is not None and t is not None and t != 0 else None for d, t in zip(self.delta, self.theta)]
#         self.delta_to_theta_ratio = [round(item, 3) if item is not None else None for item in self.delta_to_theta_ratio]
#         #option_sensitivity score - curated - finished
#         self.oss = [(float(delta) if delta is not None else 0) + (0.5 * float(gamma) if gamma is not None else 0) + (0.1 * float(vega) if vega is not None else 0) - (0.5 * float(theta) if theta is not None else 0) for delta, gamma, vega, theta in zip(self.delta, self.gamma, self.vega, self.theta)]
#         self.oss = [round(item, 3) for item in self.oss]
#         #liquidity-theta ratio - curated - finished
#         self.ltr = [
#             liquidity / abs(theta) if liquidity and theta and theta != 0 else None
#             for liquidity, theta in zip(self.liquidity_indicator, self.theta)
#         ]
#         #risk-reward score - curated - finished
#         self.rrs = [(intrinsic + extrinsic) / (iv + 1e-4) if intrinsic and extrinsic and iv else None for intrinsic, extrinsic, iv in zip(self.intrinsic_value, self.extrinsic_value, self.implied_volatility)]
#         #greeks-balance score - curated - finished
#         self.gbs = [(abs(delta) if delta else 0) + (abs(gamma) if gamma else 0) - (abs(vega) if vega else 0) - (abs(theta) if theta else 0) for delta, gamma, vega, theta in zip(self.delta, self.gamma, self.vega, self.theta)]
#         self.gbs = [round(item, 3) if item is not None else None for item in self.gbs]
#         #options profit potential: FINAL - finished
#         self.opp = [moneyness_score*oss*ltr*rrs if moneyness_score and oss and ltr and rrs else None for moneyness_score, oss, ltr, rrs in zip([1 if m == 'ITM' else 0.5 if m == 'ATM' else 0.2 for m in self.moneyness], self.oss, self.ltr, self.rrs)]
#         self.opp = [round(item, 3) if item is not None else None for item in self.opp]



                


















#         self.data_dict = {
#             'strike': self.strike,
#             'expiry': self.expiry,
#             'dte': self.days_to_expiry_series,
#             'time_value': self.time_value,
#             'moneyness': self.moneyness,
#             'liquidity_score': self.liquidity_indicator,
#             "cp": self.contract_type,
#             "change_ratio": self.change_percent,
#             'exercise_style': self.exercise_style,
#             'option_symbol': self.ticker,
#             'theta': self.theta,
#             'theta_decay_rate': self.theta_decay_rate,
#             'delta': self.delta,
#             'delta_theta_ratio': self.delta_to_theta_ratio,
#             'gamma': self.gamma,
#             'gamma_risk': self.gamma_risk,
#             'vega': self.vega,
#             'vega_impact': self.vega_impact,
#             'timestamp': self.sip_timestamp,
#             'oi': self.open_interest,
#             'open': self.open,
#             'high': self.high,
#             'low': self.low,
#             'close': self.close,
#             'intrinstic_value': self.intrinsic_value,
#             'extrinsic_value': self.extrinsic_value,
#             'leverage_ratio': self.leverage_ratio,
#             'vwap':self.vwap,
#             'conditions': self.conditions,
#             'price': self.trade_price,
#             'trade_size': self.trade_size,
#             'exchange': self.exchange,
#             'ask': self.ask,
#             'bid': self.bid,
#             'spread': self.spread,
#             'spread_pct': self.spread_pct,
#             'iv': self.implied_volatility,
#             'bid_size': self.bid_size,
#             'ask_size': self.ask_size,
#             'vol': self.volume,
#             'mid': self.midpoint,
#             'change_to_breakeven': self.change_to_breakeven,
#             'underlying_price': self.underlying_price,
#             'ticker': self.underlying_ticker,
#             'return_on_risk': self.return_on_risk,
#             'velocity': self.option_velocity,
#             'sensitivity': self.oss,
#             'greeks_balance': self.gbs,
#             'opp': self.opp
            
#         }


#         # Create DataFrame from data_dict
#         self.df = pd.DataFrame(self.data_dict)
#     def __repr__(self) -> str:
#         return f"UniversalOptionSnapshot(break_even={self.break_even}, \
#                 implied_volatility={self.implied_volatility},\
#                 open_interest ={self.open_interest}, \
#                 change={self.exchange}, \
#                 expiry={self.expiry}, \
#                 ticker={self.ticker} \
#                 contract_type={self.contract_type}, \
#                 exercise_style={self.exercise_style}, \
#                 theta={self.theta}, \
#                 delta={self.delta}, \
#                 gamma={self.gamma}, \
#                 vega={self.vega}, \
#                 sip_timestamp={self.sip_timestamp}, \
#                 conditions={self.conditions}, \
#                 trade_price={self.trade_price}, \
#                 trade_size={self.trade_size}, \
#                 exchange={self.exchange}, \
#                 ask={self.ask}, \
#                 bid={self.bid}, \
#                 bid_size={self.bid_size}, \
#                 ask_size={self.ask_size}, \
#                 midpoint={self.midpoint}, \
#                 change_to_breakeven={self.change_to_breakeven}, \
#                 underlying_price={self.underlying_price}, \
#                 underlying_ticker={self.underlying_ticker})"
    
#     def __getitem__(self, index):
#         return self.df[index]

#     def __setitem__(self, index, value):
#         self.df[index] = value
#     def __iter__(self):
#         # If df is a DataFrame, it's already iterable (over its column labels)
#         # To iterate over rows, use itertuples or iterrows
#         self.iter = self.df.itertuples()
#         return self

#     def __next__(self):
#         # Just return the next value from the DataFrame iterator
#         try:
#             return next(self.iter)
#         except StopIteration:
#             # When there are no more rows, stop iteration
#             raise StopIteration
        




class SPXSNAPSHOT:
    def __init__(self, results):
        self.break_even = [float(i['break_even_price']) if 'break_even_price' is not None and 'break_even_price' in i else None for i in results]
        self.implied_volatility = [float(i['implied_volatility']) if 'implied_volatility' in i else None for i in results] 
        self.open_interest = [float(i['open_interest']) if 'open_interest' in i else None for i in results]
        
        
        day = [i['day'] if 'day' in i else 0 for i in results]
        self.volume = [float(i.get('volume',0)) for i in day]
        self.high = [float(i.get('high',0)) for i in day]
        self.low = [float(i.get('low',0)) for i in day]
        self.vwap = [float(i.get('vwap',0)) for i in day]
        self.open = [float(i.get('open',0)) for i in day]
        self.close = [float(i.get('close',0)) for i in day]
        self.change_percent= [round(float(i.get('change_percent',0))) for i in day]



        details = [i['details'] if 'details' in i else 0 for i in results]
        self.strike = [float(i['strike_price']) if 'strike_price' in i else None for i in details]
        self.expiry = [i['expiration_date'] if 'expiration_date' in i else None for i in details]
        # Convert the expiration dates into a pandas Series
        expiry_series = pd.Series(self.expiry)
        expiry_series = pd.to_datetime(expiry_series)

        self.contract_type = [i['contract_type'] if 'contract_type' in i else None for i in details]
        self.exercise_style = [i['exercise_style'] if 'exercise_style' in i else None for i in details]
        self.ticker = [i['ticker'] if 'ticker' in i else None for i in details]

        greeks = [i.get('greeks') for i in results]
        self.theta = [round(float(i['theta']),4) if 'theta' in i else None for i in greeks]
        self.delta = [round(float(i['delta']),4) if 'delta' in i else None for i in greeks]
        self.gamma = [round(float(i['gamma']),4) if 'gamma' in i else None for i in greeks]
        self.vega = [round(float(i['vega']),4) if 'vega' in i else None for i in greeks]


        last_trade = [i['last_trade'] if i['last_trade'] is not None else None for i in results]
        self.sip_timestamp = [i['sip_timestamp'] if 'sip_timestamp' in i else None for i in last_trade]
        self.conditions = [i['conditions'] if 'conditions' in i else None for i in last_trade]
        self.conditions = [condition for sublist in self.conditions for condition in (sublist if isinstance(sublist, list) else [sublist])]
        self.trade_price = [float(i['price']) if 'price' in i else None for i in last_trade]
        self.trade_size = [float(i['size']) if 'size' in i else None for i in last_trade]
        self.exchange = [i['exchange'] if 'exchange' in i else None for i in last_trade]


        last_quote = [i['last_quote'] if i['last_quote'] is not None else None for i in results]
        self.ask = [float(i['ask']) if 'ask' in i and i['ask'] is not None else None for i in last_quote]
        self.bid = [float(i['bid']) if 'bid' in i and i['bid'] is not None else None for i in last_quote]
        self.bid_size = [float(i['bid_size']) if 'bid_size' in i and i['bid_size'] is not None else None for i in last_quote]
        self.ask_size = [float(i['ask_size']) if 'ask_size' in i and i['ask_size'] is not None else None for i in last_quote]
        self.midpoint = [float(i['midpoint']) if 'midpoint' in i and i['midpoint'] is not None else None for i in last_quote]



        underlying_asset = [i['underlying_asset'] if i['underlying_asset'] is not None else None for i in results]
        self.change_to_breakeven = [float(i['change_to_break_even']) if 'change_to_break_even' in i else None for i in underlying_asset]
        self.underlying_price = [float(i.get('price')) if i.get('price') is not None else None for i in underlying_asset]

        self.risk_free_rate = [self.risk_free_rate] * len(self.underlying_price)
        self.underlying_ticker = [i['ticker'] if 'ticker' in i else None for i in underlying_asset]

        
        expiry_series = pd.to_datetime(self.expiry)

        # Today's date
        today = pd.to_datetime(datetime.now())

  
        self.data_dict = {
            'strike': self.strike,
            'expiry': self.expiry,
            'cp': self.contract_type,
            'change_ratio': self.change_percent,
            'exercise_style': self.exercise_style,
            'option_symbol': self.ticker,
            'theta': self.theta,
            'delta': self.delta,
            'gamma': self.gamma,
            'vega': self.vega,
            'timestamp': self.sip_timestamp,
            'oi': self.open_interest,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'vwap': self.vwap,
            'conditions': self.conditions,
            'price': self.trade_price,
            'trade_size': self.trade_size,
            'exchange': self.exchange,
            'ask': self.ask,
            'bid': self.bid,
            'iv': self.implied_volatility,
            'bid_size': self.bid_size,
            'ask_size': self.ask_size,
            'vol': self.volume,
            'mid': self.midpoint,
            'change_to_breakeven': self.change_to_breakeven,
            'underlying_price': self.underlying_price,
            'ticker': self.underlying_ticker,
        }

        self.df = pd.DataFrame(self.data_dict)



class SpxSnapshot:
    def __init__(self, data):
        self.data=data
        self.session = [i.get('session', {}) if i is not None else {} for i in self.data]
        self.change = [
            float(i.get("change")) if isinstance(i.get("change"), (int, float)) else None
            for i in self.data
        ]
        self.risk_free_rate=3.79
        self.ticker = [i.get('ticker', {}) if i is not None else {} for i in self.data]
        self.name = [i.get('name', {}) if i is not None else {} for i in self.data]
        self.change_percent = [ float(i.get("change_percent")) if isinstance(i.get("change_percent"), (int, float)) else None for i in self.session ]
        self.close = [ float(i.get("close")) if isinstance(i.get("close"), (int, float)) else None for i in self.session ]
        self.early_trading_change = [ float(i.get("early_trading_change")) if isinstance(i.get("early_trading_change"), (int, float)) else None for i in self.session ]
        self.early_trading_change_percent = [ float(i.get("early_trading_change_percent")) if isinstance(i.get("early_trading_change_percent"), (int, float)) else None for i in self.session ]
        self.high = [ float(i.get("high")) if isinstance(i.get("high"), (int, float)) else None for i in self.session ]
        self.late_trading_change = [ float(i.get("late_trading_change")) if isinstance(i.get("late_trading_change"), (int, float)) else None for i in self.session ]
        self.late_trading_change_percent = [ float(i.get("late_trading_change_percent")) if isinstance(i.get("late_trading_change_percent"), (int, float)) else None for i in self.session ]
        self.low = [ float(i.get("low")) if isinstance(i.get("low"), (int, float)) else None for i in self.session ]
        self.open = [ float(i.get("open")) if isinstance(i.get("open"), (int, float)) else None for i in self.session ]
        self.previous_close = [ float(i.get("previous_close")) if isinstance(i.get("previous_close"), (int, float)) else None for i in self.session ]
        self.volume = [ float(i.get("volume")) if isinstance(i.get("volume"), (int, float)) else None for i in self.session ]

        self.details = [i.get('details', {}) if i is not None else {} for i in self.data]
        self.contract_type = [i.get("contract_type", {}) if i is not None else {} for i in self.details]
        self.exercise_style = [i.get("exercise_style", {}) if i is not None else {} for i in self.details]
        self.expiry = [i.get("expiration_date", {}) if i is not None else {} for i in self.details]
        self.shares_per_contract = [i.get("shares_per_contract", {}) if i is not None else {} for i in self.details]
        self.strike = [i.get("strike_price", {}) if i is not None else {} for i in self.details]



        self.greeks = [i.get('greeks') if isinstance(i, dict) and i is not None else {} for i in self.data]
        print(self.greeks)
        self.delta = [ round(float(i.get('delta')) * 100, 2) if isinstance(i.get('delta'), (int, float)) else None for i in self.greeks ]
        self.gamma = [ round(float(i.get('gamma')) * 100, 2) if isinstance(i.get('gamma'), (int, float)) else None for i in self.greeks ]
        self.theta = [ round(float(i.get('theta')) * 100, 2) if isinstance(i.get('theta'), (int, float)) else None for i in self.greeks ]
        self.vega = [ round(float(i.get('vega')) * 100, 2) if isinstance(i.get('vega'), (int, float)) else None for i in self.greeks ]




        self.last_quote = [i.get('last_quote', {}) if isinstance(i, dict) else {} for i in self.data]
        self.ask = [ float(i.get("ask")) if isinstance(i.get("ask"), (int, float)) else None for i in self.last_quote ]
        self.ask_exchange = [ i.get("ask_exchange") if isinstance(i.get("ask_exchange"), (int, float)) else None for i in self.last_quote ]
        self.ask_size = [ float(i.get("ask_size")) if isinstance(i.get("ask_size"), (int, float)) else None for i in self.last_quote ]
        self.bid = [ float(i.get("bid")) if isinstance(i.get("bid"), (int, float)) else None for i in self.last_quote ]
        self.bid_exchange = [ i.get("bid_exchange") if isinstance(i.get("bid_exchange"), (int, float)) else None for i in self.last_quote ]
        self.bid_size = [ float(i.get("bid_size")) if isinstance(i.get("bid_size"), (int, float)) else None for i in self.last_quote ]
        self.midpoint = [ float(i.get("midpoint")) if isinstance(i.get("midpoint"), (int, float)) else None for i in self.last_quote ]

        


        
        self.last_trade = [i.get('last_trade', {}) if isinstance(i, dict) else {} for i in self.data]
        self.conditions = [
            i.get('conditions')[0] if i.get('conditions') and isinstance(i.get('conditions'), list) else None
            for i in self.last_trade
        ]
        self.last_trade_price = [
            float(i.get("price")) if isinstance(i.get("price"), (int, float)) else None
            for i in self.last_trade
        ]
        self.last_trade_size = [
            float(i.get("size")) if isinstance(i.get("size"), (int, float)) else None
            for i in self.last_trade
        ]

        # Fixing the 'underlying_asset' section
        self.underlying_asset = [i.get('underlying_asset', {}) if isinstance(i, dict) else {} for i in self.data]
        self.last_trade_timestamp = [
       int(i.get("sip_timestamp")) if isinstance(i.get("sip_timestamp"), (int, float, str)) else None
    for i in self.last_trade
    ]
        self.underlying_price = [
            float(i.get("price")) if isinstance(i.get("price"), (int, float)) else None
            for i in self.underlying_asset
        ]
        self.risk_free_rate = [self.risk_free_rate] * len(self.underlying_price)
        self.underlying_symbol= [i.get("ticker", {}).replace('I:', '') for i in self.underlying_asset]

        
        # Safely parse the implied_volatility values, ensuring they are numbers self.iv = [ round(float(i.get('implied_volatility')) * 100, 2) if isinstance(i.get('implied_volatility'), (int, float)) else None for i in self.data ]
        # Safely parse the implied_volatility values, ensuring they are numbers
        self.oi = [ i.get('oi') if isinstance(i.get('oi'), (int, float)) else None for i in self.data]
        self.iv = [float(i['iv']) if 'iv' in i else None for i in self.data] 
        
        
        
        
        
        today = pd.Timestamp(datetime.today())
        
        
        expiry_series = pd.to_datetime(self.expiry)

        # Today's date
        today = pd.to_datetime(datetime.now())

        # Calculate days to expiry for each date in the series
        self.days_to_expiry_series = (expiry_series - today).days
        self.time_value = [float(p) - float(s) + float(k) if p and s and k else None for p, s, k in zip(self.last_trade_price, self.underlying_price, self.strike)]
        self.time_value = [round(item, 3) if item is not None else None for item in self.time_value]

        self.moneyness = [
            'Unknown' if u is None else (
                'ITM' if (ct == 'call' and s < u) or (ct == 'put' and s > u) else (
                    'OTM' if (ct == 'call' and s > u) or (ct == 'put' and s < u) else 'ATM'
                )
            ) for ct, s, u in zip(self.contract_type, self.strike, self.underlying_price)
        ]

        self.liquidity_indicator = [float(a_size) + float(b_size) if a_size is not None and b_size is not None else None for a_size, b_size in zip(self.ask_size, self.bid_size)]
        self.liquidity_indicator = [round(item, 3) if item is not None else None for item in self.liquidity_indicator]

        self.spread = [float(a) - float(b) if a is not None and b is not None else None for a, b in zip(self.ask, self.bid)]
        self.intrinsic_value = [float(u) - float(s) if ct == 'call' and u is not None and s is not None and u > s else float(s) - float(u) if ct == 'put' and u is not None and s is not None and s > u else 0.0 for ct, u, s in zip(self.contract_type, self.underlying_price, self.strike)]
        self.intrinsic_value =[round(item, 3) if item is not None else None for item in self.intrinsic_value]
        self.extrinsic_value = [float(p) - float(iv) if p is not None and iv is not None else None for p, iv in zip(self.last_trade_price, self.intrinsic_value)]
        self.extrinsic_value =[round(item, 3) if item is not None else None for item in self.extrinsic_value]
        self.leverage_ratio = [float(d) / (float(s) / float(u)) if d is not None and s is not None and u is not None else None for d, s, u in zip(self.delta, self.strike, self.underlying_price)]
        self.leverage_ratio = [round(item, 3) if item is not None else None for item in self.leverage_ratio]
        self.spread_pct = [(float(a) - float(b)) / float(m) * 100.0 if a is not None and b is not None and m is not None and m != 0 else None for a, b, m in zip(self.ask, self.bid, self.midpoint)]

        self.spread_pct = [round(item, 3) if item is not None else None for item in self.spread_pct]
        self.return_on_risk = [float(p) / (float(s) - float(u)) if ct == 'call' and p is not None and s is not None and u is not None and s > u else float(p) / (float(u) - float(s)) if ct == 'put' and p is not None and s is not None and u is not None and s < u else 0.0 for ct, p, s, u in zip(self.contract_type, self.last_trade_price, self.strike, self.underlying_price)]
        self.return_on_risk = [round(item, 3) if item is not None else None for item in self.return_on_risk]
        self.option_velocity = [float(delta) / float(p) if delta is not None and p is not None else 0.0 for delta, p in zip(self.delta, self.last_trade_price)]
        self.option_velocity = [round(item, 3) if item is not None else None for item in self.option_velocity]
        self.gamma_risk = [float(g) * float(u) if g is not None and u is not None else None for g, u in zip(self.gamma, self.underlying_price)]
        self.gamma_risk =[round(item, 3) if item is not None else None for item in self.gamma_risk]
        self.theta_decay_rate = [float(t) / float(p) if t is not None and p is not None else None for t, p in zip(self.theta, self.last_trade_price)]
        self.theta_decay_rate = [round(item, 3) if item is not None else None for item in self.theta_decay_rate]
        self.vega_impact = [float(v) / float(p) if v is not None and p is not None else None for v, p in zip(self.vega, self.last_trade_price)]
        self.vega_impact =[round(item, 3) if item is not None else None for item in self.vega_impact]
        self.delta_to_theta_ratio = [float(d) / float(t) if d is not None and t is not None and t != 0 else None for d, t in zip(self.delta, self.theta)]
        self.delta_to_theta_ratio = [round(item, 3) if item is not None else None for item in self.delta_to_theta_ratio]


        # Liquidity-theta ratio - curated - finished
        self.ltr = [liquidity / abs(theta) if liquidity and theta else None for liquidity, theta in zip(self.liquidity_indicator, self.theta)]

        # Risk-reward score - curated - finished
        self.rrs = [(intrinsic + extrinsic) / (iv + 1e-4) if intrinsic and extrinsic and iv else None for intrinsic, extrinsic, iv in zip(self.intrinsic_value, self.extrinsic_value, self.iv)]
        scaling_factor = 1e5  # Use a scaling factor to make the values more readable

        
        
        
        
        
        
        # Option sensitivity score - curated - finished
        self.oss = [(float(delta) if delta is not None else 0) + (0.5 * float(gamma) if gamma is not None else 0) + (0.1 * float(vega) if vega is not None else 0) - (0.5 * float(theta) if theta is not None else 0) for delta, gamma, vega, theta in zip(self.delta, self.gamma, self.vega, self.theta)]
        self.oss = [round(item, 3) for item in self.oss]

      

        # Risk-reward score - curated - finished
        self.rrs = [(intrinsic + extrinsic) / (iv + 1e-4) if intrinsic and extrinsic and iv else None for intrinsic, extrinsic, iv in zip(self.intrinsic_value, self.extrinsic_value, self.iv)]
        scaling_factor = 1e5  # Use a scaling factor to make the values more readable
        # Greeks-balance score - curated - finished

        # Options profit potential: FINAL - finished
        self.opp = [moneyness_score * oss * ltr * rrs if moneyness_score and oss and ltr and rrs else None for moneyness_score, oss, ltr, rrs in zip([1 if m == 'ITM' else 0.5 if m == 'ATM' else 0.2 for m in self.moneyness], self.oss, self.ltr, self.rrs)]
        self.opp = [round(item, 3) if item is not None else None for item in self.opp]

        # Create a pandas series from implied volatility without dropping NaN values
        iv_series = pd.Series(self.iv)

        # Rank the series while leaving NaN values in place
        self.iv_percentile = [round(x, 2) if not pd.isna(x) else None for x in iv_series.rank(pct=True)]

        t_years = self.days_to_expiry_series / 365

        # Calculate d1 and d2
        d1 = [
            (np.log(u / s) + (r + 0.5 * iv**2) * t) / (iv * np.sqrt(t))
            if u is not None and u > 0 and s is not None and s > 0 and r is not None and iv is not None and iv > 0 and t > 0
            else None
            for u, s, r, iv, t in zip(
                self.underlying_price, self.strike, self.risk_free_rate, self.iv, t_years
            )
        ]

        d2 = [
            d1_val - iv * np.sqrt(t)
            if d1_val and iv and t and iv > 0 and t > 0
            else None
            for d1_val, iv, t in zip(d1, self.iv, t_years)
        ]
        # Vanna Calculation
        self.vanna = [
            (v * d1_val / iv) if v and d1_val and iv and iv > 0 else None
            for v, d1_val, iv in zip(self.vega, d1, self.iv)
        ]

        # Normalize Vanna with scaling
        vanna_min = min([x for x in self.vanna if x is not None])
        vanna_max = max([x for x in self.vanna if x is not None])
        self.vanna = [((x - vanna_min) / (vanna_max - vanna_min)) * scaling_factor if x is not None else None for x in self.vanna]


        # Vanna_Vega Calculation (Note: This is not a standard Greek and may need verification)
        self.vanna_vega = [
            (d * (v / u))
            if d is not None and v is not None and u is not None and u > 0
            else None
            for d, v, u in zip(self.delta, self.vega, self.underlying_price)
        ]

        # Vanna_Delta Calculation
        self.vanna_delta = [
            (-g * u * iv * np.sqrt(t))
            if g and u and iv and t and u > 0 and iv > 0 and t > 0
            else None
            for g, u, iv, t in zip(self.gamma, self.underlying_price, self.iv, t_years)
        ]

        # Nd1_prime Calculation
        Nd1_prime = [norm.pdf(d) if d is not None else None for d in d1]

        # Color Calculation
        self.color = [
            -g * ((d1_val / (2 * t)) + (r / (iv * np.sqrt(t))))
            if g and d1_val and iv and t and iv > 0 and t > 0
            else None
            for g, d1_val, iv, t, r in zip(self.gamma, d1, self.iv, t_years, self.risk_free_rate)
        ]

        # Charm Calculation
        self.charm = [
            -nd1p
            * (
                (2 * r * t - d2_val * iv * np.sqrt(t))
                / (2 * t * iv * np.sqrt(t))
            )
            if nd1p and d2_val and iv and t and iv > 0 and t > 0
            else None
            for nd1p, d2_val, iv, t, r in zip(Nd1_prime, d2, self.iv, t_years, self.risk_free_rate)
        ]

        # Veta Calculation
        self.veta = [
            -u * nd1p * np.sqrt(t) * (r + (d1_val * d2_val) / t)
            if u and nd1p and t and d1_val and d2_val and t > 0
            else None
            for u, nd1p, t, d1_val, d2_val, r in zip(
                self.underlying_price, Nd1_prime, t_years, d1, d2, self.risk_free_rate
            )
        ]

        # Zomma Calculation
        self.zomma = [
            g * (d1_val * d2_val - 1) / iv
            if g and d1_val and d2_val and iv and iv > 0
            else None
            for g, d1_val, d2_val, iv in zip(self.gamma, d1, d2, self.iv)
        ]

        # Speed Calculation
        self.speed = [
            -g * ((d1_val / (u * iv * np.sqrt(t))) + 1) / u
            if g and d1_val and u and iv and t and u > 0 and iv > 0 and t > 0
            else None
            for g, d1_val, u, iv, t in zip(
                self.gamma, d1, self.underlying_price, self.iv, t_years
            )
        ]
        # Ultima Calculation
        self.ultima = [
            -v * (d1_val * d2_val * (d1_val * d2_val - 1) + d1_val**2 + d2_val**2) / iv**2
            if v and d1_val and d2_val and iv and iv > 0 else None
            for v, d1_val, d2_val, iv in zip(self.vega, d1, d2, self.iv)
        ]

        # Normalize Ultima with scaling
        ultima_min = min([x for x in self.ultima if x is not None])
        ultima_max = max([x for x in self.ultima if x is not None])
        self.ultima = [((x - ultima_min) / (ultima_max - ultima_min)) * scaling_factor if x is not None else None for x in self.ultima]

        # Vomma Calculation
        self.vomma = [
            v * d1_val * d2_val / iv if v and d1_val and d2_val and iv and iv > 0 else None
            for v, d1_val, d2_val, iv in zip(self.vega, d1, d2, self.iv)
        ]

        # Normalize Vomma with scaling
        vomma_min = min([x for x in self.vomma if x is not None])
        vomma_max = max([x for x in self.vomma if x is not None])
        self.vomma = [((x - vomma_min) / (vomma_max - vomma_min)) * scaling_factor if x is not None else None for x in self.vomma]



        # Epsilon Calculation
        self.epsilon = [
            -t * u * norm.cdf(d1_val)
            if t > 0 and u is not None and d1_val is not None
            else None
            for t, u, d1_val in zip(t_years, self.underlying_price, d1)
        ]

        # Volga Calculation
        self.volga = [
            (v * d1_val * d2_val / iv) if v and d1_val and d2_val and iv > 0 else None
            for v, d1_val, d2_val, iv in zip(self.vega, d1, d2, self.iv)
        ]

        # Normalize Volga with scaling
        volga_min = min([x for x in self.volga if x is not None])
        volga_max = max([x for x in self.volga if x is not None])
        
        self.volga = [((x - volga_min) / (volga_max - volga_min)) * scaling_factor if x is not None else None for x in self.volga]



        self.vera = [
            (v * d2_val / iv) if v and d2_val and iv > 0 else None
            for v, d2_val, iv in zip(self.vega, d2, self.iv)
        ]

        # Normalize Vera
        vera_min = min([x for x in self.vera if x is not None])
        vera_max = max([x for x in self.vera if x is not None])
        self.vera = [(x - vera_min) / (vera_max - vera_min) if x is not None else None for x in self.vera]


        self.data_dict = { 
            'option_symbol': self.ticker,
            'name': self.name,
            'ticker': self.underlying_symbol,
            'strike': self.strike,
            'call_put': self.contract_type,
            'expiry': self.expiry,
            'moneyness': self.moneyness,
            'volume': self.volume,
            'oi': self.oi,
            'iv': self.iv,

            'delta': self.delta,
            'delta_theta_ratio': self.delta_to_theta_ratio,
            'gamma': self.gamma,
            'gamma_risk': self.gamma_risk,
            'theta': self.theta,
            'theta_decay_rate': self.theta_decay_rate,
            'vega': self.vega,
            'vega_impact': self.vega_impact,
            'charm': self.charm,
            'vera': self.vera,
            'volga': self.volga,
            'epsilon': self.epsilon,
            'vomma': self.vomma,
            'ultima': self.ultima,
            'speed': self.speed,
            'zomma': self.zomma,
            'veta': self.veta,
            'color': self.color,
            'vanna': self.vanna,
            'vanna_delta': self.vanna_delta,
            'vanna_vega': self.vanna_vega,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'previous_close': self.previous_close,
            'intrinsic_value': self.intrinsic_value,
            'extrinsic_value': self.extrinsic_value,
            'change': self.change,
            'early_change': self.early_trading_change,
            'late_change': self.late_trading_change,
            'change_percent': self.change_percent,
            'early_change_percent': self.early_trading_change_percent,
            'late_change_percent': self.late_trading_change_percent,
            'last_trade_size': self.last_trade_size,
            'last_trade_price': self.last_trade_price,
            'last_trade_conditions': self.conditions,
            'bid': self.bid,
            'bid_size': self.bid_size,
            'bid_exchange': self.bid_exchange,
            'mid': self.midpoint,
            'ask': self.ask,
            'ask_size': self.ask_size,
            'ask_exchange': self.ask_exchange,
            'spread': self.spread,
            'spread_pct': self.spread_pct,
            'underlying_price': self.underlying_price,
            'option_profit_potential': self.opp,
            'liquidity_ratio': self.ltr,
            'option_sensitivity': self.oss,
            'risk_reward_score': self.rrs,
        }
        for k, v in self.data_dict.items():
            print(f"{k} LENGTH: {len(v)}")
        self.df = pd.DataFrame(self.data_dict)


    # Helper function to convert None to float and handle errors
    def safe_float(self, value):
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None
