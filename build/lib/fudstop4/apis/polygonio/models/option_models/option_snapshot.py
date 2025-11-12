import pandas as pd
import numpy as np
import math
import datetime
from fudstop4.apis.polygonio.mapping import option_condition_dict


class OptionSnapshotData:
    """
    Processes raw option snapshot data and produces a DataFrame with relevant fields.

    Attributes are computed from the provided data list (each element is assumed to be a dict)
    and include market data, option details, greeks, quotes, and underlying asset information.
    Additionally, time-to-maturity is computed for each option.
    """

    def __init__(self, data: list[dict]):
        # Extract top-level fields
        self.implied_volatility = [
            float(item.get("implied_volatility")) if item.get("implied_volatility") is not None else None
            for item in data
        ]
        self.open_interest = [
            float(item.get("open_interest")) if item.get("open_interest") is not None else None
            for item in data
        ]
        self.break_even_price = [
            float(item.get("break_even_price")) if item.get("break_even_price") is not None else None
            for item in data
        ]

        # Day data (assumed to be under the key 'day')
        day = [item.get("day") for item in data]
        self.day_close = [
            float(day_item.get("close")) if day_item and day_item.get("close") is not None else None
            for day_item in day
        ]
        self.day_high = [
            float(day_item.get("high")) if day_item and day_item.get("high") is not None else None
            for day_item in day
        ]
        self.last_updated = [
            day_item.get("last_updated") if day_item else None for day_item in day
        ]
        self.day_low = [
            float(day_item.get("low")) if day_item and day_item.get("low") is not None else None
            for day_item in day
        ]
        self.day_open = [
            float(day_item.get("open")) if day_item and day_item.get("open") is not None else None
            for day_item in day
        ]
        self.day_change_percent = [
            float(day_item.get("change_percent")) if day_item and day_item.get("change_percent") is not None else None
            for day_item in day
        ]
        self.day_change = [
            float(day_item.get("change")) if day_item and day_item.get("change") is not None else None
            for day_item in day
        ]
        self.previous_close = [
            float(day_item.get("previous_close")) if day_item and day_item.get("previous_close") is not None else None
            for day_item in day
        ]
        self.day_volume = [
            float(day_item.get("volume")) if day_item and day_item.get("volume") is not None else None
            for day_item in day
        ]
        self.day_vwap = [
            float(day_item.get("vwap")) if day_item and day_item.get("vwap") is not None else None
            for day_item in day
        ]

        # Details data
        details = [item.get("details", {}) for item in data]
        self.contract_type = [detail.get("contract_type") for detail in details]
        self.exercise_style = [detail.get("exercise_style") for detail in details]
        self.expiration_date = [detail.get("expiration_date") for detail in details]
        self.shares_per_contract = [detail.get("shares_per_contract") for detail in details]
        self.strike_price = [
            float(detail.get("strike_price"))
            if detail.get("strike_price") is not None else None
            for detail in details
        ]
        self.option_symbol = [detail.get("ticker") for detail in details]

        # Greeks data
        greeks = [item.get("greeks", {}) for item in data]
        self.delta = [
            float(g.get("delta")) if g.get("delta") is not None else None
            for g in greeks
        ]
        self.gamma = [
            float(g.get("gamma")) if g.get("gamma") is not None else None
            for g in greeks
        ]
        self.theta = [
            float(g.get("theta")) if g.get("theta") is not None else None
            for g in greeks
        ]
        self.vega = [
            float(g.get("vega")) if g.get("vega") is not None else None
            for g in greeks
        ]

        # Last Quote data
        lastquote = [item.get("last_quote", {}) for item in data]
        self.ask = [
            float(lq.get("ask")) if lq.get("ask") is not None else None
            for lq in lastquote
        ]
        self.ask_size = [
            float(lq.get("ask_size")) if lq.get("ask_size") is not None else None
            for lq in lastquote
        ]
        self.bid = [
            float(lq.get("bid")) if lq.get("bid") is not None else None
            for lq in lastquote
        ]
        self.bid_size = [
            float(lq.get("bid_size")) if lq.get("bid_size") is not None else None
            for lq in lastquote
        ]
        self.quote_last_updated = [lq.get("quote_last_updated") for lq in lastquote]
        self.midpoint = [
            float(lq.get("midpoint")) if lq.get("midpoint") is not None else None
            for lq in lastquote
        ]

        # Last Trade data
        lasttrade = [item.get("last_trade") for item in data]
        self.conditions = [
            lt.get("conditions") if lt is not None else None for lt in lasttrade
        ]
        self.exchange = [
            lt.get("exchange") if lt is not None else None for lt in lasttrade
        ]
        self.price = [
            float(lt.get("price")) if lt is not None and lt.get("price") is not None else None
            for lt in lasttrade
        ]
        self.sip_timestamp = [
            lt.get("sip_timestamp") if lt is not None else None for lt in lasttrade
        ]
        self.size = [
            float(lt.get("size")) if lt is not None and lt.get("size") is not None else None
            for lt in lasttrade
        ]

        # Underlying asset data
        underlying = [item.get("underlying_asset", {}) for item in data]
        self.change_to_break_even = [
            ua.get("change_to_break_even") for ua in underlying
        ]
        self.underlying_last_updated = [
            ua.get("underlying_last_updated") for ua in underlying
        ]
        self.underlying_price = [
            float(ua.get("price")) if ua.get("price") is not None else None
            for ua in underlying
        ]
        self.underlying_ticker = [ua.get("ticker") for ua in underlying]

        # Calculate time to maturity (in years) for each option
        self.time_to_maturity = [
            self.years_to_maturity(exp_date) for exp_date in self.expiration_date
        ]

        # Build a dictionary to create a DataFrame
        self.data_dict = {
            "iv": self.implied_volatility,
            "oi": self.open_interest,
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
            "timestamp": self.sip_timestamp,
            "size": self.size,
            "change_to_break_even": self.change_to_break_even,
            "underlying_last_updated": self.underlying_last_updated,
            "price": self.underlying_price,
            "symbol": self.underlying_ticker,
            "time_to_maturity": self.time_to_maturity
        }

        # Create the DataFrame
        self.df = pd.DataFrame(self.data_dict)

    @staticmethod
    def years_to_maturity(expiry_str: str) -> float | None:
        """
        Calculate the time to maturity in years given an expiration date string.

        Args:
            expiry_str: Expiration date in ISO format (YYYY-MM-DD or similar).

        Returns:
            Years to maturity as a float, or None if parsing fails.
        """
        if not expiry_str:
            return None
        try:
            exp_date = datetime.datetime.fromisoformat(expiry_str)
            now = datetime.datetime.now()
            delta = exp_date - now
            # Return years (ensure non-negative)
            return max(delta.days / 365.0, 0)
        except Exception:
            return None


import datetime
import numpy as np
import pandas as pd
from typing import Optional

# Assuming option_condition_dict is defined elsewhere
# option_condition_dict = { ... }

class WorkingUniversal:
    """
    Processes a list of raw option data dictionaries into a unified DataFrame with computed fields.

    The class extracts key fields, computes days-to-expiration (DTE), intrinsic/extrinsic values,
    spread percentages, premium percentages, volume-to-OI ratios, moneyness, and IV skew.
    """

    def __init__(self, data: list[dict]):
        self.risk_free_rate = 4.25
        rows = []

        # Process each raw option data dictionary into a flat row dictionary.
        for item in data:
            row = {}
            # Top-level fields
            row["break_even_price"] = item.get("break_even_price")
            row["name"] = item.get("name")
            row["market_status"] = item.get("market_status")
            row["option_symbol"] = item.get("ticker")
            row["type"] = item.get("type")

            # Session-related fields
            session = item.get("session", {})
            row["change"] = session.get("change")
            row["change_percent"] = session.get("change_percent")
            row["close"] = session.get("close")
            row["high"] = session.get("high")
            row["low"] = session.get("low")
            row["open"] = session.get("open")
            row["volume"] = session.get("volume")
            row["previous_close"] = session.get("previous_close")

            # Details fields
            details = item.get("details", {})
            row["call_put"] = details.get("contract_type")
            row["exercise_style"] = details.get("exercise_style")
            row["expiry"] = details.get("expiration_date")
            row["shares_per_contract"] = details.get("shares_per_contract")
            row["strike"] = details.get("strike_price")

            # Greeks
            greeks = item.get("greeks", {})
            row["delta"] = greeks.get("delta")
            row["gamma"] = greeks.get("gamma")
            row["theta"] = greeks.get("theta")
            row["vega"] = greeks.get("vega")

            # Implied Volatility
            row["iv"] = item.get("implied_volatility")

            # Last Quote
            last_quote = item.get("last_quote", {})
            row["ask"] = last_quote.get("ask")
            row["ask_size"] = last_quote.get("ask_size")
            row["ask_exchange"] = last_quote.get("ask_exchange")
            row["bid"] = last_quote.get("bid")
            row["bid_size"] = last_quote.get("bid_size")
            row["bid_exchange"] = last_quote.get("bid_exchange")
            row["midpoint"] = last_quote.get("midpoint")

            # Last Trade
            last_trade = item.get("last_trade", {})
            row["sip_timestamp"] = last_trade.get("sip_timestamp")
            conditions = last_trade.get("conditions", [])
            if conditions:
                try:
                    int_first = int(str(conditions[0]))
                    row["trade_conditions"] = option_condition_dict.get(int_first)
                except Exception:
                    row["trade_conditions"] = None
            else:
                row["trade_conditions"] = None
            row["trade_price"] = last_trade.get("price")
            row["trade_size"] = last_trade.get("size")
            row["trade_exchange"] = last_trade.get("exchange")

            # Open Interest
            row["oi"] = item.get("open_interest")

            # Underlying Asset fields
            underlying = item.get("underlying_asset", {})
            row["change_to_break_even"] = underlying.get("change_to_break_even")
            row["underlying_price"] = underlying.get("price")
            row["ticker"] = underlying.get("ticker")  # Underlying ticker

            rows.append(row)

        # Create the initial DataFrame from the list of rows.
        df = pd.DataFrame(rows)
        self.as_dataframe = df

        # ---- Vectorized computations below ----

        # 1. Compute Days-to-Expiration (DTE) by converting the expiry strings to datetime.
        df["expiry_dt"] = pd.to_datetime(df["expiry"], errors="coerce")
        now = pd.Timestamp.now()
        # Calculate difference in days and clip negative values to 0.
        df["dte"] = (df["expiry_dt"] - now).dt.days.clip(lower=0)

        # 2. Set the risk-free rate column.
        df["risk_free_rate"] = self.risk_free_rate

        # 3. Ensure numeric columns (for vectorized math) and create a lowercase version of call_put.
        df["underlying_price"] = pd.to_numeric(df["underlying_price"], errors="coerce")
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        df["midpoint"] = pd.to_numeric(df["midpoint"], errors="coerce")
        df["ask"] = pd.to_numeric(df["ask"], errors="coerce")
        df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df["oi"] = pd.to_numeric(df["oi"], errors="coerce")
        df["call_put_lower"] = df["call_put"].str.lower()

        # 4. Compute intrinsic value vectorized.
        is_call = df["call_put_lower"] == "call"
        intrinsic_call = (df["underlying_price"] - df["strike"]).clip(lower=0)
        intrinsic_put = (df["strike"] - df["underlying_price"]).clip(lower=0)
        df["intrinsic_value"] = np.where(is_call, intrinsic_call, intrinsic_put)

        # 5. Compute extrinsic value as max(midpoint - intrinsic_value, 0).
        df["extrinsic_value"] = (df["midpoint"] - df["intrinsic_value"]).clip(lower=0)

        # 6. Compute spread and spread percentage.
        df["spread"] = df["ask"] - df["bid"]
        df["spread_pct"] = (df["spread"] / df["midpoint"]) * 100

        # 7. Compute premium percentage.
        df["premium_percent"] = (df["midpoint"] / df["underlying_price"]) * 100

        # 8. Compute volume-to-open-interest ratio.
        df["vol_oi_ratio"] = df["volume"] / df["oi"]

        # 9. Compute moneyness using vectorized conditions.
        conditions = [
            (df["call_put_lower"] == "call") & (df["underlying_price"] > df["strike"]),
            (df["call_put_lower"] == "call") & (df["underlying_price"] < df["strike"]),
            (df["call_put_lower"] == "call") & (df["underlying_price"] == df["strike"]),
            (df["call_put_lower"] == "put") & (df["underlying_price"] < df["strike"]),
            (df["call_put_lower"] == "put") & (df["underlying_price"] > df["strike"]),
            (df["call_put_lower"] == "put") & (df["underlying_price"] == df["strike"]),
        ]
        choices = ["itm", "otm", "atm", "itm", "otm", "atm"]
        df["moneyness"] = np.select(conditions, choices, default=None)

        # Remove temporary helper columns if not needed.
        df.drop(columns=["expiry_dt", "call_put_lower"], inplace=True)

        # 10. Calculate IV skew metrics.
        self.calc_iv_skew()

    @staticmethod
    def compute_dte(expiry_str: str) -> Optional[int]:
        """
        Compute Days to Expiration (DTE) from an expiry date string.

        Args:
            expiry_str: Expiration date in ISO format.

        Returns:
            Number of days until expiration (non-negative), or None if parsing fails.
        """
        if not expiry_str:
            return None
        try:
            exp_date = datetime.datetime.fromisoformat(expiry_str)
            now = datetime.datetime.now()
            return max((exp_date - now).days, 0)
        except Exception:
            return None

    @staticmethod
    def compute_intrinsic_value(S: float, K: float, call_put: str) -> Optional[float]:
        """
        Compute the intrinsic value of an option.

        For a call: max(S - K, 0); for a put: max(K - S, 0).

        Args:
            S: Underlying asset price.
            K: Strike price.
            call_put: "call" or "put" (case-insensitive).

        Returns:
            The intrinsic value, or None if inputs are invalid.
        """
        if S is None or K is None or call_put is None:
            return None
        call_put = call_put.lower()
        if call_put == "call":
            return max(S - K, 0)
        elif call_put == "put":
            return max(K - S, 0)
        else:
            return None

    @staticmethod
    def compute_extrinsic_value(midpoint: float, intrinsic: float) -> Optional[float]:
        """
        Compute the extrinsic (time) value of an option.

        Args:
            midpoint: The market midpoint price.
            intrinsic: The computed intrinsic value.

        Returns:
            The extrinsic value (non-negative), or None if inputs are invalid.
        """
        if midpoint is None or intrinsic is None:
            return None
        return max(midpoint - intrinsic, 0)

    @staticmethod
    def compute_moneyness(row: pd.Series) -> Optional[str]:
        """
        Determine the moneyness of an option based on its type, strike, and underlying price.

        Returns:
            'itm' (in-the-money), 'otm' (out-of-the-money), 'atm' (at-the-money), or None.
        """
        call_put = row.get("call_put")
        S = row.get("underlying_price")
        K = row.get("strike")
        if not call_put or S is None or K is None:
            return None
        call_put = call_put.lower()
        if call_put == "call":
            if S > K:
                return "itm"
            elif S < K:
                return "otm"
            else:
                return "atm"
        elif call_put == "put":
            if S < K:
                return "itm"
            elif S > K:
                return "otm"
            else:
                return "atm"
        return None

    def calc_iv_skew(self) -> None:
        """
        Calculate IV skew by identifying the lowest implied volatility strike.

        Sets the following columns in the DataFrame:
            - avg_iv: average implied volatility across valid rows.
            - iv_skew: a label ('call_skew' or 'put_skew') based on whether the lowest-IV strike
              is above or below the underlying price.
        """
        df = self.as_dataframe
        valid = df.dropna(subset=["strike", "iv", "underlying_price"])
        if valid.empty:
            df["iv_skew"] = None
            df["avg_iv"] = None
            return

        avg_iv = valid["iv"].mean()
        df["avg_iv"] = avg_iv

        idxmin = valid["iv"].idxmin()
        lowest_iv_strike = valid.loc[idxmin, "strike"]
        underlying_price = valid.loc[idxmin, "underlying_price"]

        skew_label = "call_skew" if lowest_iv_strike > underlying_price else "put_skew"
        df["iv_skew"] = skew_label
