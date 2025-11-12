import time
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.y_finance.yf_sdk import YfSDK
opts = PolygonOptions()
yf = YfSDK()

from fudstop4.apis.occ.occ_sdk import occSDK

occ = occSDK()


import asyncio

import pandas as pd

from fudstop4.apis.occ.occ_sdk import occSDK
from fudstop4.apis.webull.webull_ta import WebullTA
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.webull.webull_trading import WebullTrading
from helpers import generate_webull_headers
opts = PolygonOptions()
ta = WebullTA()
occ = occSDK()
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
import numpy as np
trading = WebullTrading()
from numba import njit


import asyncio
import pandas as pd


# ─── NUMBA-OPTIMIZED FUNCTIONS ───────────────────────────────────────────────
@njit
def compute_wilders_rsi_numba(closes: np.ndarray, window: int) -> np.ndarray:
    """
    Compute Wilder's RSI using Numba. The first `window` values are set to NaN.
    """
    n = len(closes)
    rsi = np.empty(n, dtype=np.float64)
    for i in range(window):
        rsi[i] = np.nan

    # Calculate price changes.
    changes = np.empty(n, dtype=np.float64)
    changes[0] = 0.0
    for i in range(1, n):
        changes[i] = closes[i] - closes[i - 1]

    # Calculate gains and losses.
    gains = np.empty(n, dtype=np.float64)
    losses = np.empty(n, dtype=np.float64)
    for i in range(n):
        if changes[i] > 0:
            gains[i] = changes[i]
            losses[i] = 0.0
        else:
            gains[i] = 0.0
            losses[i] = -changes[i]

    # First average gain and loss.
    sum_gain = 0.0
    sum_loss = 0.0
    for i in range(window):
        sum_gain += gains[i]
        sum_loss += losses[i]
    avg_gain = sum_gain / window
    avg_loss = sum_loss / window

    if avg_loss == 0:
        rsi[window] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi[window] = 100.0 - (100.0 / (1.0 + rs))

    # Wilder's smoothing for the rest.
    for i in range(window + 1, n):
        avg_gain = ((avg_gain * (window - 1)) + gains[i]) / window
        avg_loss = ((avg_loss * (window - 1)) + losses[i]) / window
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def compute_wilders_rsi(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """
    Computes Wilder's RSI on the 'c' (close) column of df.
    """
    if len(df) < window:
        df['rsi'] = np.nan
        return df
    closes = df['c'].to_numpy(dtype=np.float64)
    rsi_values = compute_wilders_rsi_numba(closes, window)
    df['rsi'] = rsi_values
    return df

@njit
def ema_njit(prices: np.ndarray, period: int) -> np.ndarray:
    """
    Calculate the Exponential Moving Average (EMA) for a given period.
    """
    multiplier = 2.0 / (period + 1)
    ema = np.empty(len(prices), dtype=np.float64)
    ema[0] = prices[0]
    for i in range(1, len(prices)):
        ema[i] = (prices[i] - ema[i - 1]) * multiplier + ema[i - 1]
    return ema





@njit
def determine_macd_curvature_code(prices: np.ndarray) -> int:
    """
    Determine the MACD histogram curvature using refined momentum logic.
    
    Parameters
    ----------
    prices : np.ndarray
        Array of price data (e.g., daily close prices).
    
    Returns
    -------
    int
        An integer code representing the curvature momentum:
        
        0: insufficient data
        1: diverging bull (histogram > 0, strongly increasing)
        2: diverging bear (histogram < 0, strongly decreasing)
        3: arching bull (histogram > 0, but momentum rolling over)
        4: arching bear (histogram < 0, but momentum rolling up)
        5: converging bull (histogram > 0, moderate slope ~ zero)
        6: converging bear (histogram < 0, moderate slope ~ zero)
        7: imminent bullish cross (hist near zero, small slope, below zero => about to cross up)
        8: imminent bearish cross (hist near zero, small slope, above zero => about to cross down)
    
    Enhanced Logic Explanation
    --------------------------
    1) We need at least 4 points in the histogram to detect momentum (first derivative
    ~ slope, second derivative ~ change in slope).
    2) We compute a dynamic threshold based on recent histogram volatility (avg of absolute diffs).
    3) We check near-zero conditions and near-zero slope for "imminent cross".
    4) We check the sign of the latest histogram, the slope from the last 2-3 bars, and second derivative
    to see if it's diverging or arching.
    """
    hist = compute_macd_histogram(prices)
    n = len(hist)
    
    # Need at least 4 data points to do a basic second derivative approach.
    if n < 4:
        return 0  # insufficient data
    
    # Last four points (older -> newer)
    h1, h2, h3, h4 = hist[n - 4], hist[n - 3], hist[n - 2], hist[n - 1]
    
    # First derivative approximations
    d1 = h2 - h1
    d2 = h3 - h2
    d3 = h4 - h3
    
    # Second derivative approximations (changes in slope)
    sd1 = d2 - d1  # how the slope changed from the first gap to the second
    sd2 = d3 - d2  # how the slope changed from the second gap to the third
    
    # Basic slope measure: average of last few differences
    slope = (d2 + d3) / 2.0
    
    # Compute a dynamic threshold based on recent histogram volatility
    # We'll look at the absolute differences h2-h1, h3-h2, h4-h3, etc.
    # This helps us define "strong" vs. "mild" changes adaptively.
    recent_diffs = np.array([abs(d1), abs(d2), abs(d3)])
    avg_hist_vol = np.mean(recent_diffs) + 1e-9  # add small epsilon to avoid /0
    
    # Let's define a "strong slope" if slope magnitude is above 0.75 * avg_hist_vol
    strong_slope_thresh = 0.75 * avg_hist_vol
    
    # We define "near zero" for the histogram and slope
    # You can tweak these to suit your data scale.
    near_zero_hist = 0.1 * avg_hist_vol   # e.g., 10% of avg volatility
    near_zero_slope = 0.1 * avg_hist_vol  # slope threshold near zero
    
    # Check for near-zero histogram and slope => potential cross
    if abs(h4) < near_zero_hist and abs(d3) < near_zero_slope:
        # We examine the average sign of the last 3 or 4 histogram points
        # to guess if it's crossing up or down.
        avg_recent_hist = (h1 + h2 + h3 + h4) / 4.0
        if avg_recent_hist < 0:
            return 7  # imminent bullish cross
        else:
            return 8  # imminent bearish cross
    
    # Not near-zero => check sign of latest histogram
    if h4 > 0:
        # BULLISH SIDE
        if slope > strong_slope_thresh:
            # strongly positive slope => diverging bull
            return 1
        elif slope < -strong_slope_thresh:
            # slope is strongly negative => arching bull
            return 3
        else:
            # slope is moderate => call it converging bull
            return 5
    else:
        # BEARISH SIDE
        if slope < -strong_slope_thresh:
            # strongly negative slope => diverging bear
            return 2
        elif slope > strong_slope_thresh:
            # slope strongly positive => arching bear
            return 4
        else:
            # slope is moderate => converging bear
            return 6


@njit
def compute_macd_histogram(prices: np.ndarray) -> np.ndarray:
    """
    Compute the MACD histogram from closing prices using EMA periods of 12, 26, and 9.
    """
    fast = ema_njit(prices, 12)
    slow = ema_njit(prices, 26)
    macd_line = fast - slow
    signal = ema_njit(macd_line, 9)
    hist = macd_line - signal
    return hist


def macd_curvature_label(prices: np.ndarray) -> str:
    """
    Returns a descriptive label for the MACD curvature.
    """
    code = determine_macd_curvature_code(prices)
    mapping = {
        0: "insufficient data",
        1: "diverging bull",
        2: "diverging bear",
        3: "arching bull",
        4: "arching bear",
        5: "converging bull",
        6: "converging bear",
        7: "imminent bullish cross",
        8: "imminent bearish cross"
    }
    return mapping.get(code, "unknown")

from typing import Tuple
# ─── UPDATED TD SEQUENTIAL LOGIC ─────────────────────────────────────────────
@njit
def compute_td9_counts(closes: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute 'TD setup' counts for buy and sell but allow the counts to run 
    beyond 9 bars as long as the condition remains intact.

    RULES:
    1. Only one setup at a time (buy OR sell). 
        If buy_count > 0, we do not update sell_count (and vice versa).
    2. If buy_count > 0, keep incrementing if c[i] < c[i-4]. 
        If it fails, reset buy_count to 0, 
        then check if c[i] > c[i-4] to start a new sell_count = 1 on the same bar.
    3. If sell_count > 0, keep incrementing if c[i] > c[i-4].
        If it fails, reset sell_count to 0,
        then check if c[i] < c[i-4] to start a new buy_count = 1 on the same bar.
    4. If both buy_count and sell_count are 0, see if we can start one:
        - If c[i] < c[i-4], buy_count = 1
        - Else if c[i] > c[i-4], sell_count = 1
    """
    n = len(closes)
    td_buy = np.zeros(n, dtype=np.int32)
    td_sell = np.zeros(n, dtype=np.int32)

    buy_count = 0
    sell_count = 0

    for i in range(n):
        if i < 4:
            td_buy[i] = buy_count
            td_sell[i] = sell_count
            continue

        if buy_count > 0:
            # Already in a BUY setup
            if closes[i] < closes[i - 4]:
                buy_count += 1
            else:
                # Broke buy condition, reset
                buy_count = 0
                # Attempt to start SELL
                if closes[i] > closes[i - 4]:
                    sell_count = 1
                else:
                    sell_count = 0

        elif sell_count > 0:
            # Already in a SELL setup
            if closes[i] > closes[i - 4]:
                sell_count += 1
            else:
                # Broke sell condition, reset
                sell_count = 0
                # Attempt to start BUY
                if closes[i] < closes[i - 4]:
                    buy_count = 1
                else:
                    buy_count = 0
        else:
            # Not in an active setup
            if closes[i] < closes[i - 4]:
                buy_count = 1
            elif closes[i] > closes[i - 4]:
                sell_count = 1

        td_buy[i] = buy_count
        td_sell[i] = sell_count

    return td_buy, td_sell


def add_td9_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds two columns to df:
    - td_buy_count: increments if c[i] < c[i-4] 
    - td_sell_count: increments if c[i] > c[i-4]
    Allows extended sequences beyond 9 as long as 
    the condition is not broken.
    """
    df = df.copy()
    # Sort ascending for correct sequential logic
    df.sort_values("ts", inplace=True)
    df.reset_index(drop=True, inplace=True)

    closes = df['c'].to_numpy()
    td_buy, td_sell = compute_td9_counts(closes)

    df['td_buy_count'] = td_buy
    df['td_sell_count'] = td_sell
    return df








def did_price_reverse_immediately(df: pd.DataFrame, 
                                  signal_col: str = 'candle_completely_below_lower', 
                                  close_col: str = 'c') -> pd.Series:
    """
    Checks for each row where `signal_col` is True if the next candle's close is higher
    than the current candle's close. Returns a pd.Series (boolean) aligned with df's index.

    Parameters:
    -----------
    df : pd.DataFrame
        Candlestick data containing close prices and a boolean signal column.

    signal_col : str
        Column name indicating signal rows (e.g. 'candle_completely_below_lower').

    close_col : str
        Column name for the close price in df.

    Returns:
    --------
    pd.Series
        A boolean series indicating whether an immediate price reversal happened
        for each row in df.
    """

    # Create a copy of the close prices so we can shift
    close_prices = df[close_col].copy()

    # Shift close prices up by 1, so next_candle_close[i] refers to the close of the next row
    next_candle_close = close_prices.shift(-1)

    # We only care about rows where 'signal_col' is True
    # Condition for 'immediate reversal': next candle close > current candle close
    immediate_reversal = (df[signal_col]) & (next_candle_close > close_prices)

    # Convert NA (last row shift) to False if needed
    immediate_reversal = immediate_reversal.fillna(False)

    return immediate_reversal

# -----------------------------------------------------------------------------
# Main status function
# -----------------------------------------------------------------------------
import asyncio
import itertools
import numpy as np
import pandas as pd

# from your_module import ta, generate_webull_headers, compute_wilders_rsi, add_td9_counts, macd_curvature_label
# from your_database_module import opts

# Example ticker list


# -----------------------------------------------------------------------------
# Helper Function: immediate reversal (unchanged example)
# -----------------------------------------------------------------------------
def did_price_reverse_immediately(df: pd.DataFrame, 
                                  signal_col: str = 'candle_completely_below_lower', 
                                  close_col: str = 'c') -> pd.Series:
    """
    Checks for each row where `signal_col` is True if the next candle's close
    is higher than the current candle's close. Returns a pd.Series (boolean).
    """

    close_prices = df[close_col].copy().to_numpy()
    # next candle's close
    next_close = np.roll(close_prices, -1)  # shift array left by 1
    next_close[-1] = np.nan  # last row has no "next candle"

    # Condition:
    #  1) The signal_col is True
    #  2) The next candle's close > current candle's close
    mask_signal = df[signal_col].to_numpy(bool)
    mask_immediate = next_close > close_prices

    # Combine, handle NaN in the last row
    immediate_reversal = mask_signal & np.where(np.isnan(next_close), False, mask_immediate)

    return pd.Series(immediate_reversal, index=df.index, name='price_reversed_immediately')


# -----------------------------------------------------------------------------
# Helper Function: sharp reversal
# -----------------------------------------------------------------------------
def did_price_reverse_sharply(
    df: pd.DataFrame, 
    signal_col: str = 'candle_completely_below_lower',
    close_col: str = 'c', 
    threshold_pct: float = 0.02,
    look_ahead: int = 3
) -> pd.Series:
    """
    Determines if there's a "sharp" reversal within the next `look_ahead` candles.
    A "sharp" reversal is defined here as at least a `threshold_pct` increase in
    close price from the current row's close.

    Parameters
    ----------
    df : pd.DataFrame
        Candlestick data containing a boolean column (`signal_col`) and a close column.
    signal_col : str
        Column name indicating signal rows (e.g. 'candle_completely_below_lower').
    close_col : str
        Column name for the close price in `df`.
    threshold_pct : float
        The required percentage jump to consider it a "sharp" reversal.
        e.g. 0.02 means 2%.
    look_ahead : int
        How many subsequent candles to examine for a sharp reversal.

    Returns
    -------
    pd.Series
        A boolean series indicating whether a sharp reversal occurred for each row.
        If `signal_col` is False on a row, the output will be False on that row.
    """

    close_prices = df[close_col].to_numpy()

    # We'll create an array to hold the maximum close in the next N bars
    max_close_next_n = [None] * len(df)
    for i in range(len(close_prices)):
        end_idx = min(i + look_ahead + 1, len(close_prices))
        window = close_prices[i+1:end_idx]
        max_close_next_n[i] = max(window) if len(window) > 0 else None

    max_close_next_n = np.array(max_close_next_n, dtype=np.float64)

    # Condition: row is a signal AND next N closes contain at least threshold_pct jump
    signal_mask = df[signal_col].to_numpy(bool)
    required_price = close_prices * (1.0 + threshold_pct)

    # If the row doesn't have the signal, automatically False
    # If max_close_next_n is NaN, treat it as no reversal
    sharp_reversal_mask = signal_mask & np.where(
        np.isnan(max_close_next_n),
        False,
        (max_close_next_n >= required_price)
    )

    return pd.Series(sharp_reversal_mask, index=df.index, name='sharp_reversal')



async def main(ticker):
    try:
        intervals=  ['m1', 'm5', 'm15', 'm30', 'm60', 'm120', 'm240', 'd', 'w', 'm']
        tasks = [ta.get_candle_data(ticker, interval=i, headers=generate_webull_headers()) for i in intervals]
        results = await asyncio.gather(*tasks)

        

        candles = [i.rename(columns={'Close': 'c', 'High': 'h', 'Open': 'o', 'Close': 'c', 'Volume': 'v', 'Vwap': 'vwap', 'Timestamp': 'ts'}) for i in results]
        candles= [i.drop(columns=['Avg']) for i in candles]
        min_candles, _5min_candles, _10min_candles, _15min_candles, _30min_candles, _1h_candles, _2h_candles, _4h_candles, day_candles, week_candles, month_candles = compute_wilders_rsi(candles)
        bbands = ta.add_bollinger_bands(rsi)

        rsi = bbands['rsi'].to_list()[0]
        candle_completely_below_lower, candle_completely_above_upper = bbands['candle_completely_below_lower'].to_list()[0], bbands['candle_completely_above_upper'].to_list()[0]

        fastinfo = await yf.fast_info(ticker)
        print(fastinfo.columns)
        x = await occ.stock_info(ticker)
        fifty_day_avg, day_low, day_high, two_hundred_day_average = fastinfo['fifty_day_average'].to_list()[0], fastinfo['day_low'].to_list()[0], fastinfo['day_high'].to_list()[0], fastinfo['two_hundred_day_average'].to_list()[0]

        beta10,beta20,beta30,beta60,beta90,beta120 = x.beta10D,x.beta20D,x.beta30D,x.beta60D, x.beta90D, x.beta120D
        corr10,corr20,corr30,corr60,corr90,corr120 = x.corr10D, x.corr20D, x.corr30D, x.corr60D, x.corr90D, x.corr120D
        ivp30,ivp60,ivp90 = x.ivp30, x.ivp60, x.ivp90
        ivr30, ivr60, ivr90, ivr120 = x.ivr30, x.ivr60, x.ivr90, x.ivr120
        hvp10, hvp20, hvp30, hvp60, hvp90, hvp120 = x.hvp10, x.hvp20, x.hvp30, x.hvp60, x.hvp90, x.hvp120
        hv10, hv20, hv30, hv60, hv90, hv210 = x.hv10, x.hv20, x.hv30, x.hv60, x.hv90, x.hv120
        ivRank = x.ivRank
        fifty_high = x.highPrice52Wk
        fifty_low = x.lowPrice52Wk
        sentiment = x.sentiment
        volatile_rank = x.volatileRank
        optvol = x.optVol
        callvol = x.callVol
        putvol = x.putVol
        oi = x.openInterest
        avg1movol = x.avgOptVol1MO
        avg1mooi = x.avgOptOI1MO

        insider = await trading.insider_list(symbol=ticker)

        date= insider.transaction_date[0]
        shares = insider.shares[0]

        inst = await yf.major_holders(ticker)

        metric = inst['metric'].to_list()[0]

        stats = await trading.multi_quote([ticker])
 
        ticker_news = await trading.ai_news(ticker, page_size=1, headers=generate_webull_headers())

        




        data = {
            "beta10": beta10,
            "beta20": beta20,
            "beta30": beta30,
            "beta60": beta60,
            "beta90": beta90,
            "beta120": beta120,
            "corr10": corr10,
            "corr20": corr20,
            "corr30": corr30,
            "corr60": corr60,
            "corr90": corr90,
            "corr120": corr120,
            "ivp30": ivp30,
            "ivp60": ivp60,
            "ivp90": ivp90,
            "ivr30": ivr30,
            "ivr60": ivr60,
            "ivr90": ivr90,
            "ivr120": ivr120,
            "hvp10": hvp10,
            "hvp20": hvp20,
            "hvp30": hvp30,
            "hvp60": hvp60,
            "hvp90": hvp90,
            "hvp120": hvp120,
            "hv10": hv10,
            "hv20": hv20,
            "hv30": hv30,
            "hv60": hv60,
            "hv90": hv90,
            "hv210": hv210,
            "ivRank": ivRank,
            "fifty_high": fifty_high,
            "fifty_low": fifty_low,
            "sentiment": sentiment,
            "volatile_rank": volatile_rank,
            "optvol": optvol,
            "callvol": callvol,
            "putvol": putvol,
            "oi": oi,
            "avg1movol": avg1movol,
            "avg1mooi": avg1mooi,
            '_200avg': two_hundred_day_average,
            '_50avg': fifty_day_avg,
            'day_low': day_low,
            'day_high': day_high,
            'bband_put_signal': candle_completely_above_upper,
            'bband_call_signal': candle_completely_below_lower,
            'rsi': rsi,
            'recent_insider_shares': shares,
            'recent_insider_date': date,
            'insider_owned': metric,
            'open': stats.open[0],
            'high': stats.high[0],
            'low': stats.low[0],
            'close': stats.close[0],
            'volume': stats.volume[0],
            'avg_stockvol10d': stats.avgVol10D[0],
            'avg_stockvol3m': stats.avgVol3M[0],
            'vibration': float(stats.vibrateRatio[0]),
            'market_value': float(stats.marketValue[0]),
            'news_sentiment': ticker_news.sentiment[0],
            'news': ticker_news.summary[0]

        }


        df = pd.DataFrame(data, index=[0])
        df['ticker'] = ticker

        await opts.batch_upsert_dataframe(df, table_name='prob', unique_columns=['ticker'])
    except Exception as e:
        print(e)

async def run_main():
    await opts.connect()
    chunk_size = 5
    # Process tickers in chunks of 5
    for i in range(0, len(most_active_tickers), chunk_size):
        current_chunk = most_active_tickers[i:i+chunk_size]
        # Create a list of tasks for the current chunk
        tasks = [main(ticker) for ticker in current_chunk]
        # Run tasks concurrently and wait for all to complete in the current chunk
        results = await asyncio.gather(*tasks)
        # Optionally, you can process the results of this chunk here
        print(f"Completed chunk {i//chunk_size + 1}: {results}")

# Start the event loop and run the tasks
asyncio.run(run_main())