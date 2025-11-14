import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import imageio
import os
from scipy.stats import linregress

# Clean up frame images if you want
# for f in os.listdir('frames'):
#     os.remove(os.path.join('frames', f))
import hashlib
import random
import string
import time
def generate_webull_headers(access_token:str=None):
    """
    Dynamically generates headers for a Webull request.
    Offsets the current system time by 6 hours (in milliseconds) for 't_time'.
    Creates a randomized 'x-s' value each time.
    Adjust these methods of generation if you have more info on Webull's official approach.
    """
    # Offset by 6 hours
    offset_hours = 6
    offset_millis = offset_hours * 3600 * 1000

    # Current system time in ms
    current_millis = int(time.time() * 1000)
    t_time_value = current_millis - offset_millis

    # Generate a random string to feed into a hash
    random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
    # Create an x-s value (example: SHA256 hash of random_str + t_time_value)
    x_s_value = hashlib.sha256(f"{random_str}{t_time_value}".encode()).hexdigest()

    # Build and return the headers
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "access_token": os.environ.get('access_token'),
        "app": "global",
        "app-group": "broker",
        "appid": "wb_web_app",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "device-type": "Web",
        "did": "3uiar5zgvki16rgnpsfca4kyo4scy00a",
        "dnt": "1",
        "hl": "en",
        "lzone": "dc_core_r001",
        "origin": "https://app.webull.com",
        "os": "web",
        "osv": "i9zh",
        "platform": "web",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://app.webull.com/",
        "reqid": "a62kzwv0irtwnid2n8xmsrg3ohfv6_93",
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "t_time": str(t_time_value),
        "tz": "America/Chicago",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "ver": "5.5.2",
        "x-s": x_s_value,
        "x-sv": "xodp2vg9"
    }

    return headers


import pandas as pd
import numpy as np
from scipy.stats import linregress

def compute_trend(series: pd.Series, threshold: float = 0.001) -> str:
    """
    Compute the trend of a series (assumed to be in descending order, with the most recent value first).
    Instead of using np.polyfit, we use scipy.stats.linregress on the reversed (chronological) data
    to compute the slope, and then classify the trend based on the normalized slope.
    
    Parameters:
        series (pd.Series): A series of numeric values (e.g., band values).
        threshold (float): The minimum relative slope to call the trend increasing/decreasing.
        
    Returns:
        str: "increasing", "decreasing", or "flattening".
    """
    # Reverse the series so that values are in chronological order: oldest first, newest last.
    subset = series.copy().iloc[::-1]
    if len(subset) < 2:
        return "flattening"

    # Create an x-axis based on the length of the subset
    x = np.arange(len(subset))
    try:
        # Use linregress to perform linear regression
        result = linregress(x, subset.values)
        slope = result.slope
    except Exception:
        # If linregress fails, default to "flattening"
        return "flattening"
    
    mean_val = np.mean(subset.values)
    # Calculate relative slope (normalize by the mean of the values)
    relative_slope = slope / mean_val if mean_val != 0 else 0
    
    if relative_slope > threshold:
        return "increasing"
    elif relative_slope < -threshold:
        return "decreasing"
    else:
        return "flattening"


def add_bollinger_bands(
    df: pd.DataFrame,
    window: int = 20,
    num_std: float = 1.9,
    trend_points: int = 13
) -> pd.DataFrame:
    """
    Adds Bollinger bands (middle, upper, lower) to a DataFrame based on the 'c' column (close prices).
    Additionally, computes the trend for the upper and lower bands using the last `trend_points` values.
    
    The computation is performed on the data sorted in ascending (chronological) order,
    then merged back into the original DataFrame. Finally, the DataFrame is re-sorted
    in descending order (newest first), and the computed trends are assigned only to the
    first (newest) row as new columns 'upper_bb_trend' and 'lower_bb_trend'.
    
    For the upper band:
        - "increasing" becomes "upper_increasing"
        - "decreasing" becomes "upper_decreasing"
    For the lower band:
        - "increasing" becomes "lower_increasing"
        - "decreasing" becomes "lower_decreasing"
    If the computed trend is "flattening" for either band, the flag will be "flattening".
    
    Additionally:
      - 'candle_above_upper': True if the candle's high (or close if no 'h' column) exceeds the upper band.
      - 'candle_below_lower': True if the candle's low (or close if no 'l' column) falls below the lower band.
    
    NEW FLAGS:
      - 'candle_completely_above_upper': True if the ENTIRE candle is above the upper band
                                         (i.e., candle low > upper band).
      - 'candle_partially_above_upper':  True if the candle's high is above the upper band
                                         but the low is NOT strictly above it.
      - 'candle_completely_below_lower': True if the ENTIRE candle is below the lower band
                                         (i.e., candle high < lower band).
      - 'candle_partially_below_lower':  True if the candle's low is below the lower band
                                         but the high is NOT strictly below it.
    
    Parameters:
        df (pd.DataFrame): DataFrame with at least columns "ts" (timestamp) and "c" (close price).
                           Optionally, it may include "h" (high) and "l" (low) for full candle data.
        window (int): Window size for rolling mean and std.
        num_std (float): Number of standard deviations for the upper/lower bands.
        trend_points (int): Number of rows to use for computing the trend.
        
    Returns:
        pd.DataFrame: DataFrame with added Bollinger bands, trend columns, and candle flags.
    """

    # Work on a copy sorted in ascending order (oldest first)
    df_sorted = df.copy()
    # Calculate rolling statistics
    df_sorted["middle_band"] = df_sorted["Close"].rolling(window=window, min_periods=window).mean()
    df_sorted["std"] = df_sorted["Close"].rolling(window=window, min_periods=window).std()
    df_sorted["upper_band"] = df_sorted["middle_band"] + (num_std * df_sorted["std"])
    df_sorted["lower_band"] = df_sorted["middle_band"] - (num_std * df_sorted["std"])
    
    # Merge the calculated bands back into the original DataFrame based on timestamp
    df = df.merge(
        df_sorted[["Timestamp", "middle_band", "upper_band", "lower_band"]],
        on="Timestamp",
        how="left"
    )
    
    # Sort descending so that the first row is the most recent
    df = df.sort_values("Timestamp", ascending=False).reset_index(drop=True)
    
    # Initialize trend columns
    df["upper_bb_trend"] = pd.Series([None] * len(df), dtype="object")
    df["lower_bb_trend"] = pd.Series([None] * len(df), dtype="object")
    
    # Only compute trends if there are at least 'trend_points' rows
    if len(df) >= trend_points:
        # Use the most recent `trend_points` rows (already sorted descending)
        subset_upper = df["upper_band"].head(trend_points)
        subset_lower = df["lower_band"].head(trend_points)
        
        # Compute trends
        upper_trend = compute_trend(subset_upper)
        lower_trend = compute_trend(subset_lower)
        
        # Assign prefixed trend values to the newest row (index=0)
        if upper_trend == "increasing":
            df.at[0, "upper_bb_trend"] = "upper_increasing"
        elif upper_trend == "decreasing":
            df.at[0, "upper_bb_trend"] = "upper_decreasing"
        else:
            df.at[0, "upper_bb_trend"] = "flattening"
        
        if lower_trend == "increasing":
            df.at[0, "lower_bb_trend"] = "lower_increasing"
        elif lower_trend == "decreasing":
            df.at[0, "lower_bb_trend"] = "lower_decreasing"
        else:
            df.at[0, "lower_bb_trend"] = "flattening"
    else:
        # Not enough data; default to flattening
        df.at[0, "upper_bb_trend"] = "flattening"
        df.at[0, "lower_bb_trend"] = "flattening"
    
    # If we have 'h' and 'l', use them; else fallback to 'c'
    if {"h", "l"}.issubset(df.columns):
        df["candle_above_upper"] = df["High"] > df["upper_band"]
        df["candle_below_lower"] = df["Low"] < df["lower_band"]
    else:
        df["candle_above_upper"] = df["Close"] > df["upper_band"]
        df["candle_below_lower"] = df["Close"] < df["lower_band"]
    
    # New flags
    df["candle_completely_above_upper"] = False
    df["candle_partially_above_upper"]  = False
    df["candle_completely_below_lower"] = False
    df["candle_partially_below_lower"]  = False
    
    if {"High", "Low"}.issubset(df.columns):
        # COMPLETELY ABOVE
        df.loc[df["Low"] > df["upper_band"], "candle_completely_above_upper"] = True
        
        # PARTIALLY ABOVE
        df.loc[
            (df["High"] > df["upper_band"]) & (df["Low"] <= df["upper_band"]),
            "candle_partially_above_upper"
        ] = True
        
        # COMPLETELY BELOW
        df.loc[df["High"] < df["lower_band"], "candle_completely_below_lower"] = True
        
        # PARTIALLY BELOW
        df.loc[
            (df["Low"] < df["lower_band"]) & (df["High"] >= df["lower_band"]),
            "candle_partially_below_lower"
        ] = True
    else:
        # Fallback for data lacking 'h'/'l' columns (only 'c' is known)
        df.loc[df["Close"] > df["upper_band"], "candle_completely_above_upper"] = True
        df.loc[df["Close"] < df["lower_band"], "candle_completely_below_lower"] = True
        # partial flags remain False if we only have a single price
    
    return df



