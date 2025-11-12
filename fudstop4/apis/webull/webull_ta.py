import re
import pandas as pd
import asyncio
import time

import httpx
import numpy as np
import numpy as np
from numba import njit
import pandas as pd
import sys
from pathlib import Path
import math
from datetime import time
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from scipy.stats import linregress
import aiohttp
import logging
from asyncio import Semaphore, Lock
from typing import Dict,Tuple
import asyncio
import hashlib
import random
import string
import time

class WebullTA:
    def __init__(self):
        self.cycle_indicators = {
        "HT_DCPERIOD": {
            "description": "Hilbert Transform - Dominant Cycle Period. Measures the dominant cycle period in the price series.",
            "ideal_scan": "Look for stable or increasing cycle periods to identify trend continuation or weakening."
        },
        "HT_DCPHASE": {
            "description": "Hilbert Transform - Dominant Cycle Phase. Represents the phase of the dominant cycle.",
            "ideal_scan": "Identify phase changes for potential reversals or trend accelerations."
        },
        "HT_PHASOR": {
            "description": "Hilbert Transform - Phasor Components. Provides complex components (real and imaginary) of the phasor.",
            "ideal_scan": "Use changes in real or imaginary components to detect shifts in price momentum or trend."
        },
        "HT_SINE": {
            "description": "Hilbert Transform - SineWave. Produces sine and lead sine wave values for trend identification.",
            "ideal_scan": "Crossovers between sine and lead sine waves can signal potential trend changes."
        },
        "HT_TRENDMODE": {
            "description": "Hilbert Transform - Trend vs. Cycle Mode. Identifies if the market is in a trending or cyclic mode.",
            "ideal_scan": "HT_TRENDMODE = 1 for trending conditions; HT_TRENDMODE = 0 for cyclic conditions."
        },
    },
        
        self.pattern_recognition_indicators = {
        # Pattern Recognition Indicators
        "CDL2CROWS": {
            "description": "Two Crows - A bearish reversal pattern that occurs after an uptrend.",
            "ideal_scan": "Look for Two Crows at resistance levels or after a strong uptrend."
        },
        "CDL3BLACKCROWS": {
            "description": "Three Black Crows - A bearish reversal pattern with three consecutive long bearish candles.",
            "ideal_scan": "Appears after an uptrend; confirms bearish momentum."
        },
        "CDL3INSIDE": {
            "description": "Three Inside Up/Down - A candlestick pattern indicating potential reversal.",
            "ideal_scan": "Three Inside Up for bullish reversals; Three Inside Down for bearish reversals."
        },
        "CDL3LINESTRIKE": {
            "description": "Three-Line Strike - A potential continuation pattern after a trend.",
            "ideal_scan": "Look for confirmation with volume or other trend indicators."
        },
        "CDL3OUTSIDE": {
            "description": "Three Outside Up/Down - Indicates reversal of the current trend.",
            "ideal_scan": "Three Outside Up after a downtrend; Three Outside Down after an uptrend."
        },
        "CDL3STARSINSOUTH": {
            "description": "Three Stars In The South - A rare bullish reversal pattern.",
            "ideal_scan": "Forms in a downtrend; confirms reversal when paired with increasing volume."
        },
        "CDL3WHITESOLDIERS": {
            "description": "Three Advancing White Soldiers - A strong bullish reversal pattern.",
            "ideal_scan": "Look for this after a downtrend; confirms bullish momentum."
        },
        "CDLABANDONEDBABY": {
            "description": "Abandoned Baby - A reversal pattern with a gap on both sides of a doji.",
            "ideal_scan": "Bullish after a downtrend; bearish after an uptrend."
        },
        "CDLADVANCEBLOCK": {
            "description": "Advance Block - A bearish reversal pattern with three candles showing weakening momentum.",
            "ideal_scan": "Occurs in an uptrend; look for weakening volume."
        },
        "CDLBELTHOLD": {
            "description": "Belt-hold - A single candlestick pattern indicating reversal or continuation.",
            "ideal_scan": "Bullish at support levels; bearish at resistance levels."
        },
        "CDLBREAKAWAY": {
            "description": "Breakaway - A five-candle reversal pattern.",
            "ideal_scan": "Look for bullish Breakaway in a downtrend; bearish in an uptrend."
        },
        "CDLCLOSINGMARUBOZU": {
            "description": "Closing Marubozu - A candlestick with no shadow on the closing side.",
            "ideal_scan": "Bullish when the close is the high; bearish when the close is the low."
        },
        "CDLCONCEALBABYSWALL": {
            "description": "Concealing Baby Swallow - A bullish reversal pattern formed by four candles.",
            "ideal_scan": "Forms in a downtrend; confirms reversal with increasing volume."
        },
        "CDLCOUNTERATTACK": {
            "description": "Counterattack - A reversal pattern with a strong opposing candle.",
            "ideal_scan": "Bullish at support; bearish at resistance."
        },
        "CDLDARKCLOUDCOVER": {
            "description": "Dark Cloud Cover - A bearish reversal pattern with a strong bearish candle.",
            "ideal_scan": "Occurs at the top of an uptrend; confirms with increased volume."
        },
        "CDLDOJI": {
            "description": "Doji - Indicates indecision in the market.",
            "ideal_scan": "Look for Doji near support or resistance levels to signal potential reversals."
        },
        "CDLDOJISTAR": {
            "description": "Doji Star - A potential reversal pattern with a doji after a trend candle.",
            "ideal_scan": "Bullish after a downtrend; bearish after an uptrend."
        },
        "CDLDRAGONFLYDOJI": {
            "description": "Dragonfly Doji - A bullish reversal pattern with a long lower shadow.",
            "ideal_scan": "Occurs in a downtrend; confirms reversal with higher volume."
        },
        "CDLENGULFING": {
            "description": "Engulfing Pattern - A strong reversal pattern with a larger candle engulfing the previous one.",
            "ideal_scan": "Bullish after a downtrend; bearish after an uptrend."
        },
        "CDLEVENINGDOJISTAR": {
            "description": "Evening Doji Star - A bearish reversal pattern with a doji star.",
            "ideal_scan": "Occurs at the top of an uptrend; confirms with increased volume."
        },
        "CDLEVENINGSTAR": {
            "description": "Evening Star - A bearish reversal pattern.",
            "ideal_scan": "Forms at resistance; confirms bearish reversal."
        },
        "CDLGAPSIDESIDEWHITE": {
            "description": "Up/Down-gap side-by-side white lines - A continuation pattern.",
            "ideal_scan": "Look for confirmation with other trend indicators."
        },
        "CDLGRAVESTONEDOJI": {
            "description": "Gravestone Doji - A bearish reversal pattern with a long upper shadow.",
            "ideal_scan": "Occurs in an uptrend; confirms reversal with high volume."
        },
        "CDLHAMMER": {
            "description": "Hammer - A bullish reversal pattern with a long lower shadow.",
            "ideal_scan": "Appears in a downtrend; confirms reversal with strong volume."
        },
        "CDLHANGINGMAN": {
            "description": "Hanging Man - A bearish reversal pattern with a long lower shadow.",
            "ideal_scan": "Occurs in an uptrend; look for confirmation with volume."
        },
        "CDLHARAMI": {
            "description": "Harami Pattern - A two-candle reversal pattern.",
            "ideal_scan": "Bullish Harami in a downtrend; bearish Harami in an uptrend."
        },
        "CDLHARAMICROSS": {
            "description": "Harami Cross Pattern - A Harami pattern with a doji as the second candle.",
            "ideal_scan": "Stronger reversal signal compared to the standard Harami."
        },
        "CDLHIGHWAVE": {
            "description": "High-Wave Candle - Indicates market indecision.",
            "ideal_scan": "Look for High-Wave candles near key support or resistance levels."
        },
        "CDLHIKKAKE": {
            "description": "Hikkake Pattern - A trap pattern indicating reversal or continuation.",
            "ideal_scan": "Look for false breakout followed by a strong move in the opposite direction."
        },
        "CDLHIKKAKEMOD": {
            "description": "Modified Hikkake Pattern - A variation of the Hikkake pattern.",
            "ideal_scan": "Scan for similar setups as standard Hikkake but with adjusted conditions."
        },
        "CDLHOMINGPIGEON": {
            "description": "Homing Pigeon - A bullish reversal pattern with two candles.",
            "ideal_scan": "Forms in a downtrend; confirms reversal with higher volume."
        },
        "CDLIDENTICAL3CROWS": {
            "description": "Identical Three Crows - A bearish reversal pattern with three identical bearish candles.",
            "ideal_scan": "Appears at the top of an uptrend; confirms bearish continuation."
        },
        "CDLINNECK": {
            "description": "In-Neck Pattern - A bearish continuation pattern.",
            "ideal_scan": "Occurs in a downtrend; confirms bearish momentum."
        },
        "CDLINVERTEDHAMMER": {
            "description": "Inverted Hammer - A bullish reversal pattern with a long upper shadow.",
            "ideal_scan": "Occurs in a downtrend; confirms with higher volume."
        },
        "CDLPIERCING": {
            "description": "Piercing Pattern - A bullish reversal pattern with a strong upward move.",
            "ideal_scan": "Occurs in a downtrend; confirms with increasing volume."
        },
    "CDLKICKING": {
        "description": "Kicking - A strong reversal pattern characterized by a gap between two opposite-colored marubozu candles.",
        "ideal_scan": "Bullish kicking in a downtrend; bearish kicking in an uptrend."
    },
    "CDLKICKINGBYLENGTH": {
        "description": "Kicking by Length - Similar to Kicking but determined by the length of the marubozu.",
        "ideal_scan": "Scan for longer marubozu candles to confirm stronger signals."
    },
    "CDLLADDERBOTTOM": {
        "description": "Ladder Bottom - A bullish reversal pattern that occurs after a downtrend.",
        "ideal_scan": "Look for increasing volume on confirmation."
    },
    "CDLLONGLEGGEDDOJI": {
        "description": "Long-Legged Doji - Indicates market indecision with long upper and lower shadows.",
        "ideal_scan": "Appears near support or resistance; confirms potential reversal."
    },
    "CDLLONGLINE": {
        "description": "Long Line Candle - A single candlestick with a long body, indicating strong momentum.",
        "ideal_scan": "Bullish long lines near support; bearish near resistance."
    },
    "CDLMARUBOZU": {
        "description": "Marubozu - A candlestick with no shadows, indicating strong directional momentum.",
        "ideal_scan": "Bullish marubozu in uptrend; bearish marubozu in downtrend."
    },
    "CDLMATCHINGLOW": {
        "description": "Matching Low - A bullish reversal pattern with two candles having the same low.",
        "ideal_scan": "Occurs in a downtrend; confirms reversal with increased volume."
    },
    "CDLMATHOLD": {
        "description": "Mat Hold - A continuation pattern that indicates strong trend persistence.",
        "ideal_scan": "Bullish Mat Hold in an uptrend; bearish Mat Hold in a downtrend."
    },
    "CDLMORNINGDOJISTAR": {
        "description": "Morning Doji Star - A bullish reversal pattern with a doji and gap.",
        "ideal_scan": "Appears in a downtrend; confirms reversal with strong upward move."
    },
    "CDLMORNINGSTAR": {
        "description": "Morning Star - A bullish reversal pattern with three candles.",
        "ideal_scan": "Occurs in a downtrend; confirms with increasing volume."
    },
    "CDLONNECK": {
        "description": "On-Neck Pattern - A bearish continuation pattern.",
        "ideal_scan": "Occurs in a downtrend; confirms bearish momentum."
    },
    "CDLPIERCING": {
        "description": "Piercing Pattern - A bullish reversal pattern with a strong upward move.",
        "ideal_scan": "Appears in a downtrend; confirms with increasing volume."
    },
    "CDLRICKSHAWMAN": {
        "description": "Rickshaw Man - A variation of the Doji with long upper and lower shadows.",
        "ideal_scan": "Indicates indecision; look for context near support or resistance."
    },
    "CDLRISEFALL3METHODS": {
        "description": "Rising/Falling Three Methods - A continuation pattern with small corrective candles.",
        "ideal_scan": "Bullish in uptrend; bearish in downtrend with trend resumption confirmation."
    },
    "CDLSEPARATINGLINES": {
        "description": "Separating Lines - A continuation pattern with two strong candles.",
        "ideal_scan": "Bullish in an uptrend; bearish in a downtrend."
    },
    "CDLSHOOTINGSTAR": {
        "description": "Shooting Star - A bearish reversal pattern with a long upper shadow.",
        "ideal_scan": "Occurs in an uptrend; confirms reversal with strong bearish move."
    },
    "CDLSHORTLINE": {
        "description": "Short Line Candle - A candlestick with a short body, indicating low momentum.",
        "ideal_scan": "Look for context within larger patterns for confirmation."
    },
    "CDLSPINNINGTOP": {
        "description": "Spinning Top - A candlestick with small real body and long shadows.",
        "ideal_scan": "Indicates indecision; watch for breakouts in the direction of the trend."
    },
    "CDLSTALLEDPATTERN": {
        "description": "Stalled Pattern - A bearish reversal pattern in an uptrend.",
        "ideal_scan": "Appears near resistance; confirms reversal with volume."
    },
    "CDLSTICKSANDWICH": {
        "description": "Stick Sandwich - A bullish reversal pattern with two bearish candles sandwiching a bullish one.",
        "ideal_scan": "Occurs in a downtrend; confirms reversal when price breaks higher."
    },
    "CDLTAKURI": {
        "description": "Takuri - A Dragonfly Doji with an exceptionally long lower shadow.",
        "ideal_scan": "Occurs in a downtrend; confirms reversal with strong upward move."
    },
    "CDLTASUKIGAP": {
        "description": "Tasuki Gap - A continuation pattern with a gap and corrective candle.",
        "ideal_scan": "Bullish in uptrend; bearish in downtrend with gap hold confirmation."
    },
    "CDLTHRUSTING": {
        "description": "Thrusting Pattern - A bearish continuation pattern with partial gap filling.",
        "ideal_scan": "Occurs in a downtrend; confirms bearish continuation."
    },
    "CDLTRISTAR": {
        "description": "Tristar Pattern - A reversal pattern with three doji candles.",
        "ideal_scan": "Bullish Tristar in a downtrend; bearish Tristar in an uptrend."
    },
    "CDLUNIQUE3RIVER": {
        "description": "Unique 3 River - A rare bullish reversal pattern.",
        "ideal_scan": "Forms in a downtrend; confirms with a strong upward move."
    },
    "CDLUPSIDEGAP2CROWS": {
        "description": "Upside Gap Two Crows - A bearish reversal pattern with a gap and two bearish candles.",
        "ideal_scan": "Occurs in an uptrend; confirms bearish reversal."
    },
    "CDLXSIDEGAP3METHODS": {
        "description": "Upside/Downside Gap Three Methods - A continuation pattern with a gap and corrective candles.",
        "ideal_scan": "Bullish in uptrend; bearish in downtrend with confirmation of resumption."
    }}

        self.math_transform_indicators = {
        "ACOS": {
            "description": "Vector Trigonometric ACos - Calculates the arccosine of a vector's values.",
            "ideal_use": "Used in computations requiring the inverse cosine of an angle or value."
        },
        "ASIN": {
            "description": "Vector Trigonometric ASin - Calculates the arcsine of a vector's values.",
            "ideal_use": "Used in computations requiring the inverse sine of an angle or value."
        },
        "ATAN": {
            "description": "Vector Trigonometric ATan - Calculates the arctangent of a vector's values.",
            "ideal_use": "Used to determine the angle whose tangent is a given value."
        },
        "CEIL": {
            "description": "Vector Ceil - Rounds up each value in the vector to the nearest integer.",
            "ideal_use": "Useful for ensuring results are rounded up to whole numbers in trading algorithms."
        },
        "COS": {
            "description": "Vector Trigonometric Cos - Calculates the cosine of a vector's values.",
            "ideal_use": "Commonly used in harmonic analysis or periodic trend modeling."
        },
        "COSH": {
            "description": "Vector Trigonometric Cosh - Calculates the hyperbolic cosine of a vector's values.",
            "ideal_use": "Used in advanced mathematical computations and some exotic indicators."
        },
        "EXP": {
            "description": "Vector Arithmetic Exp - Calculates the exponential (e^x) of a vector's values.",
            "ideal_use": "Commonly used in indicators requiring exponential growth or decay, such as volatility models."
        },
        "FLOOR": {
            "description": "Vector Floor - Rounds down each value in the vector to the nearest integer.",
            "ideal_use": "Used to ensure results are rounded down to whole numbers."
        },
        "LN": {
            "description": "Vector Log Natural - Calculates the natural logarithm (log base e) of a vector's values.",
            "ideal_use": "Used in growth rate computations or natural scaling of data."
        },
        "LOG10": {
            "description": "Vector Log10 - Calculates the base-10 logarithm of a vector's values.",
            "ideal_use": "Helpful in scaling data, especially when dealing with large ranges of values."
        },
        "SIN": {
            "description": "Vector Trigonometric Sin - Calculates the sine of a vector's values.",
            "ideal_use": "Used in harmonic analysis or modeling periodic trends."
        },
        "SINH": {
            "description": "Vector Trigonometric Sinh - Calculates the hyperbolic sine of a vector's values.",
            "ideal_use": "Used in advanced mathematical and financial computations."
        },
        "SQRT": {
            "description": "Vector Square Root - Calculates the square root of a vector's values.",
            "ideal_use": "Common in risk modeling, variance analysis, and volatility computations."
        },
        "TAN": {
            "description": "Vector Trigonometric Tan - Calculates the tangent of a vector's values.",
            "ideal_use": "Used in periodic analysis or advanced technical models."
        },
        "TANH": {
            "description": "Vector Trigonometric Tanh - Calculates the hyperbolic tangent of a vector's values.",
            "ideal_use": "Used in specialized computations requiring hyperbolic functions."
        }
    }

        self.statistical_indicators = {
        "BETA": {
            "description": "Beta - Measures the relationship (sensitivity) between a security's returns and a benchmark index.",
            "ideal_use": "Identify the relative volatility of a security to the market (e.g., BETA > 1 for higher volatility)."
        },
        "CORREL": {
            "description": "Pearson's Correlation Coefficient (r) - Measures the strength and direction of the linear relationship between two data sets.",
            "ideal_use": "Use CORREL > 0.8 or CORREL < -0.8 to identify strong positive or negative correlations."
        },
        "LINEARREG": {
            "description": "Linear Regression - Best-fit line over a specified period for trend analysis.",
            "ideal_use": "Use the slope of LINEARREG to determine trend direction; upward slope for bullish and downward for bearish."
        },
        "LINEARREG_ANGLE": {
            "description": "Linear Regression Angle - The angle of the linear regression line, indicating the strength of the trend.",
            "ideal_use": "Look for high positive angles (> 45°) for strong uptrends and high negative angles (< -45°) for strong downtrends."
        },
        "LINEARREG_INTERCEPT": {
            "description": "Linear Regression Intercept - The Y-intercept of the linear regression line.",
            "ideal_use": "Use in conjunction with slope to project expected price levels."
        },
        "LINEARREG_SLOPE": {
            "description": "Linear Regression Slope - The slope of the linear regression line.",
            "ideal_use": "Positive slope indicates bullish trend strength; negative slope indicates bearish trend strength."
        },
        "STDDEV": {
            "description": "Standard Deviation - Measures the dispersion of data points from the mean.",
            "ideal_use": "High STDDEV indicates high volatility; low STDDEV suggests consolidation."
        },
        "TSF": {
            "description": "Time Series Forecast - Predicts future values based on past linear regression.",
            "ideal_use": "Use TSF to project expected price levels; compare forecast to actual price for potential trades."
        },
        "VAR": {
            "description": "Variance - Measures the variability or spread of data points.",
            "ideal_use": "High VAR indicates high market variability; low VAR indicates stability and potential consolidation."
        }
    }

        self.math_operators = {
        "ADD": {
            "description": "Addition - Adds two data series or constants.",
            "ideal_scan": "Useful for combining indicators or offsetting values."
        },
        "DIV": {
            "description": "Division - Divides one data series or constant by another.",
            "ideal_scan": "Use for creating ratio-based indicators (e.g., price/volume)."
        },
        "MAX": {
            "description": "Maximum - Finds the maximum value over a specified period.",
            "ideal_scan": "Look for peaks in price or indicators to identify resistance levels or extremes."
        },
        "MAXINDEX": {
            "description": "Maximum Index - Returns the index of the maximum value in a period.",
            "ideal_scan": "Use to pinpoint when the highest value occurred."
        },
        "MIN": {
            "description": "Minimum - Finds the minimum value over a specified period.",
            "ideal_scan": "Look for troughs to identify support levels or extremes."
        },
        "MININDEX": {
            "description": "Minimum Index - Returns the index of the minimum value in a period.",
            "ideal_scan": "Use to pinpoint when the lowest value occurred."
        },
        "MINMAX": {
            "description": "Minimum and Maximum - Calculates both the minimum and maximum values over a period.",
            "ideal_scan": "Useful for identifying ranges or volatility."
        },
        "MINMAXINDEX": {
            "description": "Minimum and Maximum Index - Returns the indices of the minimum and maximum values in a period.",
            "ideal_scan": "Identify periods of extreme price movements for potential reversals."
        },
        "MULT": {
            "description": "Multiplication - Multiplies two data series or constants.",
            "ideal_scan": "Useful for scaling or amplifying indicator values."
        },
        "SUB": {
            "description": "Subtraction - Subtracts one data series or constant from another.",
            "ideal_scan": "Commonly used for calculating spreads or deviations."
        },
        "SUM": {
            "description": "Sum - Calculates the sum of values over a specified period.",
            "ideal_scan": "Detect cumulative volume or price movements for momentum analysis."
        }
    }
        self.volume_indicators = {
            # Volume Indicators
            "AD": {
                "description": "Chaikin A/D Line - Measures the cumulative flow of money into and out of a security.",
                "ideal_scan": "AD trending upward with price indicates strong accumulation; downward indicates distribution."
            },
            "ADOSC": {
                "description": "Chaikin A/D Oscillator - Tracks momentum changes in the A/D Line.",
                "ideal_scan": "ADOSC crossing above zero indicates bullish momentum; below zero indicates bearish momentum."
            },
            "OBV": {
                "description": "On Balance Volume - Tracks cumulative volume flow to confirm price trends.",
                "ideal_scan": "OBV making higher highs supports bullish trends; lower lows confirm bearish trends."
            },
            
            # Cycle Indicators
            "HT_DCPERIOD": {
                "description": "Hilbert Transform - Dominant Cycle Period. Identifies the dominant price cycle.",
                "ideal_scan": "Stable or increasing HT_DCPERIOD suggests consistent trends; sharp drops may indicate trend changes."
            },
            "HT_DCPHASE": {
                "description": "Hilbert Transform - Dominant Cycle Phase. Represents the phase of the dominant price cycle.",
                "ideal_scan": "Look for significant phase shifts to anticipate potential reversals."
            },
            "HT_PHASOR": {
                "description": "Hilbert Transform - Phasor Components. Outputs real and imaginary components of the phasor.",
                "ideal_scan": "Use changes in real or imaginary values to detect trend direction shifts."
            },
            "HT_SINE": {
                "description": "Hilbert Transform - SineWave. Produces sine and lead sine wave values for market cycles.",
                "ideal_scan": "Crossovers between sine and lead sine waves can indicate potential trend reversals."
            },
            "HT_TRENDMODE": {
                "description": "Hilbert Transform - Trend vs Cycle Mode. Identifies whether the market is trending or cyclic.",
                "ideal_scan": "HT_TRENDMODE = 1 for trending conditions; HT_TRENDMODE = 0 for cyclic conditions."
            },}
        self.price_transform_indicators ={
    "AVGPRICE": {
        "description": "Average Price - The average of open, high, low, and close prices.",
        "ideal_scan": "Use as a reference point; price above AVGPRICE indicates bullish momentum and below indicates bearish momentum."
    },
    "MEDPRICE": {
        "description": "Median Price - The average of the high and low prices.",
        "ideal_scan": "Use MEDPRICE to identify equilibrium levels; significant deviations may signal breakouts."
    },
    "TYPPRICE": {
        "description": "Typical Price - The average of high, low, and close prices.",
        "ideal_scan": "Use TYPPRICE to identify key levels for trend analysis."
    },
    "WCLPRICE": {
        "description": "Weighted Close Price - Heavily weights the closing price for a more accurate central price.",
        "ideal_scan": "Monitor deviations from WCLPRICE to detect overbought or oversold conditions."
    },}
        self.volatility_indicators ={
    "ATR": {
        "description": "Average True Range - Measures market volatility.",
        "ideal_scan": "ATR increasing signals rising volatility, good for breakout strategies; decreasing ATR indicates consolidation."
    },
    "NATR": {
        "description": "Normalized Average True Range - ATR expressed as a percentage of price.",
        "ideal_scan": "NATR > 5% indicates high volatility; < 2% suggests low volatility or consolidation."
    },
    "TRANGE": {
        "description": "True Range - Measures the absolute price range over a period.",
        "ideal_scan": "Look for high True Range values to signal volatile trading conditions."
    }}

        self.overlap_studies_indicators = {
        # Moving Average Indicators and Trend Analysis Tools
        "BBANDS": {
            "description": "Bollinger Bands - Measures volatility and identifies potential overbought/oversold conditions.",
            "ideal_scan": "Price breaking above upper band for potential bullish continuation; below lower band for bearish continuation."
        },
        "DEMA": {
            "description": "Double Exponential Moving Average - A faster, smoother moving average.",
            "ideal_scan": "DEMA crossover above price for bullish signals; below price for bearish signals."
        },
        "EMA": {
            "description": "Exponential Moving Average - Gives more weight to recent prices for trend tracking.",
            "ideal_scan": "EMA(20) crossing above EMA(50) for bullish signal; EMA(20) crossing below EMA(50) for bearish signal."
        },
        "HT_TRENDLINE": {
            "description": "Hilbert Transform - Instantaneous Trendline. A smoothed trendline for identifying price trends.",
            "ideal_scan": "Price crossing above HT_TRENDLINE for bullish breakout; below for bearish breakdown."
        },
        "KAMA": {
            "description": "Kaufman Adaptive Moving Average - Adjusts its speed based on market volatility.",
            "ideal_scan": "Price crossing above KAMA for potential bullish trend; below KAMA for bearish trend."
        },
        "MA": {
            "description": "Moving Average - A standard average for smoothing price action.",
            "ideal_scan": "MA(50) above MA(200) for bullish trends; MA(50) below MA(200) for bearish trends."
        },
        "MAMA": {
            "description": "MESA Adaptive Moving Average - Adapts to market cycles for smoother trend detection.",
            "ideal_scan": "Price crossing above MAMA for bullish signal; below for bearish signal."
        },
        "MAVP": {
            "description": "Moving Average with Variable Period - A moving average where the period changes dynamically.",
            "ideal_scan": "Crossover logic similar to MA but adjusted for dynamic periods."
        },
        "MIDPOINT": {
            "description": "MidPoint over period - Calculates the midpoint of prices over a specified period.",
            "ideal_scan": "Look for breakouts above midpoint as confirmation of bullish momentum; below for bearish."
        },
        "MIDPRICE": {
            "description": "Midpoint Price over period - The average of the high and low prices over a period.",
            "ideal_scan": "Breakouts above MIDPRICE for bullish trend; below for bearish trend."
        },
        "SAR": {
            "description": "Parabolic SAR - A stop-and-reverse system to identify potential trend reversals.",
            "ideal_scan": "Price crossing above SAR for bullish trend; below SAR for bearish trend."
        },
        "SAREXT": {
            "description": "Parabolic SAR - Extended. A more customizable version of the Parabolic SAR.",
            "ideal_scan": "Similar logic as SAR but allows for custom acceleration settings."
        },
        "SMA": {
            "description": "Simple Moving Average - A basic average over a specified period.",
            "ideal_scan": "SMA(50) crossing above SMA(200) for bullish signal; crossing below for bearish signal."
        },
        "T3": {
            "description": "Triple Exponential Moving Average - A smoother version of EMA with less lag.",
            "ideal_scan": "T3 crossover above price for bullish trend; below price for bearish trend."
        },
        "TEMA": {
            "description": "Triple Exponential Moving Average - Reduces lag and reacts faster to price changes.",
            "ideal_scan": "Price crossing above TEMA for bullish signals; below TEMA for bearish signals."
        },
        "TRIMA": {
            "description": "Triangular Moving Average - Gives more weight to the middle of the data series.",
            "ideal_scan": "TRIMA crossover above price for bullish momentum; below price for bearish momentum."
        },
        "WMA": {
            "description": "Weighted Moving Average - Assigns more weight to recent data points.",
            "ideal_scan": "WMA(10) crossing above WMA(50) for bullish trend; crossing below for bearish trend."
        }
    }

        self.momentum_indicators = {
    "ADX": {
        "description": "Average Directional Movement Index - Measures the strength of a trend.",
        "ideal_scan": "ADX > 25 indicates a strong trend; ADX < 20 indicates a weak trend."
    },
    "ADXR": {
        "description": "Average Directional Movement Index Rating - Smoothed version of ADX.",
        "ideal_scan": "ADXR > 25 indicates a strong trend; ADXR < 20 indicates weak or no trend."
    },
    "APO": {
        "description": "Absolute Price Oscillator - Shows the difference between two moving averages.",
        "ideal_scan": "APO > 0 for bullish momentum; APO < 0 for bearish momentum."
    },
    "AROON": {
        "description": "Aroon - Measures the strength and direction of a trend.",
        "ideal_scan": "Aroon-Up > 70 and Aroon-Down < 30 for bullish signals; Aroon-Up < 30 and Aroon-Down > 70 for bearish signals."
    },
    "AROONOSC": {
        "description": "Aroon Oscillator - The difference between Aroon-Up and Aroon-Down.",
        "ideal_scan": "AroonOsc > 50 for strong bullish momentum; AroonOsc < -50 for strong bearish momentum."
    },
    "BOP": {
        "description": "Balance Of Power - Measures the strength of buying vs selling pressure.",
        "ideal_scan": "BOP > 0.5 for bullish outliers; BOP < -0.5 for bearish outliers."
    },
    "CCI": {
        "description": "Commodity Channel Index - Identifies overbought and oversold levels.",
        "ideal_scan": "CCI > 100 for overbought conditions; CCI < -100 for oversold conditions."
    },
    "CMO": {
        "description": "Chande Momentum Oscillator - Measures momentum of a security.",
        "ideal_scan": "CMO > 50 for strong upward momentum; CMO < -50 for strong downward momentum."
    },
    "DX": {
        "description": "Directional Movement Index - Indicates trend direction and strength.",
        "ideal_scan": "DX > 25 indicates a strong trend; DX < 20 suggests trend weakness."
    },
    "MACD": {
        "description": "Moving Average Convergence/Divergence - Shows the relationship between two moving averages.",
        "ideal_scan": "MACD crossing above Signal Line for bullish; MACD crossing below Signal Line for bearish."
    },
    "MACDEXT": {
        "description": "MACD with controllable MA type - Customizable MACD version.",
        "ideal_scan": "Same logic as MACD but tune MA types for sensitivity."
    },
    "MACDFIX": {
        "description": "Moving Average Convergence/Divergence Fix 12/26 - Fixed parameter MACD.",
        "ideal_scan": "Use 12/26 crossover logic for bullish or bearish momentum."
    },
    "MFI": {
        "description": "Money Flow Index - Measures buying and selling pressure using volume.",
        "ideal_scan": "MFI > 80 for overbought conditions; MFI < 20 for oversold conditions."
    },
    "MINUS_DI": {
        "description": "Minus Directional Indicator - Part of ADX, shows bearish pressure.",
        "ideal_scan": "MINUS_DI > PLUS_DI for bearish trend confirmation."
    },
    "MINUS_DM": {
        "description": "Minus Directional Movement - Measures downward movement strength.",
        "ideal_scan": "High values indicate strong downward moves."
    },
    "MOM": {
        "description": "Momentum - Measures price momentum.",
        "ideal_scan": "MOM > 0 for bullish momentum; MOM < 0 for bearish momentum."
    },
    "PLUS_DI": {
        "description": "Plus Directional Indicator - Part of ADX, shows bullish pressure.",
        "ideal_scan": "PLUS_DI > MINUS_DI for bullish trend confirmation."
    },
    "PLUS_DM": {
        "description": "Plus Directional Movement - Measures upward movement strength.",
        "ideal_scan": "High values indicate strong upward moves."
    },
    "PPO": {
        "description": "Percentage Price Oscillator - MACD in percentage terms.",
        "ideal_scan": "PPO > 0 for bullish momentum; PPO < 0 for bearish momentum."
    },
    "ROC": {
        "description": "Rate of change: ((price/prevPrice)-1)*100 - Measures price change percentage.",
        "ideal_scan": "ROC > 10% for strong bullish moves; ROC < -10% for strong bearish moves."
    },
    "ROCP": {
        "description": "Rate of change Percentage: (price-prevPrice)/prevPrice.",
        "ideal_scan": "Similar to ROC; use significant thresholds based on asset."
    },
    "ROCR": {
        "description": "Rate of change ratio: (price/prevPrice).",
        "ideal_scan": "Use >1 for bullish; <1 for bearish."
    },
    "ROCR100": {
        "description": "Rate of change ratio 100 scale: (price/prevPrice)*100.",
        "ideal_scan": "Use >100 for bullish; <100 for bearish."
    },
    "RSI": {
        "description": "Relative Strength Index - Identifies overbought or oversold conditions.",
        "ideal_scan": "RSI > 70 for overbought; RSI < 30 for oversold."
    },
    "STOCH": {
        "description": "Stochastic - Measures momentum and potential reversals.",
        "ideal_scan": "Stochastic > 80 for overbought; <20 for oversold."
    },
    "STOCHF": {
        "description": "Stochastic Fast - More sensitive version of Stochastic.",
        "ideal_scan": "Same thresholds as Stochastic, but expect quicker signals."
    },
    "STOCHRSI": {
        "description": "Stochastic Relative Strength Index - Combines Stochastic and RSI.",
        "ideal_scan": "Use RSI thresholds (70/30) applied to Stochastic."
    },
    "TRIX": {
        "description": "1-day Rate-Of-Change (ROC) of a Triple Smooth EMA.",
        "ideal_scan": "TRIX > 0 for bullish momentum; TRIX < 0 for bearish momentum."
    },
    "ULTOSC": {
        "description": "Ultimate Oscillator - Combines short, medium, and long-term momentum.",
        "ideal_scan": "ULTOSC > 70 for overbought; ULTOSC < 30 for oversold."
    },
    "WILLR": {
        "description": "Williams' %R - Measures overbought/oversold levels.",
        "ideal_scan": "WILLR > -20 for overbought; WILLR < -80 for oversold."
    }
}

        self.ticker_df = pd.read_csv('files/ticker_csv.csv')
        self.ticker_to_id_map = dict(zip(self.ticker_df['ticker'], self.ticker_df['id']))
        self.intervals_to_scan = ['m5', 'm30', 'm60', 'm120', 'm240', 'd', 'w', 'm']  # Add or remove intervals as needed
    def parse_interval(self,interval_str):
        pattern = r'([a-zA-Z]+)(\d+)'
        match = re.match(pattern, interval_str)
        if match:
            unit = match.group(1)
            value = int(match.group(2))
            if unit == 'm':
                return value * 60
            elif unit == 'h':
                return value * 3600
            elif unit == 'd':
                return value * 86400
            else:
                raise ValueError(f"Unknown interval unit: {unit}")
        else:
            raise ValueError(f"Invalid interval format: {interval_str}")
    async def get_webull_id(self, symbol):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        ticker_id = self.ticker_to_id_map.get(symbol)
        return ticker_id
    async def get_webull_ids(self, symbols):
        """Fetch ticker IDs for a list of symbols in one go."""
        return {symbol: self.ticker_to_id_map.get(symbol) for symbol in symbols}
    async def get_candle_data(self, ticker, interval, headers, count:str='200'):
        try:
            timeStamp = None
            if ticker == 'I:SPX':
                ticker = 'SPX'
            elif ticker =='I:NDX':
                ticker = 'NDX'
            elif ticker =='I:VIX':
                ticker = 'VIX'
            elif ticker == 'I:RUT':
                ticker = 'RUT'
            elif ticker == 'I:XSP':
                ticker = 'XSP'
            



            if timeStamp is None:
                # if not set, default to current time
                timeStamp = int(time.time())
            tickerid = await self.get_webull_id(ticker)
            base_fintech_gw_url = f'https://quotes-gw.webullfintech.com/api/quote/charts/query-mini?tickerId={tickerid}&type={interval}&count={count}&timestamp={timeStamp}&restorationType=1&extendTrading=0'

            interval_mapping = {
                'm5': '5 min',
                'm30': '30 min',
                'm60': '1 hour',
                'm120': '2 hour',
                'm240': '4 hour',
                'd': 'day',
                'w': 'week',
                'm': 'month'
            }

            timespan = interval_mapping.get(interval)

            async with httpx.AsyncClient(headers=headers) as client:
                data = await client.get(base_fintech_gw_url)
                r = data.json()
                if r and isinstance(r, list) and 'data' in r[0]:
                    data = r[0]['data']

     
                    split_data = [row.split(",") for row in data]
             
                    df = pd.DataFrame(split_data, columns=['Timestamp', 'Open', 'Close', 'High', 'Low', 'Vwap', 'Volume', 'Avg'])
                    df['Timestamp'] = pd.to_numeric(df['Timestamp'], errors='coerce')
                    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s', utc=True)

                    # First localize to UTC, then convert to 'US/Eastern' and remove timezone info
                    df['Timestamp'] = df['Timestamp'].dt.tz_convert('US/Eastern').dt.tz_localize(None)
                    df['Ticker'] = ticker
                    df['timespan'] = interval
                    # Format the Timestamp column into ISO 8601 strings for API compatibility
                    df['Timestamp'] = df['Timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')  # ISO 8601 format
                    df['Close'] = df['Close'].astype(float)
                    df['Open'] = df['Open'].astype(float)
                    df['High'] = df['High'].astype(float)
                    df['Low'] = df['Low'].astype(float)
                    df['Volume'] = df['Volume'].astype(float)
                    df['Vwap'] = df['Vwap'].astype(float)
                    return df[::-1]
                
        except Exception as e:
            print(e)


    # Simulating async TA data fetching for each timeframe
    async def fetch_ta_data(self, timeframe, data):
        # Simulate an async operation to fetch data (e.g., from an API)

        return data.get(timeframe, {})
    async def async_scan_candlestick_patterns(self, df, interval):
        """
        Asynchronously scans for candlestick patterns in the given DataFrame over the specified interval.

        Parameters:
        - df (pd.DataFrame): DataFrame containing market data with columns ['High', 'Low', 'Open', 'Close', 'Volume', 'Vwap', 'Timestamp']
        - interval (str): Resampling interval based on custom mappings (e.g., 'm5', 'm30', 'd', 'w', 'm')

        Returns:
        - pd.DataFrame: DataFrame with additional columns indicating detected candlestick patterns and their bullish/bearish nature
        """
        # Mapping custom interval formats to Pandas frequency strings
        interval_mapping = {
            'm5': '5min',
            'm30': '30min',
            'm60': '60min',  # or '1H'
            'm120': '120min',  # or '2H'
            'm240': '240min',  # or '4H'
            'd': '1D',
            'w': '1W',
            'm': '1M'
            # Add more mappings as needed
        }

        # Convert the interval to Pandas frequency string
        pandas_interval = interval_mapping.get(interval)
        if pandas_interval is None:
            raise ValueError(f"Invalid interval '{interval}'. Please use one of the following: {list(interval_mapping.keys())}")

        # Ensure 'Timestamp' is datetime and set it as the index
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df.set_index('Timestamp', inplace=True)

        # Since data is most recent first, sort in ascending order for resampling
        df.sort_index(ascending=True, inplace=True)

        # Asynchronous resampling (using run_in_executor to avoid blocking the event loop)
        loop = asyncio.get_event_loop()
        ohlcv = await loop.run_in_executor(None, self.resample_ohlcv, df, pandas_interval)

        # Asynchronous pattern detection
        patterns_df = await loop.run_in_executor(None, self.detect_patterns, ohlcv)

        # Since we want the most recent data first, reverse the DataFrame
        patterns_df = patterns_df.iloc[::-1].reset_index()

        return patterns_df

    def resample_ohlcv(self, df, pandas_interval):
        ohlcv = df.resample(pandas_interval).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
            'Vwap': 'mean'
        }).dropna()
        return ohlcv

    async def async_scan_candlestick_patterns(self, df, interval):
        """
        Asynchronously scans for candlestick patterns in the given DataFrame over the specified interval.
        """
        # Mapping custom interval formats to Pandas frequency strings
        interval_mapping = {
            'm5': '5min',
            'm30': '30min',
            'm60': '60min',  # or '1H'
            'm120': '120min',  # or '2H'
            'm240': '240min',  # or '4H'
            'd': '1D',
            'w': '1W',
            'm': '1M'
        }

        # Convert the interval to Pandas frequency string
        pandas_interval = interval_mapping.get(interval)
        if pandas_interval is None:
            raise ValueError(f"Invalid interval '{interval}'. Please use one of the following: {list(interval_mapping.keys())}")

        # Ensure 'Timestamp' is datetime and set it as the index
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df.set_index('Timestamp', inplace=True)

        # Since data is most recent first, sort in ascending order for resampling
        df.sort_index(ascending=True, inplace=True)

        # Asynchronous resampling (using run_in_executor to avoid blocking the event loop)
        loop = asyncio.get_event_loop()
        ohlcv = await loop.run_in_executor(None, self.resample_ohlcv, df, pandas_interval)

        # Asynchronous pattern detection
        patterns_df = await loop.run_in_executor(None, self.detect_patterns, ohlcv)

        # No need to reverse the DataFrame; keep it in ascending order
        # patterns_df = patterns_df.iloc[::-1].reset_index()

        return patterns_df.reset_index()
   
    def resample_ohlcv(self, df, pandas_interval):
        ohlcv = df.resample(pandas_interval).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
            'Vwap': 'mean'
        }).dropna()
        return ohlcv
    def detect_patterns(self, ohlcv):
        # Initialize pattern columns
        patterns = ['hammer', 'inverted_hammer', 'hanging_man', 'shooting_star', 'doji',
                    'bullish_engulfing', 'bearish_engulfing', 'bullish_harami', 'bearish_harami',
                    'morning_star', 'evening_star', 'piercing_line', 'dark_cloud_cover',
                    'three_white_soldiers', 'three_black_crows', 'abandoned_baby',
                    'rising_three_methods', 'falling_three_methods', 'three_inside_up', 'three_inside_down',
                     'gravestone_doji', 'butterfly_doji', 'harami_cross', 'tweezer_top', 'tweezer_bottom']



        for pattern in patterns:
            ohlcv[pattern] = False

        ohlcv['signal'] = None  # To indicate Bullish or Bearish signal

        # Iterate over the DataFrame to detect patterns
        for i in range(len(ohlcv)):
            curr_row = ohlcv.iloc[i]
            prev_row = ohlcv.iloc[i - 1] if i >= 1 else None
            prev_prev_row = ohlcv.iloc[i - 2] if i >= 2 else None



            uptrend = self.is_uptrend(ohlcv, i)
            downtrend = self.is_downtrend(ohlcv, i)


            # Single-candle patterns
            if downtrend and self.is_hammer(curr_row):
                ohlcv.at[ohlcv.index[i], 'hammer'] = True
                ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
            if downtrend and self.is_inverted_hammer(curr_row):
                ohlcv.at[ohlcv.index[i], 'inverted_hammer'] = True
                ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
            if uptrend and self.is_hanging_man(curr_row):
                ohlcv.at[ohlcv.index[i], 'hanging_man'] = True
                ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
            if uptrend and self.is_shooting_star(curr_row):
                ohlcv.at[ohlcv.index[i], 'shooting_star'] = True
                ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
            if downtrend and self.is_dragonfly_doji(curr_row):
                ohlcv.at[ohlcv.index[i], 'dragonfly_doji'] = True
                ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
            if uptrend and self.is_gravestone_doji(curr_row):
                ohlcv.at[ohlcv.index[i], 'gravestone_doji'] = True
                ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'

            # Two-candle patterns
            if prev_row is not None:
                if downtrend and self.is_bullish_engulfing(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'bullish_engulfing'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                if uptrend and self.is_bearish_engulfing(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'bearish_engulfing'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
                if downtrend and self.is_bullish_harami(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'bullish_harami'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                if uptrend and self.is_bearish_harami(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'bearish_harami'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
                if downtrend and self.is_piercing_line(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'piercing_line'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                if uptrend and self.is_dark_cloud_cover(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'dark_cloud_cover'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
                if downtrend and self.is_tweezer_bottom(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'tweezer_bottom'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                if uptrend and self.is_tweezer_top(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'tweezer_top'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
                if downtrend and self.is_harami_cross(prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'harami_cross'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'neutral'

            # Three-candle patterns
            if prev_row is not None and prev_prev_row is not None:
                if downtrend and self.is_morning_star(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'morning_star'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                if uptrend and self.is_evening_star(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'evening_star'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
                if downtrend and self.is_three_white_soldiers(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'three_white_soldiers'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                if uptrend and self.is_three_black_crows(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'three_black_crows'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
                if downtrend and self.is_three_inside_up(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'three_inside_up'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                if uptrend and self.is_three_inside_down(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'three_inside_down'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
                if self.is_abandoned_baby(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'abandoned_baby'] = True
                    if curr_row['Close'] > prev_row['Close']:
                        ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                    else:
                        ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'
                if downtrend and self.is_rising_three_methods(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'rising_three_methods'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bullish'
                if uptrend and self.is_falling_three_methods(prev_prev_row, prev_row, curr_row):
                    ohlcv.at[ohlcv.index[i], 'falling_three_methods'] = True
                    ohlcv.at[ohlcv.index[i], 'signal'] = 'bearish'

        return ohlcv
    def is_gravestone_doji(self, row):
        body_length = abs(row['Close'] - row['Open'])
        total_range = row['High'] - row['Low']
        upper_shadow = row['High'] - max(row['Close'], row['Open'])
        lower_shadow = min(row['Close'], row['Open']) - row['Low']
        return total_range != 0 and body_length <= 0.1 * total_range and lower_shadow == 0 and upper_shadow > 2 * body_length
        
    def is_three_inside_up(self, prev_prev_row, prev_row, curr_row):
        first_bearish = prev_prev_row['Close'] < prev_prev_row['Open']
        second_bullish = prev_row['Close'] > prev_row['Open']
        third_bullish = curr_row['Close'] > curr_row['Open']
        return (first_bearish and second_bullish and third_bullish and
                prev_row['Open'] > prev_prev_row['Close'] and prev_row['Close'] < prev_prev_row['Open'] and
                curr_row['Close'] > prev_prev_row['Open'])


    def is_tweezer_top(self, prev_row, curr_row):
        return (prev_row['High'] == curr_row['High']) and (prev_row['Close'] > prev_row['Open']) and (curr_row['Close'] < curr_row['Open'])

    def is_tweezer_bottom(self, prev_row, curr_row):
        return (prev_row['Low'] == curr_row['Low']) and (prev_row['Close'] < prev_row['Open']) and (curr_row['Close'] > curr_row['Open'])

    def is_dragonfly_doji(self, row):
        body_length = abs(row['Close'] - row['Open'])
        total_range = row['High'] - row['Low']
        upper_shadow = row['High'] - max(row['Close'], row['Open'])
        lower_shadow = min(row['Close'], row['Open']) - row['Low']
        return total_range != 0 and body_length <= 0.1 * total_range and upper_shadow == 0 and lower_shadow > 2 * body_length


    def is_uptrend(self, df: pd.DataFrame, length: int =7) -> bool:
        """
        Check if the dataframe shows an uptrend over the specified length.
        
        An uptrend is defined as consecutive increasing 'Close' values for the given length.
        The dataframe is assumed to have the most recent candle at index 0.
        """
        try:
            if len(df) < length:
                raise ValueError(f"DataFrame length ({len(df)}) is less than the specified length ({length})")
            
            # Since the most recent data is at index 0, we need to reverse the direction of comparison.
            return (df['Close'].iloc[:length].diff(periods=-1).iloc[:-1] > 0).all()

        except Exception as e:
            print(f"Failed - {e}")

    def is_downtrend(self, df: pd.DataFrame, length: int = 7) -> bool:
        """
        Check if the dataframe shows a downtrend over the specified length.
        
        A downtrend is defined as consecutive decreasing 'Close' values for the given length.
        """
        try:
            if len(df) < length:
                raise ValueError(f"DataFrame length ({len(df)}) is less than the specified length ({length})")
            
            # Since the most recent data is at index 0, we need to reverse the direction of comparison.
            return (df['Close'].iloc[:length].diff(periods=-1).iloc[:-1] < 0).all()
        except Exception as e:
            print(f"Failed - {e}")

    def is_hammer(self,row):
        body_length = abs(row['Close'] - row['Open'])
        total_range = row['High'] - row['Low']
        upper_shadow = row['High'] - max(row['Close'], row['Open'])
        lower_shadow = min(row['Close'], row['Open']) - row['Low']
        return (lower_shadow >= 2 * body_length) and (upper_shadow <= body_length)

    def is_inverted_hammer(self,row):
        body_length = abs(row['Close'] - row['Open'])
        total_range = row['High'] - row['Low']
        upper_shadow = row['High'] - max(row['Open'], row['Close'])
        lower_shadow = min(row['Open'], row['Close']) - row['Low']
        return (upper_shadow >= 2 * body_length) and (lower_shadow <= body_length)

    def is_hanging_man(self, row):
        return self.is_hammer(row)

    def is_shooting_star(self, row):
        return self.is_inverted_hammer(row)

    def is_doji(self,row):
        body_length = abs(row['Close'] - row['Open'])
        total_range = row['High'] - row['Low']
        return total_range != 0 and body_length <= 0.1 * total_range

    def is_bullish_engulfing(self,prev_row, curr_row):
        return (prev_row['Close'] < prev_row['Open']) and (curr_row['Close'] > curr_row['Open']) and \
            (curr_row['Open'] < prev_row['Close']) and (curr_row['Close'] > prev_row['Open'])

    def is_bearish_engulfing(self,prev_row, curr_row):
        return (prev_row['Close'] > prev_row['Open']) and (curr_row['Close'] < curr_row['Open']) and \
            (curr_row['Open'] > prev_row['Close']) and (curr_row['Close'] < prev_row['Open'])

    def is_bullish_harami(self,prev_row, curr_row):
        return (prev_row['Open'] > prev_row['Close']) and (curr_row['Open'] < curr_row['Close']) and \
            (curr_row['Open'] > prev_row['Close']) and (curr_row['Close'] < prev_row['Open'])

    def is_bearish_harami(self,prev_row, curr_row):
        return (prev_row['Open'] < prev_row['Close']) and (curr_row['Open'] > curr_row['Close']) and \
            (curr_row['Open'] < prev_row['Close']) and (curr_row['Close'] > prev_row['Open'])

    def is_morning_star(self,prev_prev_row, prev_row, curr_row):
        first_bearish = prev_prev_row['Close'] < prev_prev_row['Open']
        second_small_body = abs(prev_row['Close'] - prev_row['Open']) < abs(prev_prev_row['Close'] - prev_prev_row['Open']) * 0.3
        third_bullish = curr_row['Close'] > curr_row['Open']
        first_midpoint = (prev_prev_row['Open'] + prev_prev_row['Close']) / 2
        third_close_above_first_mid = curr_row['Close'] > first_midpoint
        return first_bearish and second_small_body and third_bullish and third_close_above_first_mid

    def is_evening_star(self,prev_prev_row, prev_row, curr_row):
        first_bullish = prev_prev_row['Close'] > prev_prev_row['Open']
        second_small_body = abs(prev_row['Close'] - prev_row['Open']) < abs(prev_prev_row['Close'] - prev_prev_row['Open']) * 0.3
        third_bearish = curr_row['Close'] < curr_row['Open']
        first_midpoint = (prev_prev_row['Open'] + prev_prev_row['Close']) / 2
        third_close_below_first_mid = curr_row['Close'] < first_midpoint
        return first_bullish and second_small_body and third_bearish and third_close_below_first_mid

    def is_piercing_line(self,prev_row, curr_row):
        first_bearish = prev_row['Close'] < prev_row['Open']
        second_bullish = curr_row['Close'] > curr_row['Open']
        open_below_prev_low = curr_row['Open'] < prev_row['Low']
        prev_midpoint = (prev_row['Open'] + prev_row['Close']) / 2
        close_above_prev_mid = curr_row['Close'] > prev_midpoint
        return first_bearish and second_bullish and open_below_prev_low and close_above_prev_mid
        
    def has_gap_last_4_candles(self, ohlcv, index):
        """
        Checks if there's a gap within the last 4 candles, either up or down.
        A gap up occurs when the current open is higher than the previous close,
        and a gap down occurs when the current open is lower than the previous close.
        
        :param ohlcv: The OHLCV dataframe with historical data.
        :param index: The current index in the dataframe.
        :return: Boolean value indicating whether a gap exists in the last 4 candles.
        """
        # Ensure there are at least 4 candles to check
        if index < 3:
            return False

        # Iterate through the last 4 candles
        for i in range(index - 3, index):
            curr_open = ohlcv.iloc[i + 1]['Open']
            prev_close = ohlcv.iloc[i]['Close']
            
            # Check for a gap (either up or down)
            if curr_open > prev_close or curr_open < prev_close:
                return True  # A gap is found

        return False  # No gap found in the last 4 candles

    def is_abandoned_baby(self, prev_prev_row, prev_row, curr_row):
        # Bullish Abandoned Baby
        first_bearish = prev_prev_row['Close'] < prev_prev_row['Open']
        doji = self.is_doji(prev_row)
        third_bullish = curr_row['Close'] > curr_row['Open']
        
        # Check for gaps
        gap_down = prev_row['Open'] < prev_prev_row['Close'] and prev_row['Close'] < prev_prev_row['Low']
        gap_up = curr_row['Open'] > prev_row['Close'] and curr_row['Close'] > prev_row['High']
        
        return first_bearish and doji and third_bullish and gap_down and gap_up

    def is_harami_cross(self, prev_row, curr_row):
        # Harami Cross is a special form of Harami with the second candle being a Doji
        return self.is_bullish_harami(prev_row, curr_row) and self.is_doji(curr_row)

    def is_rising_three_methods(self, prev_prev_row, prev_row, curr_row):
        # Rising Three Methods (Bullish Continuation)
        first_bullish = prev_prev_row['Close'] > prev_prev_row['Open']
        small_bearish = prev_row['Close'] < prev_row['Open'] and prev_row['Close'] > prev_prev_row['Open']
        final_bullish = curr_row['Close'] > curr_row['Open'] and curr_row['Close'] > prev_prev_row['Close']
        
        return first_bullish and small_bearish and final_bullish

    def is_falling_three_methods(self, prev_prev_row, prev_row, curr_row):
        # Falling Three Methods (Bearish Continuation)
        first_bearish = prev_prev_row['Close'] < prev_prev_row['Open']
        small_bullish = prev_row['Close'] > prev_row['Open'] and prev_row['Close'] < prev_prev_row['Open']
        final_bearish = curr_row['Close'] < curr_row['Open'] and curr_row['Close'] < prev_prev_row['Close']
        
        return first_bearish and small_bullish and final_bearish

    def is_three_inside_down(self, prev_prev_row, prev_row, curr_row):
        # Bearish reversal pattern
        first_bullish = prev_prev_row['Close'] > prev_prev_row['Open']
        second_bearish = prev_row['Close'] < prev_row['Open']
        third_bearish = curr_row['Close'] < curr_row['Open']
        
        return (first_bullish and second_bearish and third_bearish and
                prev_row['Open'] < prev_prev_row['Close'] and prev_row['Close'] > prev_prev_row['Open'] and
                curr_row['Close'] < prev_prev_row['Open'])
    def is_dark_cloud_cover(self,prev_row, curr_row):
        first_bullish = prev_row['Close'] > prev_row['Open']
        second_bearish = curr_row['Close'] < curr_row['Open']
        open_above_prev_high = curr_row['Open'] > prev_row['High']
        prev_midpoint = (prev_row['Open'] + prev_row['Close']) / 2
        close_below_prev_mid = curr_row['Close'] < prev_midpoint
        return first_bullish and second_bearish and open_above_prev_high and close_below_prev_mid

    def is_three_white_soldiers(self,prev_prev_row, prev_row, curr_row):
        first_bullish = prev_prev_row['Close'] > prev_prev_row['Open']
        second_bullish = prev_row['Close'] > prev_row['Open']
        third_bullish = curr_row['Close'] > curr_row['Open']
        return (first_bullish and second_bullish and third_bullish and
                prev_row['Open'] < prev_prev_row['Close'] and curr_row['Open'] < prev_row['Close'] and
                prev_row['Close'] > prev_prev_row['Close'] and curr_row['Close'] > prev_row['Close'])

    def is_three_black_crows(self, prev_prev_row, prev_row, curr_row):
        first_bearish = prev_prev_row['Close'] < prev_prev_row['Open']
        second_bearish = prev_row['Close'] < prev_row['Open']
        third_bearish = curr_row['Close'] < curr_row['Open']
        return (first_bearish and second_bearish and third_bearish and
                prev_row['Open'] > prev_prev_row['Close'] and curr_row['Open'] > prev_row['Close'] and
                prev_row['Close'] < prev_prev_row['Close'] and curr_row['Close'] < prev_row['Close'])
    




    async def get_candle_streak(self, ticker, headers=None):
        """Returns the streak and trend (up or down) for each timespan, along with the ticker"""
        
        async def calculate_streak(ticker, interval, data):
            """Helper function to calculate the streak and trend for a given dataset"""
            # Conversion dictionary to map intervals to human-readable timespans
            conversion = { 
                'm1': '1min',
                'm5': '5min',
                'm30': '30min',
                'm60': '1h',
                'm120': '2h',
                'm240': '4h',
                'd': 'day',
                'w': 'week',
                'm': 'month'
            }

            # Initialize variables
            streak_type = None
            streak_length = 1  # Starting with 1 since the most recent candle is part of the streak

            # Start from the most recent candle and scan forward through the data
            for i in range(1, len(data)):
                current_open = data['Open'].iloc[i]
                current_close = data['Close'].iloc[i]

                # Determine if the candle is green (up) or red (down)
                if current_close > current_open:
                    current_streak_type = 'up'
                elif current_close < current_open:
                    current_streak_type = 'down'
                else:
                    break  # Stop if the candle is neutral (no movement)

                if streak_type is None:
                    streak_type = current_streak_type  # Set initial streak type
                elif streak_type != current_streak_type:
                    break  # Break if the trend changes (from up to down or vice versa)

                streak_length += 1

            if streak_type is None:
                return {f"streak_{conversion[interval]}": 0, f"trend_{conversion[interval]}": "no trend"}

            return {f"streak_{conversion[interval]}": streak_length, f"trend_{conversion[interval]}": streak_type}


        try:
            # Define the intervals of interest
            intervals = ['d', 'w', 'm', 'm5', 'm30', 'm60', 'm120', 'm240']  # Choose 4h, day, and week for your example

            # Fetch the data asynchronously for all intervals
            # Fetch the data asynchronously for all intervals
            data_list = await asyncio.gather(
                *[self.get_candle_data(ticker=ticker, interval=interval, headers=headers, count=200) for interval in intervals]
            )

            # Process each interval's data and gather the streak and trend
            streak_data = {}
            for interval, data in zip(intervals, data_list):
                result = await calculate_streak(ticker, interval, data)
                streak_data.update(result)  # Add the streak and trend for each timespan

            # Add the ticker to the result
            streak_data["ticker"] = ticker

            return streak_data

        except Exception as e:
            print(f"{ticker}: {e}")
            return None



    def classify_candle(self,open_value, close_value):
        if close_value > open_value:
            return "green"
        elif close_value < open_value:
            return "red"
        else:
            return "neutral"

    # Function to classify candle colors across all intervals
    def classify_candle_set(self,opens, closes):
        return [self.classify_candle(open_val, close_val) for open_val, close_val in zip(opens, closes)]

    # Function to classify shapes across rows for one set of rows
    def classify_shape(self,open_val, high_val, low_val, close_val, color, interval, ticker):
        body = abs(close_val - open_val)
        upper_wick = high_val - max(open_val, close_val)
        lower_wick = min(open_val, close_val) - low_val
        total_range = high_val - low_val

        if total_range == 0:
            return None  # Skip if there's no valid data

        body_percentage = (body / total_range) * 100
        upper_wick_percentage = (upper_wick / total_range) * 100
        lower_wick_percentage = (lower_wick / total_range) * 100

        if body_percentage < 10 and upper_wick_percentage > 45 and lower_wick_percentage > 45:
            return f"Doji ({color}) - {ticker} [{interval}]"
        elif body_percentage > 60 and upper_wick_percentage < 20 and lower_wick_percentage < 20:
            return f"Long Body ({color}) - {ticker} [{interval}]"
        elif body_percentage < 30 and lower_wick_percentage > 50:
            return f"Hammer ({color}) - {ticker} [{interval}]" if color == "green" else f"Hanging Man ({color}) - {ticker} [{interval}]"
        elif body_percentage < 30 and upper_wick_percentage > 50:
            return f"Inverted Hammer ({color}) - {ticker} [{interval}]" if color == "green" else f"Shooting Star ({color}) - {ticker} [{interval}]"
        elif body_percentage < 50 and upper_wick_percentage > 20 and lower_wick_percentage > 20:
            return f"Spinning Top ({color}) - {ticker} [{interval}]"
        else:
            return f"Neutral ({color}) - {ticker} [{interval}]"

    # Function to classify candle shapes across all intervals for a given ticker
    def classify_candle_shapes(self, opens, highs, lows, closes, colors, intervals, ticker):
        return [self.classify_shape(open_val, high_val, low_val, close_val, color, interval, ticker)
                for open_val, high_val, low_val, close_val, color, interval in zip(opens, highs, lows, closes, colors, intervals)]



    async def get_candle_patterns(self, ticker:str='AAPL', interval:str='m60', headers=None):

        # Function to compare two consecutive candles and detect patterns like engulfing and tweezers
        def compare_candles(open1, close1, high1, low1, color1, open2, close2, high2, low2, color2, interval, ticker):
            conversion = { 
                'm1': '1min',
                'm5': '5min',
                'm30': '30min',
                'm60': '1h',
                'm120': '2h',
                'm240': '4h',
                'd': 'day',
                'w': 'week',
                'm': 'month'
            }

            # Bullish Engulfing
            if color1 == "red" and color2 == "green" and open2 < close1 and close2 > open1:
                candle_pattern = f"Bullish Engulfing - {ticker} {conversion.get(interval)}"
                return candle_pattern
            # Bearish Engulfing
            elif color1 == "green" and color2 == "red" and open2 > close1 and close2 < open1:
                candle_pattern = f"Bearish Engulfing - {conversion.get(interval)}"
                return candle_pattern
            # Tweezer Top
            elif color1 == "green" and color2 == "red" and high1 == high2:
                candle_pattern = f"Tweezer Top - {conversion.get(interval)}"
                return candle_pattern
            # Tweezer Bottom
            elif color1 == "red" and color2 == "green" and low1 == low2:
                candle_pattern = f"tweezer_bottom"
                return candle_pattern
            
    
        try:
            df = await self.async_get_td9(ticker=ticker, interval=interval, headers=headers)
            df = df[::-1]

            color1 = 'red' if df['Open'].loc[0] > df['Close'].loc[0] else 'green' if df['Close'].loc[0] > df['Open'].loc[0] else 'grey'
            color2 = 'red' if df['Open'].loc[1] > df['Close'].loc[1] else 'green' if df['Close'].loc[1] > df['Open'].loc[1] else 'grey'




            candle_pattern = compare_candles(close1=df['Close'].loc[0], close2=df['Close'].loc[1], high1=df['High'].loc[0], high2=df['High'].loc[1], low1=df['Low'].loc[0], low2=df['Low'].loc[1], open1=df['Open'].loc[0], open2=df['Open'].loc[1], color1=color1, color2=color2, interval=interval, ticker=ticker)
            if candle_pattern is not []:
                dict = { 
                    'ticker': ticker,
                    'interval': interval,
                    'shape': candle_pattern
                }

                df = pd.DataFrame(dict, index=[0])
                if df['shape'] is not None:
                    return df
        except Exception as e:
            print(e)


    async def get_second_ticks(self, headers, ticker:str, second_timespan:str='5s',count:str='800'):
        ticker_id = await self.get_webull_id(ticker)
        url=f"https://quotes-gw.webullfintech.com/api/quote/charts/seconds-mini?type={second_timespan}&count={count}&restorationType=0&tickerId={ticker_id}"



        async with httpx.AsyncClient(headers=headers) as client:
            data = await client.get(url)

            data = data.json()

            data = [i.get('data') for i in data]

            for i in data:
                print(i)


    async def macd_rsi(self, rsi_type, macd_type, size:str='50'):

        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/rsi-macd?rankType=rsi_macd&regionId=6&supportBroker=8&rsi=rsi.{rsi_type}&macd=macd.{macd_type}&direction=-1&pageIndex=1&pageSize={size}")

            data = data.json()
            data = data['data']
            ticker = [i.get('ticker') for i in data]
            symbols = [i.get('symbol') for i in ticker]

            return symbols


    @njit
    def ema_njit(self, prices: np.ndarray, period: int) -> np.ndarray:
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
    def compute_macd_histogram(self, prices: np.ndarray) -> np.ndarray:
        """
        Compute the MACD histogram from closing prices using EMA periods of 12, 26, and 9.
        
        Parameters
        ----------
        prices : np.ndarray
            Array of price data (e.g., daily close prices).
        
        Returns
        -------
        np.ndarray
            The MACD histogram values: MACD_line - signal_line.
        
        Explanation
        -----------
        1) fast = EMA(prices, 12)
        2) slow = EMA(prices, 26)
        3) macd_line = fast - slow
        4) signal_line = EMA(macd_line, 9)
        5) histogram = macd_line - signal_line
        """
        # Quick checks
        if len(prices) < 2:
            return np.array([], dtype=np.float64)
        
        fast = self.ema_njit(prices, 12)
        slow = self.ema_njit(prices, 26)
        macd_line = fast - slow
        signal_line = self.ema_njit(macd_line, 9)
        hist = macd_line - signal_line
        return hist

    def add_parabolic_sar_signals(
            self,
        df: pd.DataFrame,
        af_initial: float = 0.23,
        af_max: float = 0.75,
        bb_period: int = 20,
        bb_mult: float = 2.0
    ) -> pd.DataFrame:
        """
        Compute the Parabolic SAR for each bar, then compare it to Bollinger Bands
        to see if:
        - A 'long' (up) PSAR is below the lower BB.
        - A 'short' (down) PSAR is above the upper BB.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain columns:
                'h': high
                'l': low
                'c': close
            and be sorted in ascending timestamp order.
        af_initial : float
            The initial acceleration factor for the Parabolic SAR.
        af_max : float
            The maximum (acceleration) factor to which AF can increase.
        bb_period : int
            The look-back period for Bollinger Bands (on 'c' by default).
        bb_mult : float
            The standard-deviation multiplier for Bollinger Bands.

        Returns
        -------
        pd.DataFrame
            The original DataFrame with new columns added:
                'psar': the Parabolic SAR value at each bar
                'psar_direction': 'long' or 'short'
                'bb_middle', 'bb_upper', 'bb_lower': Bollinger Band columns
                'psar_long_below_lower_band': boolean
                'psar_short_above_upper_band': boolean

        Notes
        -----
        - This implementation of Parabolic SAR follows Welles Wilder's original
        algorithm. 
        - Bollinger Bands default to 20 bars and 2 std dev, which you can adjust.
        """
        df = df.copy()

        # Safety check: Must have columns h, l, c and be sorted
        required_cols = {'h', 'l', 'c'}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"DataFrame must contain {required_cols} columns.")

        # ─────────────────────────────────────────────────────────────────────────
        # 1) Compute Bollinger Bands on 'close' with rolling mean & std
        # ─────────────────────────────────────────────────────────────────────────
        df["bb_middle"] = df["c"].rolling(bb_period).mean()
        df["bb_std"] = df["c"].rolling(bb_period).std(ddof=0)  # population std
        df["bb_upper"] = df["bb_middle"] + bb_mult * df["bb_std"]
        df["bb_lower"] = df["bb_middle"] - bb_mult * df["bb_std"]

        # ─────────────────────────────────────────────────────────────────────────
        # 2) Compute Parabolic SAR
        #    We'll store the result in df["psar"] and df["psar_direction"].
        # ─────────────────────────────────────────────────────────────────────────
        n = len(df)
        psar = [np.nan] * n
        direction = [None] * n  # 'long' or 'short'
        
        if n < 2:
            # Not enough data to compute a meaningful PSAR
            df["psar"] = psar
            df["psar_direction"] = direction
            return df

        # Initialize the very first PSAR “trend” based on the first two bars:
        # We'll assume that if the second bar's close is higher than the first,
        # we start in an uptrend, else downtrend.
        # Start the PSAR at the first bar's low/high in up/down trend.
        first_bar = 0
        second_bar = 1

        if df.loc[second_bar, "c"] > df.loc[first_bar, "c"]:
            current_direction = "long"
            # Start PSAR at lowest low of first two bars
            psar[first_bar] = df.loc[first_bar, "l"]
            ep = df.loc[first_bar:second_bar, "h"].max()  # highest high so far
        else:
            current_direction = "short"
            # Start PSAR at highest high of first two bars
            psar[first_bar] = df.loc[first_bar, "h"]
            ep = df.loc[first_bar:second_bar, "l"].min()  # lowest low so far

        # For the second bar, we must still finalize the initial PSAR.
        psar[second_bar] = psar[first_bar]
        af = af_initial  # acceleration factor

        direction[first_bar] = current_direction
        direction[second_bar] = current_direction

        # Main loop for bars 2..n-1
        for i in range(2, n):
            prev_psar = psar[i - 1]
            prev_dir = direction[i - 1]

            if prev_dir == "long":
                # Tentative next PSAR:
                new_psar = prev_psar + af * (ep - prev_psar)
                # SAR cannot exceed the last two lows in an uptrend
                new_psar = min(
                    new_psar,
                    df.loc[i - 1, "l"],
                    df.loc[i - 2, "l"] if i - 2 >= 0 else df.loc[i - 1, "l"]
                )

                # Check if we continue or flip direction
                if df.loc[i, "l"] > new_psar:
                    # Still in uptrend
                    current_direction = "long"
                    psar[i] = new_psar
                    # Update EP if we made a new high
                    if df.loc[i, "h"] > ep:
                        ep = df.loc[i, "h"]
                        af = min(af + af_initial, af_max)
                else:
                    # Flip to downtrend
                    current_direction = "short"
                    psar[i] = ep  # start new PSAR at previous EP
                    ep = df.loc[i, "l"]  # reset EP to this bar's low
                    af = af_initial  # reset AF
            else:
                # short
                new_psar = prev_psar - af * (prev_psar - ep)
                # SAR cannot be lower than the last two highs in a downtrend
                new_psar = max(
                    new_psar,
                    df.loc[i - 1, "h"],
                    df.loc[i - 2, "h"] if i - 2 >= 0 else df.loc[i - 1, "h"]
                )

                # Check if we continue or flip direction
                if df.loc[i, "h"] < new_psar:
                    # Still in downtrend
                    current_direction = "short"
                    psar[i] = new_psar
                    # Update EP if we made a new low
                    if df.loc[i, "l"] < ep:
                        ep = df.loc[i, "l"]
                        af = min(af + af_initial, af_max)
                else:
                    # Flip to uptrend
                    current_direction = "long"
                    psar[i] = ep  # start new PSAR at previous EP
                    ep = df.loc[i, "h"]  # reset EP to this bar's high
                    af = af_initial  # reset AF

            direction[i] = current_direction

        df["psar"] = psar
        df["psar_direction"] = direction

        # ─────────────────────────────────────────────────────────────────────────
        # 3) Identify where PSAR-long is below the lower BB, 
        #    or PSAR-short is above the upper BB
        # ─────────────────────────────────────────────────────────────────────────
        # Safety: Bollinger columns may have NaN in the first ~bb_period rows.
        df["psar_long_below_lower_band"] = (
            (df["psar_direction"] == "long") &
            (df["psar"] < df["bb_lower"])
        )
        
        df["psar_short_above_upper_band"] = (
            (df["psar_direction"] == "short") &
            (df["psar"] > df["bb_upper"])
        )

        # Cleanup: optional drop of the 'bb_std' intermediate column
        df.drop(columns=["bb_std"], inplace=True)

        return df

    def compute_volume_profile(self, df_intraday, num_bins=100):
        """
        Compute POC, VAH, VAL from intraday data using a simple volume profile approach.
        Args:
            df_intraday: DataFrame with columns [open, high, low, close, volume].
                        All rows must be from the SAME period (e.g. the same day or same week).
            num_bins:    How many price bins to use for the volume distribution.
        Returns:
            (poc, vah, val) for the given period.
        """

        # 1) Determine the price range for this period
        period_low = df_intraday['l'].min()
        period_high = df_intraday['h'].max()
        total_volume = df_intraday['v'].sum()

        if period_low == period_high:
            # Edge case: no price range
            return (period_low, period_low, period_low)

        # 2) Create price bins
        bin_edges = np.linspace(period_low, period_high, num_bins + 1)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
        volume_profile = np.zeros(num_bins)

        # 3) Distribute volume across bins
        for idx, row in df_intraday.iterrows():
            bar_low = row['l']
            bar_high = row['h']
            bar_volume = row['v']

            # Simple approach: add this bar's volume to the bin closest to the bar's "mid" price
            bar_mid = (bar_low + bar_high) / 2.0
            closest_bin = np.argmin(np.abs(bin_centers - bar_mid))
            volume_profile[closest_bin] += bar_volume

            # Alternatively, do something more advanced: distribute proportionally from low->high
            # This would require a bit more looping or interpolation.

        # 4) Find the Point of Control (POC): bin with highest volume
        poc_index = np.argmax(volume_profile)
        poc_price = bin_centers[poc_index]

        # 5) Identify Value Area: we want ~70% of total volume around the POC
        # Start from the poc bin, expand up/down until we capture ~70% of volume.
        cum_volume = volume_profile[poc_index]
        lower_idx = poc_index
        upper_idx = poc_index

        # The fraction of total volume we want:
        target_volume = 0.70 * total_volume

        # Expand outwards
        while cum_volume < target_volume:
            # Expand either up or down depending on which side has more volume.
            move_lower = False
            move_upper = False

            # Check if we can move down
            if lower_idx > 0:
                down_vol = volume_profile[lower_idx - 1]
            else:
                down_vol = -1  # can't move lower

            # Check if we can move up
            if upper_idx < num_bins - 1:
                up_vol = volume_profile[upper_idx + 1]
            else:
                up_vol = -1  # can't move higher

            if down_vol > up_vol:
                move_lower = True
            else:
                move_upper = True

            if move_lower and lower_idx > 0:
                lower_idx -= 1
                cum_volume += volume_profile[lower_idx]
            elif move_upper and upper_idx < num_bins - 1:
                upper_idx += 1
                cum_volume += volume_profile[upper_idx]
            else:
                # We can't expand any further
                break

        vah_price = bin_centers[upper_idx]
        val_price = bin_centers[lower_idx]

        return (poc_price, vah_price, val_price)


    # ============================================================================
    # OTHER TECHNICAL INDICATORS
    # ============================================================================

    def compute_bollinger_bands(self, df: pd.DataFrame, window: int = 20, num_std: float = 2) -> pd.DataFrame:
        """
        Compute Bollinger Bands for the 'c' (close) column.
        Adds columns: 'bb_mid', 'bb_upper', 'bb_lower'
        """
        rolling_mean = df['c'].rolling(window).mean()
        rolling_std = df['c'].rolling(window).std()

        df['bb_mid'] = rolling_mean
        df['bb_upper'] = rolling_mean + (rolling_std * num_std)
        df['bb_lower'] = rolling_mean - (rolling_std * num_std)
        return df

    def compute_stochastic_oscillator(self, df: pd.DataFrame, k_window: int = 14, d_window: int = 3) -> pd.DataFrame:
        """
        Compute the Stochastic Oscillator (%K and %D).
        Adds columns: 'stoch_k', 'stoch_d'
        """
        low_min = df['l'].rolling(window=k_window).min()
        high_max = df['h'].rolling(window=k_window).max()

        df['stoch_k'] = ((df['c'] - low_min) / (high_max - low_min)) * 100
        df['stoch_d'] = df['stoch_k'].rolling(window=d_window).mean()
        return df

    def compute_atr(self, df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
        """
        Compute the Average True Range (ATR).
        Adds column: 'atr'
        """
        # True Range
        df['h-l'] = df['h'] - df['l']
        df['h-pc'] = abs(df['h'] - df['c'].shift(1))
        df['l-pc'] = abs(df['l'] - df['c'].shift(1))

        tr = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        df['atr'] = tr.rolling(window).mean()

        # Clean up intermediate columns if desired
        df.drop(['h-l','h-pc','l-pc'], axis=1, inplace=True)
        return df

    def compute_supertrend(self, df: pd.DataFrame, atr_multiplier: float = 3.0, atr_period: int = 10) -> pd.DataFrame:
        """
        Compute the Supertrend indicator.
        Adds columns: 'supertrend', 'supertrend_direction'
        """
        # First compute ATR if not present
        if 'atr' not in df.columns:
            df = self.compute_atr(df, atr_period)
        
        # Basic upper band & lower band
        hl2 = (df['h'] + df['l']) / 2
        df['basic_ub'] = hl2 + (atr_multiplier * df['atr'])
        df['basic_lb'] = hl2 - (atr_multiplier * df['atr'])

        # Initialize final bands
        df['final_ub'] = df['basic_ub']
        df['final_lb'] = df['basic_lb']

        for i in range(1, len(df)):
            # Final upper band
            if (df['basic_ub'].iloc[i] < df['final_ub'].iloc[i-1]) or (df['c'].iloc[i-1] > df['final_ub'].iloc[i-1]):
                df.at[i, 'final_ub'] = df['basic_ub'].iloc[i]
            else:
                df.at[i, 'final_ub'] = df['final_ub'].iloc[i-1]

            # Final lower band
            if (df['basic_lb'].iloc[i] > df['final_lb'].iloc[i-1]) or (df['c'].iloc[i-1] < df['final_lb'].iloc[i-1]):
                df.at[i, 'final_lb'] = df['basic_lb'].iloc[i]
            else:
                df.at[i, 'final_lb'] = df['final_lb'].iloc[i-1]

        # SuperTrend
        df['supertrend'] = 0.0
        df['supertrend_direction'] = 1

        for i in range(1, len(df)):
            if (df['c'].iloc[i] <= df['final_ub'].iloc[i]):
                df.at[i, 'supertrend'] = df['final_ub'].iloc[i]
                df.at[i, 'supertrend_direction'] = -1
            else:
                df.at[i, 'supertrend'] = df['final_lb'].iloc[i]
                df.at[i, 'supertrend_direction'] = 1
        
        # Optional: drop intermediate columns
        df.drop(['basic_ub','basic_lb','final_ub','final_lb'], axis=1, inplace=True)
        return df


    def compute_adx(self, df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
        """
        Compute the Average Directional Index (ADX).
        Adds columns: '+DI', '-DI', 'adx'.
        """
        # Ensure ATR is computed
        if 'atr' not in df.columns:
            df = self.compute_atr(df, window)

        # Directional movements
        df['up_move'] = df['h'] - df['h'].shift(1)
        df['down_move'] = df['l'].shift(1) - df['l']

        # +DM and -DM
        df['+DM'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0.0)
        df['-DM'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0.0)

        # Smooth +DM, -DM
        df['+DM_ema'] = df['+DM'].ewm(alpha=1/window, adjust=False).mean()
        df['-DM_ema'] = df['-DM'].ewm(alpha=1/window, adjust=False).mean()

        # +DI, -DI
        df['+DI'] = (df['+DM_ema'] / df['atr']) * 100
        df['-DI'] = (df['-DM_ema'] / df['atr']) * 100

        # DX
        df['dx'] = (abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI'])) * 100

        # ADX
        df['adx'] = df['dx'].ewm(alpha=1/window, adjust=False).mean()

        # Clean up intermediate columns if desired
        df.drop(['up_move','down_move','+DM','-DM','+DM_ema','-DM_ema','dx'], axis=1, inplace=True)
        return df

    def compute_trend(
        self,
        series: pd.Series, 
        threshold: float = 0.001, 
        window: int = 5
    ) -> str:
        """
        Compute the trend of the final `window` candles in a time series.
        
        Assumes the incoming `series` is in descending order (most recent value first).
        We flip the series to chronological order (oldest first, newest last), then 
        take only the last `window` points to compute a linear regression slope. 
        The slope is normalized by the mean of that subset to get a 'relative slope.'
        
        Returns one of: "increasing", "decreasing", or "flattening".
        """
        # Reverse so oldest is first, newest is last
        chronological_data = series.iloc[::-1].copy()
        
        # Slice only the last `window` points
        recent_subset = chronological_data.iloc[-window:]
        
        if len(recent_subset) < 2:
            return "flattening"
        
        x = np.arange(len(recent_subset))
        try:
            result = linregress(x, recent_subset.values)
            slope = result.slope
        except Exception:
            return "flattening"
        
        mean_val = recent_subset.mean()
        relative_slope = slope / mean_val if mean_val != 0 else 0
        
        # Classify trend by relative slope
        if relative_slope > threshold:
            return "increasing"
        elif relative_slope < -threshold:
            return "decreasing"
        else:
            return "flattening"

    def compute_angle(self, series: pd.Series) -> float:
        """
        Compute the angle (in degrees) for a descending-order Series,
        using linear regression over ALL the data in that subset.

        We'll invert the standard angle so that slope=0 => angle ~ 180°.
        
        Returns 0.0 if there's insufficient data or an error.
        """
        # Reverse so oldest is first, newest is last
        chronological_data = series.iloc[::-1].copy()
        
        if len(chronological_data) < 2:
            return 0.0
        
        x = np.arange(len(chronological_data))
        y = chronological_data.values
        
        try:
            result = linregress(x, y)
            slope = result.slope
        except Exception:
            return 0.0
        
        # Standard angle from x-axis
        angle_radians = math.atan(slope)  # slope=0 => 0 rad => 0 deg
        angle_degrees = math.degrees(angle_radians)
        
        # Adjust so that slope=0 => angle=180
        # Up slope => angle < 180, Down slope => angle > 180
        adjusted_angle = 180.0 - angle_degrees
        
        return adjusted_angle

    def add_bollinger_bands(
        self,
        df: pd.DataFrame,
        window: int = 20,
        num_std: float = 1.9,
        trend_points: int = 13
    ) -> pd.DataFrame:
        """
        Adds Bollinger bands (middle, upper, lower) to a DataFrame based on 'c' (close).
        Also computes the trend for the upper and lower bands using the last `trend_points` values,
        plus angles for upper, middle, lower bands (with a "flat => ~180°" orientation).
        
        Flags: candle_above_upper, candle_below_lower, candle_completely_above_upper,
            candle_partially_above_upper, candle_completely_below_lower,
            candle_partially_below_lower
        """
        # Sort ascending for rolling calculations
        df_sorted = df.copy().sort_values("ts", ascending=True).reset_index(drop=True)
        
        # Bollinger calculations
        df_sorted["middle_band"] = (
            df_sorted["c"].rolling(window=window, min_periods=window).mean()
        )
        df_sorted["std"] = (
            df_sorted["c"].rolling(window=window, min_periods=window).std()
        )
        df_sorted["upper_band"] = df_sorted["middle_band"] + (num_std * df_sorted["std"])
        df_sorted["lower_band"] = df_sorted["middle_band"] - (num_std * df_sorted["std"])
        
        # Merge back
        df = df.merge(
            df_sorted[["ts", "middle_band", "upper_band", "lower_band"]],
            on="ts",
            how="left"
        )
        
        # Sort descending so row 0 is the most recent
        df = df.sort_values("ts", ascending=False).reset_index(drop=True)
        
        # Initialize new columns
        df["upper_bb_trend"] = None
        df["lower_bb_trend"] = None
        df["upper_bb_angle"] = None
        df["middle_bb_angle"] = None
        df["lower_bb_angle"] = None
        
        if len(df) >= trend_points:
            # Subsets for the last `trend_points` rows (descending)
            subset_upper = df["upper_band"].head(trend_points)
            subset_lower = df["lower_band"].head(trend_points)
            subset_middle = df["middle_band"].head(trend_points)
            
            # Trends
            upper_trend = self.compute_trend(subset_upper, threshold=0.001, window=trend_points)
            lower_trend = self.compute_trend(subset_lower, threshold=0.001, window=trend_points)
            
            # Trend text
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
            
            # Angles (with "flat => 180°" orientation)
            df.at[0, "upper_bb_angle"] = self.compute_angle(subset_upper)
            df.at[0, "lower_bb_angle"] = self.compute_angle(subset_lower)
            df.at[0, "middle_bb_angle"] = self.compute_angle(subset_middle)
        else:
            # Not enough data
            df.at[0, "upper_bb_trend"] = "flattening"
            df.at[0, "lower_bb_trend"] = "flattening"
            # Could default angles to 180 or None if not enough data
            df.at[0, "upper_bb_angle"] = None
            df.at[0, "lower_bb_angle"] = None
            df.at[0, "middle_bb_angle"] = None
        
        # Basic candle flags
        if {"h", "l"}.issubset(df.columns):
            df["candle_above_upper"] = df["h"] > df["upper_band"]
            df["candle_below_lower"] = df["l"] < df["lower_band"]
        else:
            # Fallback if no high/low columns
            df["candle_above_upper"] = df["c"] > df["upper_band"]
            df["candle_below_lower"] = df["c"] < df["lower_band"]
        
        # Additional flags
        df["candle_completely_above_upper"] = False
        df["candle_partially_above_upper"] = False
        df["candle_completely_below_lower"] = False
        df["candle_partially_below_lower"] = False
        
        if {"h", "l"}.issubset(df.columns):
            df.loc[df["l"] > df["upper_band"], "candle_completely_above_upper"] = True
            df.loc[
                (df["h"] > df["upper_band"]) & (df["l"] <= df["upper_band"]),
                "candle_partially_above_upper"
            ] = True
            
            df.loc[df["h"] < df["lower_band"], "candle_completely_below_lower"] = True
            df.loc[
                (df["l"] < df["lower_band"]) & (df["h"] >= df["lower_band"]),
                "candle_partially_below_lower"
            ] = True
        else:
            df.loc[df["c"] > df["upper_band"], "candle_completely_above_upper"] = True
            df.loc[df["c"] < df["lower_band"], "candle_completely_below_lower"] = True
        
        return df



    def add_atr(self, df, window=14):
        """
        Adds Average True Range (ATR) to the dataframe.
        ATR is computed from the True Range (TR), where:
        TR = max[(high - low), abs(high - previous close), abs(low - previous close)]
        """
        high = df['h']
        low = df['l']
        close = df['c']
        df['prev_close'] = close.shift(1)
        df['tr'] = np.maximum(high - low,
                            np.maximum(abs(high - df['prev_close']),
                                        abs(low - df['prev_close'])))
        df['atr'] = df['tr'].rolling(window=window).mean()
        df.drop(columns=['prev_close', 'tr'], inplace=True)
        return df


    def add_stochastic_oscillator(self, df, window=14, smooth_window=3):
        """
        Adds the Stochastic Oscillator (%K and %D) to the dataframe.
        %K = 100 * (close - lowest low) / (highest high - lowest low)
        %D = Simple moving average of %K over 'smooth_window' periods.
        """
        df['lowest_low'] = df['l'].rolling(window=window).min()
        df['highest_high'] = df['h'].rolling(window=window).max()
        df['stoch_k'] = 100 * (df['c'] - df['lowest_low']) / (df['highest_high'] - df['lowest_low'])
        df['stoch_d'] = df['stoch_k'].rolling(window=smooth_window).mean()
        df.drop(columns=['lowest_low', 'highest_high'], inplace=True)
        return df


    def add_cci(self, df, window=20):
        """
        Adds the Commodity Channel Index (CCI) to the dataframe.
        Typical Price (TP) = (high + low + close) / 3.
        CCI = (TP - SMA(TP)) / (0.015 * Mean Deviation)
        """
        tp = (df['h'] + df['l'] + df['c']) / 3.0
        df['tp_ma'] = tp.rolling(window=window).mean()
        # Calculate Mean Absolute Deviation (MAD)
        df['tp_mad'] = tp.rolling(window=window).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
        df['cci'] = (tp - df['tp_ma']) / (0.015 * df['tp_mad'])
        df.drop(columns=['tp_ma', 'tp_mad'], inplace=True)
        return df
    def filter_regular_trading_hours(self, df: pd.DataFrame, tz='US/Eastern') -> pd.DataFrame:
        """
        Ensures 'ts' is a datetime in Eastern time, then filters out rows outside 09:30-16:00 Eastern.
        """

        if df.empty:
            return df

        # 1) Convert ts to a datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(df['ts']):
            df['ts'] = pd.to_datetime(df['ts'], errors='coerce')

        # 2) Drop rows where 'ts' did not parse
        df.dropna(subset=['ts'], inplace=True)
        if df.empty:
            return df

        # 3) If 'ts' is naive, localize to UTC first, or whichever zone your data is in originally.
        #    For example, if your raw timestamps represent seconds since epoch in UTC:
        if df['ts'].dt.tz is None:
            df['ts'] = df['ts'].dt.tz_localize('UTC')  # or 'UTC', or whichever your data truly represents

        # 4) Convert from that zone to Eastern
        df['ts'] = df['ts'].dt.tz_convert(tz)  # tz='US/Eastern'

        # 5) Filter by local time-of-day
        df['time_only'] = df['ts'].dt.time
        mask = (df['time_only'] >= time(9, 30)) & (df['time_only'] < time(16, 0))
        df = df[mask].copy()
        df.drop(columns=['time_only'], inplace=True)

        # 6) (Optional) remove timezone if your DB is storing naive datetime 
        #    (otherwise, you'll see 'YYYY-MM-DD HH:MM:SS-05:00' in your DB).
        df['ts'] = df['ts'].dt.tz_localize(None)

        return df


    def add_obv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds a standard On-Balance Volume (OBV) series to the DataFrame.

        OBV is computed as follows:
            1) Sort candles in ascending order by timestamp.
            2) Set obv[0] = 0 (arbitrary starting point).
            3) For each row i from 1..n-1:
                if close[i] > close[i-1]:  obv[i] = obv[i-1] + volume[i]
                if close[i] < close[i-1]:  obv[i] = obv[i-1] - volume[i]
                otherwise:                 obv[i] = obv[i-1]
            4) (Optional) re-sort back descending by timestamp if that is your project convention.

        Returns:
            DataFrame with a new column 'obv'.
        """

        # ── 1) Make a copy & sort ascending to ensure correct calculation ──────────────────────
        df = df.copy()
        df.sort_values("ts", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # ── 2) Initialize an array or list for OBV ─────────────────────────────────────────────
        obv_values = [0.0]  # Start from zero for the first row

        # ── 3) Loop through each row, compute OBV incrementally ───────────────────────────────
        for i in range(1, len(df)):
            current_close = df.loc[i, "c"]
            previous_close = df.loc[i - 1, "c"]
            current_volume = df.loc[i, "v"]
            last_obv = obv_values[-1]

            if current_close > previous_close:
                obv_values.append(last_obv + current_volume)
            elif current_close < previous_close:
                obv_values.append(last_obv - current_volume)
            else:
                # current_close == previous_close
                obv_values.append(last_obv)

        # ── 4) Assign OBV to new column ────────────────────────────────────────────────────────
        df["obv"] = obv_values

        # ── 5) (Optional) Re-sort descending if your system uses newest-first ─────────────────
        # df.sort_values("ts", ascending=False, inplace=True)
        # df.reset_index(drop=True, inplace=True)

        return df



    # ─── UTILITY: RETRY AIOHTTP REQUESTS ─────────────────────────────────────────
    async def fetch_with_retries(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict,
        retries: int = 3,
        delay: float = 1.0
    ) -> dict:
        """
        Fetch a URL with retries upon failure.
        """
        for attempt in range(retries):
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                logging.warning(
                    "Attempt %d/%d failed for URL %s: %s",
                    attempt + 1, retries, url, e
                )
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    raise

    # ─── NUMBA-OPTIMIZED FUNCTIONS ───────────────────────────────────────────────
    @njit
    def compute_wilders_rsi_numba(self, closes: np.ndarray, window: int) -> np.ndarray:
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

    def compute_wilders_rsi(self, df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
        """
        Computes Wilder's RSI on the 'c' (close) column of df.
        """
        if len(df) < window:
            df['rsi'] = np.nan
            return df
        closes = df['c'].to_numpy(dtype=np.float64)
        rsi_values = self.compute_wilders_rsi_numba(closes, window)
        df['rsi'] = rsi_values
        return df

    @njit
    def ema_njit(self, prices: np.ndarray, period: int) -> np.ndarray:
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
    def compute_macd_histogram(self, prices: np.ndarray) -> np.ndarray:
        """
        Compute the MACD histogram from closing prices using EMA periods of 12, 26, and 9.
        """
        fast = self.ema_njit(prices, 12)
        slow = self.ema_njit(prices, 26)
        macd_line = fast - slow
        signal = self.ema_njit(macd_line, 9)
        hist = macd_line - signal
        return hist

    @njit
    def determine_macd_curvature_code(self, prices: np.ndarray) -> int:
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
        hist = self.compute_macd_histogram(prices)
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

    def macd_curvature_label(self, prices: np.ndarray) -> str:
        """
        Returns a descriptive label for the MACD curvature.
        """
        code = self.determine_macd_curvature_code(prices)
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

    # ─── UPDATED TD SEQUENTIAL LOGIC ─────────────────────────────────────────────
    @njit
    def compute_td9_counts(self, closes: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
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

    def add_td9_counts(self, df: pd.DataFrame) -> pd.DataFrame:
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
        td_buy, td_sell = self.compute_td9_counts(closes)

        df['td_buy_count'] = td_buy
        df['td_sell_count'] = td_sell
        return df

    # ──────────────────────────────────────────────────────────────────────────────
    # BULLISH/BEARISH ENGULFING DETECTION
    # ──────────────────────────────────────────────────────────────────────────────
    def add_engulfing_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flags perfect bullish or bearish engulfing patterns.
        """
        df = df.copy()
        df['bullish_engulfing'] = False
        df['bearish_engulfing'] = False

        if len(df) < 2:
            return df

        # Sort ascending by timestamp for consistent logic
        df.sort_values("ts", inplace=True)
        df.reset_index(drop=True, inplace=True)

        for i in range(1, len(df)):
            # Previous candle
            pOpen = df.loc[i-1, 'o']
            pClose = df.loc[i-1, 'c']
            pHigh = df.loc[i-1, 'h']
            pLow = df.loc[i-1, 'l']

            # Current candle
            cOpen = df.loc[i, 'o']
            cClose = df.loc[i, 'c']
            cHigh = df.loc[i, 'h']
            cLow = df.loc[i, 'l']

            # Check for bullish engulfing
            if (pClose < pOpen and cClose > cOpen):
                if (cHigh > pHigh and cLow < pLow):
                    if (cOpen < pClose and cClose > pOpen):
                        df.loc[i, 'bullish_engulfing'] = True

            # Check for bearish engulfing
            if (pClose > pOpen and cClose < cOpen):
                if (cHigh > pHigh and cLow < pLow):
                    if (cOpen > pClose and cClose < pOpen):
                        df.loc[i, 'bearish_engulfing'] = True

        return df




    def add_volume_metrics(self, df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
        """
        Add various volume-based metrics to the DataFrame.
        
        Columns added:
        - volume_diff: difference in volume from the previous bar
        - volume_pct_change: (current_volume / previous_volume - 1) * 100
        - n_increasing_volume_streak: consecutive bars of increasing volume
        - n_decreasing_volume_streak: consecutive bars of decreasing volume
        - volume_ma_{window}: rolling average of volume over `window` bars
        - volume_zscore_{window}: Z-score of the current volume vs. rolling mean/std
        """

        # Ensure the DataFrame is sorted by ascending timestamp
        df = df.sort_values("ts").reset_index(drop=True)

        # 1) Volume Difference
        df["volume_diff"] = df["v"].diff().fillna(0)

        # 2) Volume % Change
        df["volume_pct_change"] = df["v"].pct_change().fillna(0) * 100

        # 3) Streaks of Increasing/Decreasing Volume
        n_increasing = [0] * len(df)
        n_decreasing = [0] * len(df)

        for i in range(1, len(df)):
            # If this bar's volume is higher than previous, increase the 'n_increasing' streak
            if df.loc[i, "v"] > df.loc[i - 1, "v"]:
                n_increasing[i] = n_increasing[i - 1] + 1
            else:
                n_increasing[i] = 0
            
            # If this bar's volume is lower than previous, increase the 'n_decreasing' streak
            if df.loc[i, "v"] < df.loc[i - 1, "v"]:
                n_decreasing[i] = n_decreasing[i - 1] + 1
            else:
                n_decreasing[i] = 0

        df["volume_increasing_streak"] = n_increasing
        df["volume_decreasing_streak"] = n_decreasing



        return df



    def generate_webull_headers(self):
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
            "access_token": "dc_us_tech1.1951b429494-d1e03590463e429b9337fde1f94d91c0",
            "app": "global",
            "app-group": "broker",
            "appid": "wb_web_app",
            "cache-control": "no-cache",
            "device-type": "Web",
            "did": "3uiar5zgvki16rgnpsfca4kyo4scy00a",
            "dnt": "1",
            "hl": "en",
            "origin": "https://app.webull.com",
            "os": "web",
            "osv": "i9zh",
            "platform": "web",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://app.webull.com/",
            "reqid": "kyiyrlq2kxig1vcwrdhcxvp3h5lc0_45",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Google Chrome\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "t_time": str(t_time_value),
            "tz": "America/Chicago",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "ver": "5.3.4",
            "x-s": x_s_value,
            "x-sv": "xodp2vg9"
        }

        return headers


