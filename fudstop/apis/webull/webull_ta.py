import re
import pandas as pd
import asyncio
import time
import ta.momentum
import ta.others
import ta.trend
import ta.volatility
import ta.volume
import talib
import httpx
import numpy as np
import ta


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

    async def get_candle_data(self, ticker, interval, headers, count:str='800'):
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
            base_fintech_gw_url = f'https://quotes-gw.webullfintech.com/api/quote/charts/query-mini?tickerId={tickerid}&type={interval}&count={count}&timestamp={timeStamp}&restorationType=1&extendTrading=1'

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


    async def ta_bollinger(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the ease_of_movement indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month

        ARGS:
        window: default 14
        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker, interval, headers=headers)
                
            boll_hband = ta.volatility.bollinger_hband(close=df['Close'].astype(float), fillna=True)
            boll_lband = ta.volatility.bollinger_lband(close=df['Close'].astype(float), fillna=True)
            boll_pband = ta.volatility.bollinger_pband(close=df['Close'].astype(float), fillna=True)
            boll_wband = ta.volatility.bollinger_wband(close=df['Close'].astype(float), fillna=True)
            boll_mavg = ta.volatility.bollinger_mavg(close=df['Close'].astype(float), fillna=True)
        

            df['boll_wband'] = boll_wband
            df['boll_hband'] = boll_hband
            df['boll_lband'] = boll_lband
            df['boll_pband'] = boll_pband
            df['boll_mavg'] = boll_mavg

            
            return df
        except Exception as e:
            print(e)


    async def ta_donchain(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the ease_of_movement indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker, interval, headers=headers)
                
            donchain_hband = ta.volatility.donchian_channel_hband(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            donchain_lband = ta.volatility.donchian_channel_lband(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            donchain_pband=ta.volatility.donchian_channel_pband(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            donchain_mband = ta.volatility.donchian_channel_mband(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            donchain_wband = ta.volatility.donchian_channel_wband(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)

            df['donchain_hband'] = donchain_hband
            df['donchain_lband'] = donchain_lband
            df['donchain_midband'] = donchain_mband
            df['donchain_pctband'] = donchain_pband
            df['donchain_wband'] = donchain_wband
            
            return df
        except Exception as e:
            print(e)

    async def ta_kelter_channel(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the ease_of_movement indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            kelter_hband = ta.volatility.keltner_channel_hband(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            kelter_lband = ta.volatility.keltner_channel_lband(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            kelter_mavg = ta.volatility.keltner_channel_mband(high=df['High'].astype(float), close=df['Close'].astype(float), low=df['Low'].astype(float), fillna=True)
            kelter_pband = ta.volatility.keltner_channel_pband(high=df['High'].astype(float), close=df['Close'].astype(float), low=df['Low'].astype(float), fillna=True)
            kelter_wband = ta.volatility.keltner_channel_wband(high=df['High'].astype(float), close=df['Close'].astype(float), low=df['Low'].astype(float), fillna=True)


            df['kelter_hband'] = kelter_hband
            df['kelter_lband'] = kelter_lband
            df['kelter_mavg'] = kelter_mavg
            df['kelter_pctband'] = kelter_pband
            df['kelter_wband'] = kelter_wband

            
            return df
        except Exception as e:
            print(e)


    async def ta_awesome_oscillator(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the ease_of_movement indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            awesome_oscillator = ta.momentum.awesome_oscillator(high=df['High'].astype(float), low=df['Low'].astype(float), fillna=True)

            df['awesome_oscillator'] = awesome_oscillator

            
            return df
        except Exception as e:
            print(e)



    async def ta_kama(self, headers, ticker:str, interval:str='m60'):
        """Moving average designed to account for market noise or volatility. KAMA will closely follow prices when the price swings are relatively small and the noise is low. KAMA will adjust when the price swings widen and follow prices from a greater distance. This trend-following indicator can be used to identify the overall trend, time turning points and filter price movements.
        
        
        
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
            

            kama = ta.momentum.kama(close=df['Close'].astype(float), fillna=True)


            df['kama'] = kama


            return df
        except Exception as e:
            print(e)


    async def ta_ppo(self, headers, ticker:str, interval:str='m60'):
        """The Percentage Price Oscillator (PPO) is a momentum oscillator that measures the difference between two moving averages as a percentage of the larger moving average.

        https://school.stockcharts.com/doku.php?id=technical_indicators:price_oscillators_ppo
                
        
        
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
            

            ppo = ta.momentum.ppo(df['Close'], fillna=True)

            ppo_hist = ta.momentum.ppo_hist(df['Close'].astype(float), fillna=True)

            ppo_signal = ta.momentum.ppo_signal(df['Close'].astype(float), fillna=True)


            df['ppo'] = ppo
            df['ppo_hist'] = ppo_hist
            df['ppo_signal'] = ppo_signal

            return df
        except Exception as e:
            print(e)


    async def ta_stoch(self, headers, ticker:str, interval:str='m60'):
        """Developed in the late 1950s by George Lane. The stochastic oscillator presents the location of the closing price of a stock in relation to the high and low range of the price of a stock over a period of time, typically a 14-day period.

        https://www.investopedia.com/terms/s/stochasticoscillator.asp
                        
                
        
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
            

            stoch = ta.momentum.stoch(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            stoch_signal = ta.momentum.stoch_signal(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)

            df['stoch'] = stoch
            df['stoch_signal'] = stoch_signal



            return df
        except Exception as e:
            print(e)


    async def ta_tsi(self, headers, ticker:str, interval:str='m60'):
        """Shows both trend direction and overbought/oversold conditions.

        https://en.wikipedia.org/wiki/True_strength_index
                                
                
        
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
            

            tsi = ta.momentum.tsi(close=df['Close'].astype(float), fillna=True)

            df['tsi'] = tsi

            return df
        except Exception as e:
            print(e)

    async def ta_williamsr(self, headers, ticker:str, interval:str='m60'):
        """Developed by Larry Williams, Williams %R is a momentum indicator that is the inverse of the Fast Stochastic Oscillator. Also referred to as %R, Williams %R reflects the level of the close relative to the highest high for the look-back period. In contrast, the Stochastic Oscillator reflects the level of the close relative to the lowest low. %R corrects for the inversion by multiplying the raw value by -100. As a result, the Fast Stochastic Oscillator and Williams %R produce the exact same lines, only the scaling is different. Williams %R oscillates from 0 to -100.

        Readings from 0 to -20 are considered overbought. Readings from -80 to -100 are considered oversold.

        Unsurprisingly, signals derived from the Stochastic Oscillator are also applicable to Williams %R.

        %R = (Highest High - Close)/(Highest High - Lowest Low) * -100

        Lowest Low = lowest low for the look-back period Highest High = highest high for the look-back period %R is multiplied by -100 correct the inversion and move the decimal.

        From: https://www.investopedia.com/terms/w/williamsr.asp The Williams %R oscillates from 0 to -100. When the indicator produces readings from 0 to -20, this indicates overbought market conditions. When readings are -80 to -100, it indicates oversold market conditions.
                                        
                
        
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
            

            williams_r = ta.momentum.williams_r(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)

            df['williams_r'] = williams_r

            return df
        except Exception as e:
            print(e)


    async def ta_macd(self, headers, ticker:str, interval:str='m60'):
        """
        
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
            
            macd= ta.trend.macd(close=df['Close'].astype(float), fillna=True)
            macd_diff = ta.trend.macd_diff(close=df['Close'].astype(float), fillna=True)
            macd_signal = ta.trend.macd_signal(close=df['Close'].astype(float), fillna=True)


            df['macd'] = macd
            df['macd_diff'] = macd_diff
            df['macd_signal'] = macd_signal

            return df
        except Exception as e:
            print(e)




    async def ta_vortex(self, headers, ticker:str, interval:str='m60'):
        """
        It consists of two oscillators that capture positive and negative trend movement. A bearish signal triggers when the negative trend indicator crosses above the positive trend indicator or a key level.


        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
            
            vortex_neg = ta.trend.vortex_indicator_neg(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            vortex_pos = ta.trend.vortex_indicator_pos(high=df['High'].astype(float), low=df['Low'].astype(float), close=df['Close'].astype(float), fillna=True)
            df['vortex_pos'] = vortex_pos
            df['vortex_neg'] = vortex_neg
            return df
        except Exception as e:
            print(e)


    async def ta_cumulative_return(self, headers, ticker:str, interval:str='m60'):
        """

        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
            df['High'] = df['High'].astype(float)
            df['Close'] = df['Close'].astype(float)
            cum = ta.others.cumulative_return(close=df['Close'], fillna=True)

            df['cum_return'] = cum
            return df
        except Exception as e:
            print(e)

    async def ta_aroon(self, ticker, timespan, headers, window:int=25):
        """
        Asynchronously calculate the Aroon Up and Aroon Down indicators, starting from the most recent candle,
        and scan for bullish or bearish signals based on extreme Aroon values.
        
        Parameters:
        df (DataFrame): DataFrame containing 'High', 'Low', and 'Timestamp' columns.
        period (int): The number of periods to look back for the highest high and lowest low.
        
        Returns:
        DataFrame: DataFrame with added 'Aroon_Up', 'Aroon_Down', and 'Signal' columns.
        """
        try:
            df = await self.get_candle_data(ticker=ticker, interval=timespan, headers=headers)

            aroon_down = ta.trend.aroon_down(df['High'],df['Low'], window=window, fillna=False)
            aroon_up = ta.trend.aroon_up(df['High'], df['Low'], window=window, fillna=True)
            
            df['aroon_up'] = aroon_up
            df['aroon_down'] = aroon_down

            return df
        except Exception as e:
            print(e)




    async def ta_stochrsi(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the stochrsi indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            stochrsi = ta.momentum.stochrsi(close=df['Close'].astype(float), fillna=True)
            stochrsi_d = ta.momentum.stochrsi_d(close=df['Close'].astype(float), fillna=True)
            stochrsi_k = ta.momentum.stochrsi_k(close=df['Close'].astype(float), fillna=True)


            df['stochrsi'] = stochrsi
            df['stochrsi_d'] = stochrsi_d
            df['stochrsi_k'] = stochrsi_k


            
            return df
        except Exception as e:
            print(e)


    async def ta_rate_of_change(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the rate of change indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            roc = ta.momentum.roc(close=df['Close'].astype(float), fillna=True)


            df['roc'] = roc



            
            return df
        except Exception as e:
            print(e)


    async def ta_ultimate_oscillator(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the ultimate oscillator indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            ultimate_oscillator = ta.momentum.ultimate_oscillator(close=df['Close'].astype(float),high=df['High'].astype(float),low = df['Low'].astype(float),  fillna=True)


            df['ultimate_oscillator'] = ultimate_oscillator



            
            return df
        except Exception as e:
            print(e)



    async def ta_adx(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the adx indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            adx = ta.trend.adx(close=df['Close'].astype(float),high=df['High'].astype(float),low = df['Low'].astype(float),  fillna=True)
            adx_neg = ta.trend.adx_neg(close=df['Close'].astype(float),high=df['High'].astype(float),low = df['Low'].astype(float),  fillna=True)
            adx_pos = ta.trend.adx_pos(close=df['Close'].astype(float),high=df['High'].astype(float),low = df['Low'].astype(float),  fillna=True)


            df['adx'] = adx
            df['adx_neg'] = adx_neg
            df['adx_pos'] = adx_pos


            
            return df
        except Exception as e:
            print(e)


    async def ta_cci(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the cci indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            cci = ta.trend.cci(close=df['Close'].astype(float),high=df['High'].astype(float),low = df['Low'].astype(float),  fillna=True)

            df['cci'] = cci



            
            return df
        except Exception as e:
            print(e)


    async def ta_dpo(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the dpo indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            dpo = ta.trend.dpo(close=df['Close'].astype(float),  fillna=True)

            df['dpo'] = dpo



            
            return df
        except Exception as e:
            print(e)



    async def ta_ichomoku(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the ta_ichomoku indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            ichimoku_a = ta.trend.ichimoku_a(high=df['High'].astype(float),low=df['Low'].astype(float),  fillna=True)
            ichimoku_b = ta.trend.ichimoku_b(high=df['High'].astype(float),low=df['Low'].astype(float),  fillna=True)
            ichimoku_baseline = ta.trend.ichimoku_base_line(high=df['High'].astype(float),low=df['Low'].astype(float),  fillna=True)
            ichimoku_conversionline = ta.trend.ichimoku_conversion_line(high=df['High'].astype(float),low=df['Low'].astype(float),  fillna=True)

            df['ichimoku_a'] = ichimoku_a
            df['ichimoku_b'] = ichimoku_b
            df['ichimoku_baseline'] = ichimoku_baseline
            df['ichimoku_conversionline'] = ichimoku_conversionline



            
            return df
        except Exception as e:
            print(e)



    async def ta_psar(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the psar indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            psar_down = ta.trend.psar_down(high=df['High'].astype(float),low=df['Low'].astype(float), close=df['Close'].astype(float),  fillna=True)
            psar_up = ta.trend.psar_up(high=df['High'].astype(float),low=df['Low'].astype(float), close=df['Close'].astype(float),  fillna=True)

            df['psar_down'] = psar_down
            df['psar_up'] = psar_up

            
            return df
        except Exception as e:
            print(e)



    async def ta_trix(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the trix indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            trix = ta.trend.trix(close=df['Close'].astype(float),  fillna=True)

            df['trix'] = trix
            
            return df
        except Exception as e:
            print(e)


    async def ta_daily_log_return(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the daily log return indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            daily_log_return = ta.others.daily_log_return(close=df['Close'].astype(float),  fillna=True)

            df['daily_log_return'] = daily_log_return
            
            return df
        except Exception as e:
            print(e)



    async def ta_pvo(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the pvo indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            pvo = ta.momentum.pvo(close=df['Volume'].astype(float),  fillna=True)
            pvo_hist = ta.momentum.pvo_hist(close=df['Volume'].astype(float),  fillna=True)
            pvo_signal = ta.momentum.pvo_signal(close=df['Volume'].astype(float),  fillna=True)

            df['pvo'] = pvo
            df['pvo_hist'] = pvo_hist
            df['pvo_signal'] = pvo_signal
            
            return df
        except Exception as e:
            print(e)


    async def ta_kst(self, ticker: str, interval: str, headers):
        """Gets a dataframe of the kst indicator. 
        INTERVALS:
        >>> m1 - 1 minute
        >>> m5 - 5 minute
        >>> m30 - 30 minute
        >>> m60 - 1 hour
        >>> m120 - 2 hour
        >>> m240 - 4 hour
        >>> d - day
        >>> w - week
        >>> m - month


        """
        try:
            # Get the main dataframe (e.g., price data)
            df = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)


            kst = ta.trend.kst(close=df['Close'].astype(float), fillna=True)
            kst_signal = ta.trend.kst_sig(close=df['Close'].astype(float), fillna=True)

            df['kst'] = kst
            df['kst_signal'] = kst_signal
            
            return df
        except Exception as e:
            print(e)


    def rank_adx_signal(self,adx, adx_neg, adx_pos):
        """
        Rank the ADX signal based on the values of ADX, adx_neg (-DI), and adx_pos (+DI).
        """
        if adx < 20:
            return "weak trend"
        if adx_neg > adx_pos:
            if adx >= 25:
                return "strong bearish"
            else:
                return "bearish"
        elif adx_pos > adx_neg:
            if adx >= 25:
                return "strong bullish"
            else:
                return "bullish"
        else:
            return "neutral"

    def rank_aroon_signal(self,aroon_up, aroon_down):
        """
        Rank the Aroon signal based on the values of aroon_up and aroon_down.
        """
        if aroon_up > 70 and aroon_down < 30:
            return "strong bullish"
        elif aroon_down > 70 and aroon_up < 30:
            return "strong bearish"
        elif aroon_up > 50 and aroon_down < 50:
            return "bullish"
        elif aroon_down > 50 and aroon_up < 50:
            return "bearish"
        else:
            return "neutral"

    def rank_donchian_signal(self,close_price, donchian_upper_band, donchian_lower_band):
        """
        Rank the Donchian Channel signal based on the close price and the Donchian bands.
        """
        if close_price > donchian_upper_band:
            return "bullish breakout"
        elif close_price < donchian_lower_band:
            return "bearish breakout"
        else:
            return "neutral"

    def rank_keltner_signal(self,close_price, keltner_upper_band, keltner_lower_band):
        """
        Rank the Keltner Channel signal based on the close price and the Keltner bands.
        """
        if close_price > keltner_upper_band:
            return "bullish breakout"
        elif close_price < keltner_lower_band:
            return "bearish breakout"
        else:
            return "neutral"

    def rank_ppo_signal(self,ppo, ppo_signal):
        """
        Rank the PPO signal based on PPO and its signal line.
        """
        if ppo > ppo_signal:
            return "bullish"
        elif ppo < ppo_signal:
            return "bearish"
        else:
            return "neutral"

    def rank_ichimoku_signal(self,close_price, conversion_line, base_line, span_a, span_b):
        """
        Rank the Ichimoku signal based on price and Ichimoku components.
        """
        if close_price > max(span_a, span_b):
            if conversion_line > base_line:
                return "strong bullish"
            else:
                return "bullish"
        elif close_price < min(span_a, span_b):
            if conversion_line < base_line:
                return "strong bearish"
            else:
                return "bearish"
        else:
            return "neutral"

    def rank_dpo_signal(self,dpo):
        """
        Rank the Detrended Price Oscillator (DPO) signal.
        """
        if dpo > 0:
            return "bullish"
        elif dpo < 0:
            return "bearish"
        else:
            return "neutral"

    def rank_ao_signal(self,ao):
        """
        Rank the Awesome Oscillator (AO) signal.
        """
        if ao > 0:
            return "bullish"
        elif ao < 0:
            return "bearish"
        else:
            return "neutral"

    def rank_kama_signal(self,close_price, kama):
        """
        Rank the Kaufman's Adaptive Moving Average (KAMA) signal.
        """
        if close_price > kama:
            return "bullish"
        elif close_price < kama:
            return "bearish"
        else:
            return "neutral"

    def rank_psar_signal(self,psar, close_price):
        """
        Rank the Parabolic SAR (PSAR) signal.
        """
        if psar < close_price:
            return "bullish"
        elif psar > close_price:
            return "bearish"
        else:
            return "neutral"

    def rank_tsi_signal(self,tsi):
        """
        Rank the True Strength Index (TSI) signal.
        """
        if tsi > 0:
            return "bullish"
        elif tsi < 0:
            return "bearish"
        else:
            return "neutral"

    def rank_trix_signal(self,trix):
        """
        Rank the TRIX indicator signal.
        """
        if trix > 0:
            return "bullish"
        elif trix < 0:
            return "bearish"
        else:
            return "neutral"

    def rank_bollinger_signal(self,close_price, upper_band, lower_band):
        """
        Rank the Bollinger Bands signal.
        """
        if close_price > upper_band:
            return "overbought"
        elif close_price < lower_band:
            return "oversold"
        else:
            return "neutral"

    def rank_cci_signal(self,cci):
        """
        Rank the Commodity Channel Index (CCI) signal.
        """
        if cci > 100:
            return "overbought"
        elif cci < -100:
            return "oversold"
        else:
            return "neutral"

    def rank_roc_signal(self,roc):
        """
        Rank the Rate of Change (ROC) signal.
        """
        if roc > 0:
            return "bullish"
        elif roc < 0:
            return "bearish"
        else:
            return "neutral"

    def rank_stochrsi_signal(self,k, d):
        """
        Rank the Stochastic RSI signal.
        """
        if k > 80 and d > 80:
            return "overbought"
        elif k < 20 and d < 20:
            return "oversold"
        else:
            return "neutral"

    def rank_stoch_signal(self,k, d):
        """
        Rank the Stochastic Oscillator signal.
        """
        if k > 80 and d > 80:
            return "overbought"
        elif k < 20 and d < 20:
            return "oversold"
        else:
            return "neutral"

    def rank_ultimate_oscillator_signal(self,uo):
        """
        Rank the Ultimate Oscillator signal.
        """
        if uo > 70:
            return "overbought"
        elif uo < 30:
            return "oversold"
        else:
            return "neutral"

    def rank_vortex_signal(self,vortex_pos, vortex_neg):
        """
        Rank the Vortex Indicator signal.
        """
        if vortex_pos > vortex_neg:
            return "bullish"
        elif vortex_neg > vortex_pos:
            return "bearish"
        else:
            return "neutral"

    async def get_ta_signals(self, ticker, interval, close_price, headers):
        try:
            # Fetch data

            # Create tasks for concurrent execution
            tasks = [
                self.ta_adx(ticker=ticker, interval=interval, headers=headers),
                self.ta_aroon(ticker=ticker, timespan=interval, headers=headers),
                self.ta_donchain(ticker=ticker, interval=interval, headers=headers),
                self.ta_kelter_channel(ticker=ticker, interval=interval, headers=headers),
                self.ta_ppo(ticker=ticker, interval=interval, headers=headers),
                self.ta_ichomoku(ticker=ticker, interval=interval, headers=headers),
                self.ta_dpo(ticker=ticker, interval=interval, headers=headers),
                self.ta_awesome_oscillator(ticker=ticker, interval=interval, headers=headers),
                self.ta_kama(ticker=ticker, interval=interval, headers=headers),
                self.ta_psar(ticker=ticker, interval=interval, headers=headers),
                self.ta_tsi(ticker=ticker, interval=interval, headers=headers),
                self.ta_trix(ticker=ticker, interval=interval, headers=headers),
                self.ta_bollinger(ticker=ticker, interval=interval, headers=headers),
                self.ta_cci(ticker=ticker, interval=interval, headers=headers),
                self.ta_rate_of_change(ticker=ticker, interval=interval, headers=headers),
                self.ta_stoch(ticker=ticker, interval=interval, headers=headers),
                self.ta_stochrsi(ticker=ticker, interval=interval, headers=headers),
                self.ta_vortex(ticker=ticker, interval=interval, headers=headers),
                self.ta_ultimate_oscillator(ticker=ticker, interval=interval, headers=headers),
            ]

            # Run tasks concurrently and gather results
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for exceptions in results
            processed_results = []
            for result in results:
                if isinstance(result, Exception):
        
                    processed_results.append(pd.DataFrame())  # Add empty DataFrame for failed tasks
                else:
                    processed_results.append(result)

            # Unpack results
            (
                adx,
                aroon,
                donchian,
                keltner,
                ppo,
                ichimoku,
                dpo,
                ao,
                kama,
                psar,
                tsi,
                trix,
                boll,
                cci,
                roc,
                stoch,
                stochrsi,
                vortex,
                ultimate,
            ) = processed_results

            # Safely extract latest data with fallback
            def get_latest_or_default(df, default=None):
                return df.iloc[-1] if not df.empty else default

            latest_adx = get_latest_or_default(adx, {})
            latest_aroon = get_latest_or_default(aroon, {})
            latest_donchian = get_latest_or_default(donchian, {})
            latest_keltner = get_latest_or_default(keltner, {})
            latest_ppo = get_latest_or_default(ppo, {})
            latest_ichimoku = get_latest_or_default(ichimoku, {})
            latest_dpo = get_latest_or_default(dpo, {})
            latest_ao = get_latest_or_default(ao, {})
            latest_kama = get_latest_or_default(kama, {})
            latest_tsi = get_latest_or_default(tsi, {})
            latest_trix = get_latest_or_default(trix, {})
            latest_boll = get_latest_or_default(boll, {})
            latest_cci = get_latest_or_default(cci, {})
            latest_roc = get_latest_or_default(roc, {})
            latest_stoch = get_latest_or_default(stoch, {})
            latest_stochrsi = get_latest_or_default(stochrsi, {})
            latest_vortex = get_latest_or_default(vortex, {})
            latest_ultimate = get_latest_or_default(ultimate, {})
            latest_psar = get_latest_or_default(psar, {})

            # Compute signals with safe fallbacks
            signals = {
                'adx': self.rank_adx_signal(latest_adx.get('adx', 0), latest_adx.get('adx_neg', 0), latest_adx.get('adx_pos', 0)),
                'aroon': self.rank_aroon_signal(latest_aroon.get('aroon_up', 0), latest_aroon.get('aroon_down', 0)),
                'donchain': self.rank_donchian_signal(close_price, latest_donchian.get('donchain_hband', 0), latest_donchian.get('donchain_lband', 0)),
                'keltner': self.rank_keltner_signal(close_price, latest_keltner.get('kelter_hband', 0), latest_keltner.get('kelter_lband', 0)),
                'ppo': self.rank_ppo_signal(latest_ppo.get('ppo', 0), latest_ppo.get('ppo_signal', 0)),
                'ichimoku': self.rank_ichimoku_signal(
                    close_price,
                    latest_ichimoku.get('ichimoku_conversionline', 0),
                    latest_ichimoku.get('ichimoku_baseline', 0),
                    latest_ichimoku.get('ichimoku_a', 0),
                    latest_ichimoku.get('ichimoku_b', 0),
                ),
                'dpo': self.rank_dpo_signal(latest_dpo.get('dpo', 0)),
                'awesome_oscillator': self.rank_ao_signal(latest_ao.get('awesome_oscillator', 0)),
                'kama': self.rank_kama_signal(close_price, latest_kama.get('kama', 0)),
                'tsi': self.rank_tsi_signal(latest_tsi.get('tsi', 0)),
                'trix': self.rank_trix_signal(latest_trix.get('trix', 0)),
                'bollinger': self.rank_bollinger_signal(close_price, latest_boll.get('boll_hband', 0), latest_boll.get('boll_lband', 0)),
                'cci': self.rank_cci_signal(latest_cci.get('cci', 0)),
                'roc': self.rank_roc_signal(latest_roc.get('roc', 0)),
                'stoch': self.rank_stoch_signal(latest_stoch.get('stoch', 0), latest_stoch.get('stoch_signal', 0)),
                'stochrsi': self.rank_stochrsi_signal(latest_stochrsi.get('stochrsi_k', 0), latest_stochrsi.get('stochrsi_d', 0)),
                'vortex': self.rank_vortex_signal(latest_vortex.get('vortex_pos', 0), latest_vortex.get('vortex_neg', 0)),
                'ultimate_scillator': self.rank_ultimate_oscillator_signal(latest_ultimate.get('ultimate_oscillator', 0)),
                'psar': self.rank_psar_signal(latest_psar.get('psar', 0)),
            }
            
            # Create DataFrame
            df = pd.DataFrame(signals)
            interval_dict = {'d': 'day', 'w': 'week', 'm': 'month'}
            df['ticker'] = ticker
            df['timespan'] = interval_dict.get(interval)

            return df
        except Exception as e:
            print(f"Error in get_ta_signals: {e}")
            return pd.DataFrame()



    async def get_all_signals_and_add_to_dataframe(self, headers, ticker, interval: str = 'd',):
        """
        Gets a dataframe of OHLC and volume data and computes all TA-Lib indicators,
        adding their latest values to the dataframe.

        ARGS:
            ticker: The stock ticker to get data for.
            interval: The timeframe to use. Choices are:
                    m1, m5, m60, m120, m240, d, w, m (defaults to 'd').

        RETURNS:
            A DataFrame with all the computed indicators added.
        """

        # Get candle data
        dataframe = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
        dataframe = dataframe.drop(columns=['Avg'], errors='ignore')  # Drop unnecessary column if present

        # Extract column data
        close_prices = dataframe['Close'].to_numpy(dtype=float)
        high_prices = dataframe['High'].to_numpy(dtype=float)
        low_prices = dataframe['Low'].to_numpy(dtype=float)
        open_prices = dataframe['Open'].to_numpy(dtype=float)
        volumes = dataframe['Volume'].to_numpy(dtype=float)

        # TA-Lib indicator categories
        indicators = {
            'Cycle Indicators': ['HT_DCPERIOD', 'HT_DCPHASE', 'HT_PHASOR', 'HT_SINE', 'HT_TRENDMODE'],
            'Math Operators': ['ADD', 'DIV', 'MAX', 'MAXINDEX', 'MIN', 'MININDEX', 'MINMAX', 'MINMAXINDEX', 'MULT', 'SUB', 'SUM'],
            'Momentum Indicators': ['ADX', 'ADXR', 'AROON', 'AROONOSC', 'BOP', 'CCI', 'CMO', 'DX', 'MACD', 'MACDEXT', 'MACDFIX', 'MFI', 'MINUS_DI', 'MINUS_DM', 'MOM', 'PLUS_DI', 'PLUS_DM', 'PPO', 'ROC', 'ROCP', 'ROCR', 'ROCR100', 'RSI', 'STOCH', 'STOCHF', 'STOCHRSI', 'TRIX', 'ULTOSC', 'WILLR'],
            'Overlap Studies': ['BBANDS', 'DEMA', 'EMA', 'HT_TRENDLINE', 'KAMA', 'MA', 'MAMA', 'MAVP', 'MIDPOINT', 'MIDPRICE', 'SAR', 'SAREXT', 'SMA', 'T3', 'TEMA', 'TRIMA', 'WMA'],
            'Pattern Recognition': ['CDL2CROWS', 'CDL3BLACKCROWS', 'CDL3INSIDE', 'CDL3LINESTRIKE', 'CDL3OUTSIDE', 'CDL3STARSINSOUTH', 'CDL3WHITESOLDIERS', 'CDLABANDONEDBABY', 'CDLADVANCEBLOCK', 'CDLBELTHOLD', 'CDLBREAKAWAY', 'CDLCLOSINGMARUBOZU', 'CDLCONCEALBABYSWALL', 'CDLCOUNTERATTACK', 'CDLDARKCLOUDCOVER', 'CDLDOJI', 'CDLDOJISTAR', 'CDLDRAGONFLYDOJI', 'CDLENGULFING', 'CDLEVENINGDOJISTAR', 'CDLEVENINGSTAR', 'CDLGAPSIDESIDEWHITE', 'CDLGRAVESTONEDOJI', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLHARAMI', 'CDLHARAMICROSS', 'CDLHIGHWAVE', 'CDLHIKKAKE', 'CDLHIKKAKEMOD', 'CDLHOMINGPIGEON', 'CDLIDENTICAL3CROWS', 'CDLINNECK', 'CDLINVERTEDHAMMER', 'CDLKICKING', 'CDLKICKINGBYLENGTH', 'CDLLADDERBOTTOM', 'CDLLONGLEGGEDDOJI', 'CDLLONGLINE', 'CDLMARUBOZU', 'CDLMATCHINGLOW', 'CDLMATHOLD', 'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR', 'CDLONNECK', 'CDLPIERCING', 'CDLRICKSHAWMAN', 'CDLRISEFALL3METHODS', 'CDLSEPARATINGLINES', 'CDLSHOOTINGSTAR', 'CDLSHORTLINE', 'CDLSPINNINGTOP', 'CDLSTALLEDPATTERN', 'CDLSTICKSANDWICH', 'CDLTAKURI', 'CDLTASUKIGAP', 'CDLTHRUSTING', 'CDLTRISTAR', 'CDLUNIQUE3RIVER', 'CDLUPSIDEGAP2CROWS', 'CDLXSIDEGAP3METHODS'],
            'Price Transform': ['AVGPRICE', 'MEDPRICE', 'TYPPRICE', 'WCLPRICE'],
            'Statistic Functions': ['BETA', 'CORREL', 'LINEARREG', 'LINEARREG_ANGLE', 'LINEARREG_INTERCEPT', 'LINEARREG_SLOPE', 'STDDEV', 'TSF', 'VAR'],
            'Volatility Indicators': ['ATR', 'NATR', 'TRANGE'],
            'Volume Indicators': ['AD', 'ADOSC', 'OBV']
        }

        for category, indicator_list in indicators.items():
            for indicator in indicator_list:
                try:
                    func = getattr(talib, indicator)

                    # Momentum Indicators
                    if indicator in ['ADX', 'ADXR']:
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    elif indicator == 'APO':
                        dataframe[indicator] = func(close_prices, fastperiod=12, slowperiod=26, matype=0)
                    elif indicator == 'AROON':
                        aroondown, aroonup = func(high_prices, low_prices, timeperiod=14)
                        dataframe[f"{indicator}_down"] = aroondown
                        dataframe[f"{indicator}_up"] = aroonup
                    elif indicator == 'AROONOSC':
                        dataframe[indicator] = func(high_prices, low_prices, timeperiod=14)
                    elif indicator == 'BOP':
                        dataframe[indicator] = func(open_prices, high_prices, low_prices, close_prices)
                    elif indicator == 'CCI':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    elif indicator == 'CMO':
                        dataframe[indicator] = func(close_prices, timeperiod=14)
                    elif indicator == 'DX':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    elif indicator in ['MACD', 'MACDEXT', 'MACDFIX']:
                        if indicator == 'MACD':
                            macd, macdsignal, macdhist = func(close_prices, fastperiod=12, slowperiod=26, signalperiod=9)
                        elif indicator == 'MACDEXT':
                            macd, macdsignal, macdhist = func(close_prices, fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0)
                        else:  # MACDFIX
                            macd, macdsignal, macdhist = func(close_prices, signalperiod=9)
                        dataframe[f"{indicator}_macd"] = macd
                        dataframe[f"{indicator}_signal"] = macdsignal
                        dataframe[f"{indicator}_hist"] = macdhist
                    elif indicator == 'MFI':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, volumes, timeperiod=14)
                    elif indicator in ['MINUS_DI', 'PLUS_DI']:
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    elif indicator in ['MINUS_DM', 'PLUS_DM']:
                        dataframe[indicator] = func(high_prices, low_prices, timeperiod=14)
                    elif indicator == 'MOM':
                        dataframe[indicator] = func(close_prices, timeperiod=10)
                    elif indicator == 'PPO':
                        dataframe[indicator] = func(close_prices, fastperiod=12, slowperiod=26, matype=0)
                    elif indicator in ['ROC', 'ROCP', 'ROCR', 'ROCR100']:
                        dataframe[indicator] = func(close_prices, timeperiod=10)
                    elif indicator == 'RSI':
                        dataframe[indicator] = func(close_prices, timeperiod=14)
                    elif indicator == 'STOCH':
                        slowk, slowd = func(high_prices, low_prices, close_prices, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
                        dataframe[f"{indicator}_slowk"] = slowk
                        dataframe[f"{indicator}_slowd"] = slowd
                    elif indicator == 'STOCHF':
                        fastk, fastd = func(high_prices, low_prices, close_prices, fastk_period=5, fastd_period=3, fastd_matype=0)
                        dataframe[f"{indicator}_fastk"] = fastk
                        dataframe[f"{indicator}_fastd"] = fastd
                    elif indicator == 'STOCHRSI':
                        fastk, fastd = func(close_prices, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
                        dataframe[f"{indicator}_fastk"] = fastk
                        dataframe[f"{indicator}_fastd"] = fastd
                    elif indicator == 'TRIX':
                        dataframe[indicator] = func(close_prices, timeperiod=30)
                    elif indicator == 'ULTOSC':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, timeperiod1=7, timeperiod2=14, timeperiod3=28)
                    elif indicator == 'WILLR':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    # Overlap Studies
                    elif indicator == 'BBANDS':
                        upperband, middleband, lowerband = func(close_prices, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
                        dataframe[f"{indicator}_upper"] = upperband
                        dataframe[f"{indicator}_middle"] = middleband
                        dataframe[f"{indicator}_lower"] = lowerband
                    elif indicator == 'DEMA':
                        dataframe[indicator] = func(close_prices, timeperiod=30)
                    elif indicator == 'EMA':
                        dataframe[indicator] = func(close_prices, timeperiod=30)
                    elif indicator == 'HT_TRENDLINE':
                        dataframe[indicator] = func(close_prices)
                    elif indicator == 'KAMA':
                        dataframe[indicator] = func(close_prices, timeperiod=30)
                    elif indicator == 'MA':
                        dataframe[indicator] = func(close_prices, timeperiod=30, matype=0)
                    elif indicator == 'MAMA':
                        mama, fama = func(close_prices, fastlimit=0, slowlimit=0)
                        dataframe[f"{indicator}_mama"] = mama
                        dataframe[f"{indicator}_fama"] = fama
                    elif indicator == 'MAVP':
                        periods = np.linspace(2, 30, len(close_prices))  # Example: create variable periods
                        dataframe[indicator] = func(close_prices, periods, minperiod=2, maxperiod=30, matype=0)
                    elif indicator == 'MIDPOINT':
                        dataframe[indicator] = func(close_prices, timeperiod=14)
                    elif indicator == 'MIDPRICE':
                        dataframe[indicator] = func(high_prices, low_prices, timeperiod=14)
                    elif indicator == 'SAR':
                        dataframe[indicator] = func(high_prices, low_prices, acceleration=0, maximum=0)
                    elif indicator == 'SAREXT':
                        dataframe[indicator] = func(
                            high_prices, 
                            low_prices, 
                            startvalue=0, 
                            offsetonreverse=0, 
                            accelerationinitlong=0, 
                            accelerationlong=0, 
                            accelerationmaxlong=0, 
                            accelerationinitshort=0, 
                            accelerationshort=0, 
                            accelerationmaxshort=0
                        )
                    elif indicator == 'SMA':
                        dataframe[indicator] = func(close_prices, timeperiod=30)
                    elif indicator == 'T3':
                        dataframe[indicator] = func(close_prices, timeperiod=5, vfactor=0)
                    elif indicator == 'TEMA':
                        dataframe[indicator] = func(close_prices, timeperiod=30)
                    elif indicator == 'TRIMA':
                        dataframe[indicator] = func(close_prices, timeperiod=30)
                    elif indicator == 'WMA':
                        dataframe[indicator] = func(close_prices, timeperiod=30)
                    # Volume Indicators
                    elif indicator == 'AD':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, volumes)
                    elif indicator == 'ADOSC':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, volumes, fastperiod=3, slowperiod=10)
                    elif indicator == 'OBV':
                        dataframe[indicator] = func(close_prices, volumes)
                    # Volatility Indicators
                    elif indicator == 'ATR':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    elif indicator == 'NATR':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    elif indicator == 'TRANGE':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices)

                    # Price Transform Indicators
                    elif indicator == 'AVGPRICE':
                        dataframe[indicator] = func(open_prices, high_prices, low_prices, close_prices)
                    elif indicator == 'MEDPRICE':
                        dataframe[indicator] = func(high_prices, low_prices)
                    elif indicator == 'TYPPRICE':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices)
                    elif indicator == 'WCLPRICE':
                        dataframe[indicator] = func(high_prices, low_prices, close_prices)

                    # Cycle Indicator Functions
                    elif indicator == 'HT_DCPERIOD':
                        dataframe[indicator] = func(close_prices)
                    elif indicator == 'HT_DCPHASE':
                        dataframe[indicator] = func(close_prices)
                    elif indicator == 'HT_PHASOR':
                        inphase, quadrature = func(close_prices)
                        dataframe[f"{indicator}_inphase"] = inphase
                        dataframe[f"{indicator}_quadrature"] = quadrature
                    elif indicator == 'HT_SINE':
                        sine, leadsine = func(close_prices)
                        dataframe[f"{indicator}_sine"] = sine
                        dataframe[f"{indicator}_leadsine"] = leadsine
                    elif indicator == 'HT_TRENDMODE':
                        dataframe[indicator] = func(close_prices)


                    # Pattern Recognition Functions
                    pattern_indicators = [
                        'CDL2CROWS', 'CDL3BLACKCROWS', 'CDL3INSIDE', 'CDL3LINESTRIKE', 'CDL3OUTSIDE', 'CDL3STARSINSOUTH',
                        'CDL3WHITESOLDIERS', 'CDLABANDONEDBABY', 'CDLADVANCEBLOCK', 'CDLBELTHOLD', 'CDLBREAKAWAY',
                        'CDLCLOSINGMARUBOZU', 'CDLCONCEALBABYSWALL', 'CDLCOUNTERATTACK', 'CDLDARKCLOUDCOVER', 'CDLDOJI',
                        'CDLDOJISTAR', 'CDLDRAGONFLYDOJI', 'CDLENGULFING', 'CDLEVENINGDOJISTAR', 'CDLEVENINGSTAR',
                        'CDLGAPSIDESIDEWHITE', 'CDLGRAVESTONEDOJI', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLHARAMI',
                        'CDLHARAMICROSS', 'CDLHIGHWAVE', 'CDLHIKKAKE', 'CDLHIKKAKEMOD', 'CDLHOMINGPIGEON',
                        'CDLIDENTICAL3CROWS', 'CDLINNECK', 'CDLINVERTEDHAMMER', 'CDLKICKING', 'CDLKICKINGBYLENGTH',
                        'CDLLADDERBOTTOM', 'CDLLONGLEGGEDDOJI', 'CDLLONGLINE', 'CDLMARUBOZU', 'CDLMATCHINGLOW',
                        'CDLMATHOLD', 'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR', 'CDLONNECK', 'CDLPIERCING',
                        'CDLRICKSHAWMAN', 'CDLRISEFALL3METHODS', 'CDLSEPARATINGLINES', 'CDLSHOOTINGSTAR',
                        'CDLSHORTLINE', 'CDLSPINNINGTOP', 'CDLSTALLEDPATTERN', 'CDLSTICKSANDWICH', 'CDLTAKURI',
                        'CDLTASUKIGAP', 'CDLTHRUSTING', 'CDLTRISTAR', 'CDLUNIQUE3RIVER', 'CDLUPSIDEGAP2CROWS',
                        'CDLXSIDEGAP3METHODS'
                    ]

                    for indicator in pattern_indicators:
                        try:
                            func = getattr(talib, indicator)

                            # Handle special case for functions with additional arguments like penetration
                            if indicator in ['CDLABANDONEDBABY', 'CDLDARKCLOUDCOVER', 'CDLEVENINGDOJISTAR', 
                                            'CDLEVENINGSTAR', 'CDLMATHOLD', 'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR']:
                                dataframe[indicator] = func(open_prices, high_prices, low_prices, close_prices, penetration=0)
                            else:
                                dataframe[indicator] = func(open_prices, high_prices, low_prices, close_prices)

                        except Exception as e:
                            print(f"Error with indicator {indicator}: {e}")

                except Exception as e:
                    print(f"Error with indicator {indicator}: {e}")


        # Statistic Functions
        statistic_indicators = [
            'BETA', 'CORREL', 'LINEARREG', 'LINEARREG_ANGLE', 'LINEARREG_INTERCEPT', 
            'LINEARREG_SLOPE', 'STDDEV', 'TSF', 'VAR'
        ]

        for indicator in statistic_indicators:
            try:
                func = getattr(talib, indicator)

                if indicator == 'BETA':
                    dataframe[indicator] = func(high_prices, low_prices, timeperiod=5)
                elif indicator == 'CORREL':
                    dataframe[indicator] = func(high_prices, low_prices, timeperiod=30)
                elif indicator in ['LINEARREG', 'LINEARREG_ANGLE', 'LINEARREG_INTERCEPT', 'LINEARREG_SLOPE', 'TSF']:
                    dataframe[indicator] = func(close_prices, timeperiod=14)
                elif indicator == 'STDDEV':
                    dataframe[indicator] = func(close_prices, timeperiod=5, nbdev=1)
                elif indicator == 'VAR':
                    dataframe[indicator] = func(close_prices, timeperiod=5, nbdev=1)

            except Exception as e:
                print(f"Error with indicator {indicator}: {e}")

        # Adding Math Transform Functions
        math_transform_indicators = [
            'ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'COSH', 'EXP', 'FLOOR', 'LN', 'LOG10', 
            'SIN', 'SINH', 'SQRT', 'TAN', 'TANH'
        ]

        for indicator in math_transform_indicators:
            try:
                func = getattr(talib, indicator)
                # Apply the function to close prices
                dataframe[indicator] = func(close_prices)
            except Exception as e:
                print(f"Error with Math Transform Function {indicator}: {e}")


        # Adding Math Operator Functions
        math_operator_indicators = [
            'ADD', 'DIV', 'MAX', 'MAXINDEX', 'MIN', 'MININDEX', 'MINMAX', 
            'MINMAXINDEX', 'MULT', 'SUB', 'SUM'
        ]

        for indicator in math_operator_indicators:
            try:
                func = getattr(talib, indicator)
                # Handle each case based on the required input and output
                if indicator in ['ADD', 'DIV', 'MULT', 'SUB']:  # Two-input operations
                    dataframe[indicator] = func(close_prices, open_prices)  # Example: use close_prices and open_prices
                elif indicator in ['MAX', 'MIN', 'SUM']:  # Single input with timeperiod
                    dataframe[indicator] = func(close_prices, timeperiod=30)
                elif indicator in ['MAXINDEX', 'MININDEX']:  # Index functions
                    dataframe[indicator] = func(close_prices, timeperiod=30)
                elif indicator == 'MINMAX':  # Two-output function
                    min_vals, max_vals = func(close_prices, timeperiod=30)
                    dataframe[f"{indicator}_min"] = min_vals
                    dataframe[f"{indicator}_max"] = max_vals
                elif indicator == 'MINMAXINDEX':  # Two-output index function
                    min_idx, max_idx = func(close_prices, timeperiod=30)
                    dataframe[f"{indicator}_min_idx"] = min_idx
                    dataframe[f"{indicator}_max_idx"] = max_idx
            except Exception as e:
                print(f"Error with Math Operator Function {indicator}: {e}")
        return dataframe
    





    async def get_all_signals_and_add_to_dataframe(self, headers, ticker, interval: str = 'd'):
        """
        Gets a dataframe of OHLC and volume data and computes all TA-Lib indicators,
        adding their latest values to the dataframe.

        ARGS:
            ticker: The stock ticker to get data for.
            interval: The timeframe to use. Choices are:
                    m1, m5, m60, m120, m240, d, w, m (defaults to 'd').

        RETURNS:
            A DataFrame with all the computed indicators added.
        """

        # Get candle data
        dataframe = await self.get_candle_data(ticker=ticker, interval=interval, headers=headers)
        dataframe = dataframe.drop(columns=['Avg'], errors='ignore')  # Drop unnecessary column if present

        # Ensure columns are lowercase
        dataframe.columns = [col.lower() for col in dataframe.columns]

        # Extract column data
        close_prices = dataframe['close'].to_numpy(dtype=float)
        high_prices = dataframe['high'].to_numpy(dtype=float)
        low_prices = dataframe['low'].to_numpy(dtype=float)
        open_prices = dataframe['open'].to_numpy(dtype=float)
        volumes = dataframe['volume'].to_numpy(dtype=float)

        # TA-Lib indicator categories
        indicators = {
            'Cycle Indicators': ['HT_DCPERIOD', 'HT_DCPHASE', 'HT_PHASOR', 'HT_SINE', 'HT_TRENDMODE'],
            'Math Operators': ['ADD', 'DIV', 'MAX', 'MAXINDEX', 'MIN', 'MININDEX', 'MINMAX', 'MINMAXINDEX', 'MULT', 'SUB', 'SUM'],
            'Momentum Indicators': ['ADX', 'ADXR', 'APO', 'AROON', 'AROONOSC', 'BOP', 'CCI', 'CMO', 'DX', 'MACD', 'MACDEXT', 'MACDFIX', 'MFI', 'MINUS_DI', 'MINUS_DM', 'MOM', 'PLUS_DI', 'PLUS_DM', 'PPO', 'ROC', 'ROCP', 'ROCR', 'ROCR100', 'RSI', 'STOCH', 'STOCHF', 'STOCHRSI', 'TRIX', 'ULTOSC', 'WILLR'],
            'Overlap Studies': ['BBANDS', 'DEMA', 'EMA', 'HT_TRENDLINE', 'KAMA', 'MA', 'MAMA', 'MAVP', 'MIDPOINT', 'MIDPRICE', 'SAR', 'SAREXT', 'SMA', 'T3', 'TEMA', 'TRIMA', 'WMA'],
            'Pattern Recognition': ['CDL2CROWS', 'CDL3BLACKCROWS', 'CDL3INSIDE', 'CDL3LINESTRIKE', 'CDL3OUTSIDE', 'CDL3STARSINSOUTH', 'CDL3WHITESOLDIERS', 'CDLABANDONEDBABY', 'CDLADVANCEBLOCK', 'CDLBELTHOLD', 'CDLBREAKAWAY', 'CDLCLOSINGMARUBOZU', 'CDLCONCEALBABYSWALL', 'CDLCOUNTERATTACK', 'CDLDARKCLOUDCOVER', 'CDLDOJI', 'CDLDOJISTAR', 'CDLDRAGONFLYDOJI', 'CDLENGULFING', 'CDLEVENINGDOJISTAR', 'CDLEVENINGSTAR', 'CDLGAPSIDESIDEWHITE', 'CDLGRAVESTONEDOJI', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLHARAMI', 'CDLHARAMICROSS', 'CDLHIGHWAVE', 'CDLHIKKAKE', 'CDLHIKKAKEMOD', 'CDLHOMINGPIGEON', 'CDLIDENTICAL3CROWS', 'CDLINNECK', 'CDLINVERTEDHAMMER', 'CDLKICKING', 'CDLKICKINGBYLENGTH', 'CDLLADDERBOTTOM', 'CDLLONGLEGGEDDOJI', 'CDLLONGLINE', 'CDLMARUBOZU', 'CDLMATCHINGLOW', 'CDLMATHOLD', 'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR', 'CDLONNECK', 'CDLPIERCING', 'CDLRICKSHAWMAN', 'CDLRISEFALL3METHODS', 'CDLSEPARATINGLINES', 'CDLSHOOTINGSTAR', 'CDLSHORTLINE', 'CDLSPINNINGTOP', 'CDLSTALLEDPATTERN', 'CDLSTICKSANDWICH', 'CDLTAKURI', 'CDLTASUKIGAP', 'CDLTHRUSTING', 'CDLTRISTAR', 'CDLUNIQUE3RIVER', 'CDLUPSIDEGAP2CROWS', 'CDLXSIDEGAP3METHODS'],
            'Price Transform': ['AVGPRICE', 'MEDPRICE', 'TYPPRICE', 'WCLPRICE'],
            'Statistic Functions': ['BETA', 'CORREL', 'LINEARREG', 'LINEARREG_ANGLE', 'LINEARREG_INTERCEPT', 'LINEARREG_SLOPE', 'STDDEV', 'TSF', 'VAR'],
            'Volatility Indicators': ['ATR', 'NATR', 'TRANGE'],
            'Volume Indicators': ['AD', 'ADOSC', 'OBV'],
            'Math Transform': ['ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'COSH', 'EXP', 'FLOOR', 'LN', 'LOG10', 'SIN', 'SINH', 'SQRT', 'TAN', 'TANH']
        }

        for category, indicator_list in indicators.items():
            for indicator in indicator_list:
                try:
                    func = getattr(talib, indicator)

                    # Handle indicator computations based on their required parameters
                    if indicator in ['ADX', 'ADXR', 'DX', 'MINUS_DI', 'PLUS_DI', 'MINUS_DM', 'PLUS_DM']:
                        dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    elif indicator in ['APO']:
                        dataframe[indicator.lower()] = func(close_prices, fastperiod=12, slowperiod=26, matype=0)
                    elif indicator == 'AROON':
                        aroondown, aroonup = func(high_prices, low_prices, timeperiod=14)
                        dataframe['aroon_down'] = aroondown
                        dataframe['aroon_up'] = aroonup
                    elif indicator == 'AROONOSC':
                        dataframe[indicator.lower()] = func(high_prices, low_prices, timeperiod=14)
                    elif indicator == 'BOP':
                        dataframe[indicator.lower()] = func(open_prices, high_prices, low_prices, close_prices)
                    elif indicator == 'CCI':
                        dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    elif indicator == 'CMO':
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=14)
                    elif indicator in ['MACD', 'MACDEXT', 'MACDFIX']:
                        if indicator == 'MACD':
                            macd, macdsignal, macdhist = func(close_prices, fastperiod=12, slowperiod=26, signalperiod=9)
                        elif indicator == 'MACDEXT':
                            macd, macdsignal, macdhist = func(close_prices, fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0)
                        else:  # MACDFIX
                            macd, macdsignal, macdhist = func(close_prices, signalperiod=9)
                        dataframe[f"{indicator.lower()}_macd"] = macd
                        dataframe[f"{indicator.lower()}_signal"] = macdsignal
                        dataframe[f"{indicator.lower()}_hist"] = macdhist
                    elif indicator == 'MFI':
                        dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices, volumes, timeperiod=14)
                    elif indicator == 'MOM':
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=10)
                    elif indicator == 'PPO':
                        dataframe[indicator.lower()] = func(close_prices, fastperiod=12, slowperiod=26, matype=0)
                    elif indicator in ['ROC', 'ROCP', 'ROCR', 'ROCR100']:
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=10)
                    elif indicator == 'RSI':
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=14)
                    elif indicator == 'STOCH':
                        slowk, slowd = func(high_prices, low_prices, close_prices)
                        dataframe['slowk'] = slowk
                        dataframe['slowd'] = slowd
                    elif indicator == 'STOCHF':
                        fastk, fastd = func(high_prices, low_prices, close_prices)
                        dataframe['fastk'] = fastk
                        dataframe['fastd'] = fastd
                    elif indicator == 'STOCHRSI':
                        fastk, fastd = func(close_prices)
                        dataframe['stochrsi_fastk'] = fastk
                        dataframe['stochrsi_fastd'] = fastd
                    elif indicator == 'TRIX':
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=30)
                    elif indicator == 'ULTOSC':
                        dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices, timeperiod1=7, timeperiod2=14, timeperiod3=28)
                    elif indicator == 'WILLR':
                        dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices, timeperiod=14)
                    # Overlap Studies
                    elif indicator == 'BBANDS':
                        upperband, middleband, lowerband = func(close_prices, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
                        dataframe[f"{indicator.lower()}_upper"] = upperband
                        dataframe[f"{indicator.lower()}_middle"] = middleband
                        dataframe[f"{indicator.lower()}_lower"] = lowerband
                    elif indicator in ['DEMA', 'EMA', 'KAMA', 'MA', 'MIDPOINT', 'SMA', 'T3', 'TEMA', 'TRIMA', 'WMA']:
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=30)
                    elif indicator == 'HT_TRENDLINE':
                        dataframe[indicator.lower()] = func(close_prices)
                    elif indicator == 'MAMA':
                        mama, fama = func(close_prices, fastlimit=0.5, slowlimit=0.05)
                        dataframe[f"{indicator.lower()}_mama"] = mama
                        dataframe[f"{indicator.lower()}_fama"] = fama
                    elif indicator == 'MAVP':
                        periods = np.linspace(2, 30, len(close_prices))
                        dataframe[indicator.lower()] = func(close_prices, periods)
                    elif indicator == 'MIDPRICE':
                        dataframe[indicator.lower()] = func(high_prices, low_prices, timeperiod=14)
                    elif indicator in ['SAR', 'SAREXT']:
                        dataframe[indicator.lower()] = func(high_prices, low_prices)
                    # Volume Indicators
                    elif indicator in ['AD', 'ADOSC', 'OBV']:
                        if indicator == 'AD':
                            dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices, volumes)
                        elif indicator == 'ADOSC':
                            dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices, volumes, fastperiod=3, slowperiod=10)
                        elif indicator == 'OBV':
                            dataframe[indicator.lower()] = func(close_prices, volumes)
                    # Volatility Indicators
                    elif indicator in ['ATR', 'NATR', 'TRANGE']:
                        dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices)
                    # Price Transform Indicators
                    elif indicator == 'AVGPRICE':
                        dataframe[indicator.lower()] = func(open_prices, high_prices, low_prices, close_prices)
                    elif indicator == 'MEDPRICE':
                        dataframe[indicator.lower()] = func(high_prices, low_prices)
                    elif indicator in ['TYPPRICE', 'WCLPRICE']:
                        dataframe[indicator.lower()] = func(high_prices, low_prices, close_prices)
                    # Cycle Indicator Functions
                    elif indicator in ['HT_DCPERIOD', 'HT_DCPHASE', 'HT_TRENDMODE']:
                        dataframe[indicator.lower()] = func(close_prices)
                    elif indicator == 'HT_PHASOR':
                        inphase, quadrature = func(close_prices)
                        dataframe['ht_phasor_inphase'] = inphase
                        dataframe['ht_phasor_quadrature'] = quadrature
                    elif indicator == 'HT_SINE':
                        sine, leadsine = func(close_prices)
                        dataframe['ht_sine'] = sine
                        dataframe['ht_leadsine'] = leadsine
                    # Pattern Recognition Functions
                    elif indicator.startswith('CDL'):
                        # Handle special cases with penetration
                        if indicator in ['CDLABANDONEDBABY', 'CDLDARKCLOUDCOVER', 'CDLEVENINGDOJISTAR', 
                                        'CDLEVENINGSTAR', 'CDLMATHOLD', 'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR']:
                            dataframe[indicator.lower()] = func(open_prices, high_prices, low_prices, close_prices, penetration=0)
                        else:
                            dataframe[indicator.lower()] = func(open_prices, high_prices, low_prices, close_prices)
                    # Statistic Functions
                    elif indicator == 'BETA':
                        dataframe[indicator.lower()] = func(high_prices, low_prices, timeperiod=5)
                    elif indicator == 'CORREL':
                        dataframe[indicator.lower()] = func(high_prices, low_prices, timeperiod=30)
                    elif indicator in ['LINEARREG', 'LINEARREG_ANGLE', 'LINEARREG_INTERCEPT', 'LINEARREG_SLOPE', 'TSF']:
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=14)
                    elif indicator in ['STDDEV', 'VAR']:
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=5, nbdev=1)
                    # Math Transform Functions
                    elif indicator in ['ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'COSH', 'EXP', 'FLOOR', 'LN',
                                    'LOG10', 'SIN', 'SINH', 'SQRT', 'TAN', 'TANH']:
                        dataframe[indicator.lower()] = func(close_prices)
                    # Math Operator Functions
                    elif indicator in ['ADD', 'DIV', 'MULT', 'SUB']:
                        dataframe[indicator.lower()] = func(close_prices, open_prices)
                    elif indicator in ['MAX', 'MIN', 'SUM']:
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=30)
                    elif indicator in ['MAXINDEX', 'MININDEX']:
                        dataframe[indicator.lower()] = func(close_prices, timeperiod=30)
                    elif indicator == 'MINMAX':
                        min_vals, max_vals = func(close_prices, timeperiod=30)
                        dataframe['min'] = min_vals
                        dataframe['max'] = max_vals
                    elif indicator == 'MINMAXINDEX':
                        min_idx, max_idx = func(close_prices, timeperiod=30)
                        dataframe['min_idx'] = min_idx
                        dataframe['max_idx'] = max_idx
                    else:
                        print(f"Indicator {indicator} not processed.")
                except Exception as e:
                    print(f"Error processing indicator {indicator}: {e}")

        # Add interval column
        dataframe['interval'] = interval

        return dataframe


    def scan_for_signals(self, df, ticker, interval):
        """
        Scans the input dataframe for bullish and bearish signals based on multiple technical indicators.
        Includes every indicator, computes them, assigns sentiments, and returns the dataframe.

        Parameters:
        df (pd.DataFrame): DataFrame containing stock data with columns ['open', 'high', 'low', 'close', 'volume']
        ticker (str): The ticker symbol of the stock
        interval (str): The interval of the data (e.g., 'd' for daily)

        Returns:
        pd.DataFrame: DataFrame with Open, High, Low, Close, Volume, indicator sentiment columns, ticker, and interval.
        """
        # Ensure the dataframe is sorted by date in ascending order
        try:
            df = df.sort_index()

            # Ensure data types are correct
            df['close'] = df['close'].astype(float)
            df['open'] = df['open'].astype(float)
            df['low'] = df['low'].astype(float)
            df['high'] = df['high'].astype(float)
            df['volume'] = df['volume'].astype(float)

            # Check if there is enough data to compute indicators
            if len(df) < 100:
                raise ValueError("Dataframe must contain at least 100 rows of data to compute indicators.")

            # Prepare a list to store indicator sentiments
            sentiments = {}

            # Overlap Studies Indicators
            # Moving Averages and Trend Analysis Tools
            try:
                df['ema_20'] = ta.EMA(df['close'], timeperiod=20)
                df['ema_50'] = ta.EMA(df['close'], timeperiod=50)
                df['ema_crossover'] = np.where(df['ema_20'] > df['ema_50'], 'Bullish',
                                            np.where(df['ema_20'] < df['ema_50'], 'Bearish', 'Neutral'))
            except Exception as e:
                print(f"Error computing EMA indicators: {e}")

            try:
                df['bb_upper'], df['bb_middle'], df['bb_lower'] = ta.BBANDS(df['close'], timeperiod=20)
                df['bb_sentiment'] = np.where(df['close'] > df['bb_upper'], 'Bearish',
                                            np.where(df['close'] < df['bb_lower'], 'Bullish', 'Neutral'))
            except Exception as e:
                print(f"Error computing Bollinger Bands: {e}")

            # Include other overlap studies indicators with sentiments
            overlap_studies = {
                'DEMA': {'period': 30},
                'KAMA': {'period': 30},
                'MA': {'period': 30, 'matype': 0},
                'MIDPOINT': {'period': 14},
                'MIDPRICE': {'period': 14},
                'SAR': {'acceleration': 0.02, 'maximum': 0.2},
                'SMA': {'period': 30},
                'T3': {'period': 5, 'vfactor': 0.7},
                'TEMA': {'period': 30},
                'TRIMA': {'period': 30},
                'WMA': {'period': 30}
            }

            for indicator, params in overlap_studies.items():
                try:
                    if indicator in ['SAR']:
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], **params)
                    elif indicator in ['MIDPRICE']:
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], timeperiod=params['period'])
                    else:
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], **params)
                    # Assign sentiment
                    df[f"{indicator.lower()}_sentiment"] = np.where(df['close'] > df[indicator.lower()], 'Bullish', 'Bearish')
                except Exception as e:
                    print(f"Error computing {indicator}: {e}")

            # Momentum Indicators
            # Include all momentum indicators with sentiments
            momentum_indicators = {
                'ADX': {'period': 14},
                'ADXR': {'period': 14},
                'APO': {'fastperiod': 12, 'slowperiod': 26, 'matype': 0},
                'AROON': {'period': 14},
                'AROONOSC': {'period': 14},
                'BOP': {},
                'CCI': {'period': 14},
                'CMO': {'period': 14},
                'DX': {'period': 14},
                'MACD': {'fastperiod': 12, 'slowperiod': 26, 'signalperiod': 9},
                'MFI': {'period': 14},
                'MOM': {'period': 10},
                'PPO': {'fastperiod': 12, 'slowperiod': 26, 'matype': 0},
                'ROC': {'period': 10},
                'RSI': {'period': 14},
                'STOCH': {},
                'STOCHF': {},
                'STOCHRSI': {'period': 14},
                'TRIX': {'period': 30},
                'ULTOSC': {'timeperiod1': 7, 'timeperiod2': 14, 'timeperiod3': 28},
                'WILLR': {'period': 14}
            }

            for indicator, params in momentum_indicators.items():
                try:
                    if indicator in ['ADX', 'ADXR', 'DX']:
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'], timeperiod=params['period'])
                        # Assign sentiment
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 25, 'Strong Trend', 'Weak Trend')
                    elif indicator == 'APO':
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], **params)
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 0, 'Bullish', 'Bearish')
                    elif indicator == 'AROON':
                        aroondown, aroonup = ta.AROON(df['high'], df['low'], timeperiod=params['period'])
                        df['aroon_down'] = aroondown
                        df['aroon_up'] = aroonup
                        df['aroon_sentiment'] = np.where(aroonup > aroondown, 'Bullish',
                                                        np.where(aroonup < aroondown, 'Bearish', 'Neutral'))
                    elif indicator == 'AROONOSC':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], timeperiod=params['period'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 0, 'Bullish', 'Bearish')
                    elif indicator == 'BOP':
                        df[indicator.lower()] = getattr(ta, indicator)(df['open'], df['high'], df['low'], df['close'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 0, 'Bullish', 'Bearish')
                    elif indicator == 'CCI':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'], timeperiod=params['period'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 100, 'Overbought (Bearish)',
                                                                        np.where(df[indicator.lower()] < -100, 'Oversold (Bullish)', 'Neutral'))
                    elif indicator == 'CMO':
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], timeperiod=params['period'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 50, 'Bullish',
                                                                        np.where(df[indicator.lower()] < -50, 'Bearish', 'Neutral'))
                    elif indicator == 'MACD':
                        macd, macdsignal, macdhist = ta.MACD(df['close'], **params)
                        df['macd'] = macd
                        df['macd_signal'] = macdsignal
                        df['macd_hist'] = macdhist
                        df['macd_sentiment'] = np.where(macd > macdsignal, 'Bullish',
                                                        np.where(macd < macdsignal, 'Bearish', 'Neutral'))
                    elif indicator == 'MFI':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'], df['volume'], timeperiod=params['period'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 80, 'Overbought (Bearish)',
                                                                        np.where(df[indicator.lower()] < 20, 'Oversold (Bullish)', 'Neutral'))
                    elif indicator == 'RSI':
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], timeperiod=params['period'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 70, 'Overbought (Bearish)',
                                                                        np.where(df[indicator.lower()] < 30, 'Oversold (Bullish)', 'Neutral'))
                    elif indicator in ['STOCH', 'STOCHF']:
                        if indicator == 'STOCH':
                            slowk, slowd = ta.STOCH(df['high'], df['low'], df['close'])
                            df['slowk'] = slowk
                            df['slowd'] = slowd
                            df['stoch_sentiment'] = np.where(slowk > 80, 'Overbought (Bearish)',
                                                            np.where(slowk < 20, 'Oversold (Bullish)', 'Neutral'))
                        else:
                            fastk, fastd = ta.STOCHF(df['high'], df['low'], df['close'])
                            df['fastk'] = fastk
                            df['fastd'] = fastd
                            df['stochf_sentiment'] = np.where(fastk > 80, 'Overbought (Bearish)',
                                                            np.where(fastk < 20, 'Oversold (Bullish)', 'Neutral'))
                    elif indicator == 'STOCHRSI':
                        fastk, fastd = ta.STOCHRSI(df['close'], timeperiod=params['period'])
                        df['stochrsi_fastk'] = fastk
                        df['stochrsi_fastd'] = fastd
                        df['stochrsi_sentiment'] = np.where(fastk > 80, 'Overbought (Bearish)',
                                                            np.where(fastk < 20, 'Oversold (Bullish)', 'Neutral'))
                    elif indicator == 'TRIX':
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], timeperiod=params['period'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 0, 'Bullish', 'Bearish')
                    elif indicator == 'ULTOSC':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'], **params)
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 70, 'Overbought (Bearish)',
                                                                        np.where(df[indicator.lower()] < 30, 'Oversold (Bullish)', 'Neutral'))
                    elif indicator == 'WILLR':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'], timeperiod=params['period'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > -20, 'Overbought (Bearish)',
                                                                        np.where(df[indicator.lower()] < -80, 'Oversold (Bullish)', 'Neutral'))
                    else:
                        # For other indicators not specifically handled
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], **params)
                except Exception as e:
                    print(f"Error computing {indicator}: {e}")

            # Volume Indicators
            volume_indicators = ['AD', 'ADOSC', 'OBV']

            for indicator in volume_indicators:
                try:
                    if indicator == 'AD':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'], df['volume'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > df[indicator.lower()].shift(1), 'Bullish', 'Bearish')
                    elif indicator == 'ADOSC':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'], df['volume'], fastperiod=3, slowperiod=10)
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 0, 'Bullish', 'Bearish')
                    elif indicator == 'OBV':
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], df['volume'])
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > df[indicator.lower()].shift(1), 'Bullish', 'Bearish')
                except Exception as e:
                    print(f"Error computing {indicator}: {e}")

            # Volatility Indicators
            volatility_indicators = ['ATR', 'NATR', 'TRANGE']

            for indicator in volatility_indicators:
                try:
                    df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'])
                    # Assign sentiment based on current value compared to moving average
                    df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > df[indicator.lower()].rolling(window=14).mean(), 'High Volatility', 'Low Volatility')
                except Exception as e:
                    print(f"Error computing {indicator}: {e}")

            # Price Transform
            price_transforms = ['AVGPRICE', 'MEDPRICE', 'TYPPRICE', 'WCLPRICE']

            for indicator in price_transforms:
                try:
                    if indicator == 'AVGPRICE':
                        df[indicator.lower()] = getattr(ta, indicator)(df['open'], df['high'], df['low'], df['close'])
                    else:
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], df['close'])
                    df[f"{indicator.lower()}_sentiment"] = np.where(df['close'] > df[indicator.lower()], 'Bullish', 'Bearish')
                except Exception as e:
                    print(f"Error computing {indicator}: {e}")

            # Cycle Indicators
            cycle_indicators = ['HT_DCPERIOD', 'HT_DCPHASE', 'HT_PHASOR', 'HT_SINE', 'HT_TRENDMODE']

            for indicator in cycle_indicators:
                try:
                    if indicator == 'HT_PHASOR':
                        inphase, quadrature = getattr(ta, indicator)(df['close'])
                        df['ht_phasor_inphase'] = inphase
                        df['ht_phasor_quadrature'] = quadrature
                        # Sentiment can be complex; placeholder
                        df['ht_phasor_sentiment'] = 'Neutral'
                    elif indicator == 'HT_SINE':
                        sine, leadsine = getattr(ta, indicator)(df['close'])
                        df['ht_sine'] = sine
                        df['ht_leadsine'] = leadsine
                        df['ht_sine_sentiment'] = np.where((sine > leadsine) & (sine.shift(1) <= leadsine.shift(1)), 'Bullish',
                                                        np.where((sine < leadsine) & (sine.shift(1) >= leadsine.shift(1)), 'Bearish', 'Neutral'))
                    else:
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'])
                        if indicator == 'HT_TRENDMODE':
                            df['ht_trendmode_sentiment'] = np.where(df[indicator.lower()] == 1, 'Trending Market', 'Cycle Mode')
                        else:
                            df[f"{indicator.lower()}_sentiment"] = 'Neutral'
                except Exception as e:
                    print(f"Error computing {indicator}: {e}")

            # Pattern Recognition
            pattern_indicators = [
                'CDL2CROWS', 'CDL3BLACKCROWS', 'CDL3INSIDE', 'CDL3LINESTRIKE', 'CDL3OUTSIDE',
                'CDL3STARSINSOUTH', 'CDL3WHITESOLDIERS', 'CDLABANDONEDBABY', 'CDLADVANCEBLOCK',
                'CDLBELTHOLD', 'CDLBREAKAWAY', 'CDLCLOSINGMARUBOZU', 'CDLCONCEALBABYSWALL',
                'CDLCOUNTERATTACK', 'CDLDARKCLOUDCOVER', 'CDLDOJI', 'CDLDOJISTAR', 'CDLDRAGONFLYDOJI',
                'CDLENGULFING', 'CDLEVENINGDOJISTAR', 'CDLEVENINGSTAR', 'CDLGAPSIDESIDEWHITE',
                'CDLGRAVESTONEDOJI', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLHARAMI', 'CDLHARAMICROSS',
                'CDLHIGHWAVE', 'CDLHIKKAKE', 'CDLHIKKAKEMOD', 'CDLHOMINGPIGEON', 'CDLIDENTICAL3CROWS',
                'CDLINNECK', 'CDLINVERTEDHAMMER', 'CDLKICKING', 'CDLKICKINGBYLENGTH', 'CDLLADDERBOTTOM',
                'CDLLONGLEGGEDDOJI', 'CDLLONGLINE', 'CDLMARUBOZU', 'CDLMATCHINGLOW', 'CDLMATHOLD',
                'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR', 'CDLONNECK', 'CDLPIERCING', 'CDLRICKSHAWMAN',
                'CDLRISEFALL3METHODS', 'CDLSEPARATINGLINES', 'CDLSHOOTINGSTAR', 'CDLSHORTLINE',
                'CDLSPINNINGTOP', 'CDLSTALLEDPATTERN', 'CDLSTICKSANDWICH', 'CDLTAKURI', 'CDLTASUKIGAP',
                'CDLTHRUSTING', 'CDLTRISTAR', 'CDLUNIQUE3RIVER', 'CDLUPSIDEGAP2CROWS', 'CDLXSIDEGAP3METHODS'
            ]

            for pattern in pattern_indicators:
                try:
                    df[pattern.lower()] = getattr(ta, pattern)(df['open'], df['high'], df['low'], df['close'])
                    df[f"{pattern.lower()}_sentiment"] = np.where(df[pattern.lower()] > 0, 'Bullish',
                                                                np.where(df[pattern.lower()] < 0, 'Bearish', 'No Pattern'))
                except Exception as e:
                    print(f"Error computing pattern {pattern}: {e}")

            # Statistic Functions
            statistic_indicators = ['BETA', 'CORREL', 'LINEARREG', 'LINEARREG_ANGLE', 'LINEARREG_INTERCEPT',
                                    'LINEARREG_SLOPE', 'STDDEV', 'TSF', 'VAR']

            for indicator in statistic_indicators:
                try:
                    if indicator == 'BETA':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], timeperiod=5)
                        df[f"{indicator.lower()}_sentiment"] = 'Neutral'
                    elif indicator == 'CORREL':
                        df[indicator.lower()] = getattr(ta, indicator)(df['high'], df['low'], timeperiod=30)
                        df[f"{indicator.lower()}_sentiment"] = 'Neutral'
                    elif indicator in ['LINEARREG', 'LINEARREG_INTERCEPT', 'TSF']:
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], timeperiod=14)
                        df[f"{indicator.lower()}_sentiment"] = np.where(df['close'] > df[indicator.lower()], 'Bullish', 'Bearish')
                    elif indicator == 'LINEARREG_ANGLE':
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], timeperiod=14)
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 0, 'Uptrend', 'Downtrend')
                    elif indicator == 'LINEARREG_SLOPE':
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], timeperiod=14)
                        df[f"{indicator.lower()}_sentiment"] = np.where(df[indicator.lower()] > 0, 'Uptrend', 'Downtrend')
                    elif indicator in ['STDDEV', 'VAR']:
                        df[indicator.lower()] = getattr(ta, indicator)(df['close'], timeperiod=5, nbdev=1)
                        df[f"{indicator.lower()}_sentiment"] = 'Neutral'
                except Exception as e:
                    print(f"Error computing {indicator}: {e}")

            # Math Operators and Transforms
            # These functions are mathematical and may not have direct trading signals
            # For completeness, they are computed but sentiments are set to 'Neutral'
            math_functions = ['ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'COSH', 'EXP', 'FLOOR', 'LN',
                            'LOG10', 'SIN', 'SINH', 'SQRT', 'TAN', 'TANH',
                            'ADD', 'DIV', 'MAX', 'MAXINDEX', 'MIN', 'MININDEX', 'MINMAX',
                            'MINMAXINDEX', 'MULT', 'SUB', 'SUM']

            for func in math_functions:
                try:
                    if func in ['ADD', 'DIV', 'MULT', 'SUB']:
                        df[func.lower()] = getattr(ta, func)(df['high'], df['low'])
                    elif func in ['MAX', 'MIN', 'SUM']:
                        df[func.lower()] = getattr(ta, func)(df['close'], timeperiod=30)
                    elif func == 'MAXINDEX':
                        df[func.lower()] = getattr(ta, func)(df['close'], timeperiod=30)
                    elif func == 'MININDEX':
                        df[func.lower()] = getattr(ta, func)(df['close'], timeperiod=30)
                    elif func == 'MINMAX':
                        min_vals, max_vals = getattr(ta, func)(df['close'], timeperiod=30)
                        df['min'] = min_vals
                        df['max'] = max_vals
                    elif func == 'MINMAXINDEX':
                        min_idx, max_idx = getattr(ta, func)(df['close'], timeperiod=30)
                        df['min_idx'] = min_idx
                        df['max_idx'] = max_idx
                    else:
                        df[func.lower()] = getattr(ta, func)(df['close'])
                    df[f"{func.lower()}_sentiment"] = 'Neutral'
                except Exception as e:
                    print(f"Error computing {func}: {e}")

            # Add interval column
            df['interval'] = interval

            # Return the dataframe
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
