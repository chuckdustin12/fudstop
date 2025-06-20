from datetime import datetime, timedelta
import pytz
from typing import List, Union, Dict
import numpy as np
import pandas as pd
def convert_to_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        # Handle the error or return None if the string cannot be converted
        return 
def flatten(item, parent_key='', separator='_'):
    if item is not None:
        items = {}
        if isinstance(item, dict):
            for k, v in item.items():
                new_key = f"{parent_key}{separator}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.update(flatten(v, new_key, separator=separator))
                elif isinstance(v, list):
                    for i, elem in enumerate(v):
                        items.update(flatten(elem, f"{new_key}_{i}", separator=separator))
                else:
                    items[new_key] = v
        elif isinstance(item, list):
            for i, elem in enumerate(item):
                items.update(flatten(elem, f"{parent_key}_{i}", separator=separator))
        else:
            items[parent_key] = item
        return items
def flatten_list_of_dicts(lst: List[Union[Dict, List]]) -> List[Dict]:
    return [flatten(item) for item in lst] if lst is not None else None
def flatten_dict(d, parent_key='', sep='.'):
    if d is not None:
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
        return items
async def parse_most_active(ticker_entry):

    all_parsed_data = []
    # Parsing 'ticker' attributes
    datas = ticker_entry.get('data', {})
    for data in datas:
        parsed_data = {}
        ticker_info = data.get('ticker', {})
        parsed_data['tickerId'] = ticker_info.get('tickerId')
        parsed_data['exchangeId'] = ticker_info.get('exchangeId')
        parsed_data['regionId'] = ticker_info.get('regionId')
        parsed_data['currencyId'] = ticker_info.get('currencyId')
        parsed_data['currencyCode'] = ticker_info.get('currencyCode')
        parsed_data['name'] = ticker_info.get('name')
        parsed_data['symbol'] = ticker_info.get('symbol')
        parsed_data['disSymbol'] = ticker_info.get('disSymbol')
        parsed_data['disExchangeCode'] = ticker_info.get('disExchangeCode')
        parsed_data['status'] = ticker_info.get('status')
        parsed_data['close'] = ticker_info.get('close')
        parsed_data['change'] = ticker_info.get('change')
        parsed_data['changeRatio'] = ticker_info.get('changeRatio')
        parsed_data['marketValue'] = ticker_info.get('marketValue')
        parsed_data['volume'] = ticker_info.get('volume')
        parsed_data['turnoverRate'] = ticker_info.get('turnoverRate')
        parsed_data['regionName'] = ticker_info.get('regionName')
        parsed_data['peTtm'] = ticker_info.get('peTtm')
        parsed_data['timeZone'] = ticker_info.get('timeZone')
        parsed_data['preClose'] = ticker_info.get('preClose')
        parsed_data['fiftyTwoWkHigh'] = ticker_info.get('fiftyTwoWkHigh')
        parsed_data['fiftyTwoWkLow'] = ticker_info.get('fiftyTwoWkLow')
        parsed_data['open'] = ticker_info.get('open')
        parsed_data['high'] = ticker_info.get('high')
        parsed_data['low'] = ticker_info.get('low')
        parsed_data['vibrateRatio'] = ticker_info.get('vibrateRatio')
        

        all_parsed_data.append(parsed_data)
    return all_parsed_data



# Creating a function to parse each attribute of the data_entry and return it as a dictionary
async def parse_total_top_options(data_entry):
    all_parsed_data = []
    
    for data in data_entry:
        parsed_data = {}
        ticker_info = data.get('ticker', {})
        for key, value in ticker_info.items():
            if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value
    
        # Parsing 'values' attributes
        values_info = data.get('values', {})
        for key, value in values_info.items():

            if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value
        
        all_parsed_data.append(parsed_data)
    if 't_sectype' in all_parsed_data:
        all_parsed_data.remove('t_sectype')

    return all_parsed_data



async def parse_contract_top_options(data_entry):
    all_parsed_data = []
    for data in data_entry:
        parsed_data = {}
        # Parsing 'belongTicker' attributes
        belong_ticker_info = data.get('belongTicker', {})
        
        for key, value in belong_ticker_info.items():
            if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value

       
        
        # Parsing 'derivative' attributes
        derivative_info = data.get('derivative', {})
        for key, value in derivative_info.items():
            if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value
        
        # Parsing 'values' attributes
        values_info = data.get('values', {})
        for key, value in values_info.items():
            if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value 

        all_parsed_data.append(parsed_data)
    if 'bt_secType' in all_parsed_data:
        all_parsed_data = all_parsed_data.remove('bt_secType')
    return all_parsed_data



# Creating a function to parse each attribute of the data_entry and return it as a dictionary
async def parse_ticker_values(data_entry):
    all_parsed_data = []
    data_entry = data_entry.get('data', {})
    for data in data_entry:
        parsed_data = {}
        ticker_info = data.get('ticker', {})
        for key, value in ticker_info.items():
            if type(key) != list and key != 'secType' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value
    
        # Parsing 'values' attributes
        values_info = data.get('values', {})
        for key, value in values_info.items():
            if type(key) != list and key != 'secType' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value
        
        all_parsed_data.append(parsed_data)
    return all_parsed_data


def parse_forex(ticker_list):
    parsed_data_list = []
    
    for ticker_entry in ticker_list:
        parsed_data = {}
        
        parsed_data['tickerId'] = ticker_entry.get('tickerId')
        parsed_data['exchangeId'] = ticker_entry.get('exchangeId')
        
        parsed_data['name'] = ticker_entry.get('name')
        parsed_data['symbol'] = ticker_entry.get('symbol')
        parsed_data['disSymbol'] = ticker_entry.get('disSymbol')
        parsed_data['status'] = ticker_entry.get('status')
        parsed_data['close'] = ticker_entry.get('close')
        parsed_data['change'] = ticker_entry.get('change')
        parsed_data['changeRatio'] = ticker_entry.get('changeRatio')
        parsed_data['marketValue'] = ticker_entry.get('marketValue')
        
        parsed_data_list.append(parsed_data)
    
    return parsed_data_list



# Creating a function to parse each attribute of the data_entry and return it as a dictionary
def parse_ticker_values_sync(data_entry):
    all_parsed_data = []
    data_entry = data_entry.get('data', {})
    for data in data_entry:
        parsed_data = {}
        ticker_info = data.get('ticker', {})
        for key, value in ticker_info.items():
            if type(key) != list and key != 'secType' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value
    
        # Parsing 'values' attributes
        values_info = data.get('values', {})
        for key, value in values_info.items():
            if type(key) != list and key != 'secType' and key != 'derivativeSupport':
                parsed_data[f'{key}'] = value
        
        all_parsed_data.append(parsed_data)
    return all_parsed_data


def parse_forex(ticker_list):
    parsed_data_list = []
    
    for ticker_entry in ticker_list:
        parsed_data = {}
        
        parsed_data['tickerId'] = ticker_entry.get('tickerId')
        parsed_data['exchangeId'] = ticker_entry.get('exchangeId')
        
        parsed_data['name'] = ticker_entry.get('name')
        parsed_data['symbol'] = ticker_entry.get('symbol')
        parsed_data['disSymbol'] = ticker_entry.get('disSymbol')
        parsed_data['status'] = ticker_entry.get('status')
        parsed_data['close'] = ticker_entry.get('close')
        parsed_data['change'] = ticker_entry.get('change')
        parsed_data['changeRatio'] = ticker_entry.get('changeRatio')
        parsed_data['marketValue'] = ticker_entry.get('marketValue')
        
        parsed_data_list.append(parsed_data)
    
    return parsed_data_list


async def parse_etfs(response):
    flattened_data = []
    
    for tab in response.get('tabs', []):
        tab_info = {
            'id': tab.get('id'),
            'name': tab.get('name'),
            'comment': tab.get('comment'),
            'queryId': tab.get('queryId'),
            'upNum': tab.get('upNum'),
            'dowoNum': tab.get('dowoNum'),
            'flatNum': tab.get('flatNum'),
        }
        
        for ticker in tab.get('tickerTupleList', []):
            # Merge the 'tab' info and the 'ticker' info into a single dictionary
            merged_info = {**tab_info, **ticker}
            flattened_data.append(merged_info)

    return flattened_data

# Define a function to parse the given data object with specific attributes under the parent key "item"
async def parse_ipo_data(data):
    """
    Parses an IPO data object and returns a dictionary with relevant fields.

    Args:
    - item (dict): The IPO data item to parse.

    Returns:
    - dict: A dictionary containing parsed IPO data.
    """
    items = data['items']
    all_parsed_data=[]
    for item in items:
        parsed_data = {
            'ticker_id': item.get('tickerId', None),
            'list_date': item.get('listDate', None),
            'issue_up_limit': item.get('issueUpLimit', None),
            'issue_price': item.get('issuePrice', None),
            'currency_id': item.get('currencyId', None),
            'exchange_code': item.get('disExchangeCode', None),
            'symbol': item.get('disSymbol', None),
            'ipo_status': item.get('ipoStatus', None),
            'issue_currency_id': item.get('issueCurrencyId', None),
            'issue_down_limit': item.get('issueDownLimit', None),
            'issue_price_str': item.get('issuePriceStr', None),
            'name': item.get('name', None),
            'offering_type': item.get('offeringType', None),
            'prospectus': item.get('prospectus', None),
            'prospectus_publish_date': item.get('prospectusPublishDate', None),
            'purchase_end_date': item.get('purchaseEndDate', None),
            'purchase_start_date': item.get('purchaseStartDate', None),
            'close_days': item.get('closeDays', 0)  # Assuming 0 if not present
        }
        all_parsed_data.append(parsed_data)
    return all_parsed_data




# Function to convert Unix timestamps in seconds to Eastern Time in milliseconds
def convert_seconds_to_ms_eastern_time(seconds_timestamp):
    et_offset = -5 * 3600  # Eastern Standard Time (EST) offset in seconds
    utc_time = datetime.utcfromtimestamp(int(seconds_timestamp))
    eastern_time = utc_time + timedelta(seconds=et_offset)
    eastern_time_ms = int(eastern_time.timestamp() * 1000)  # Convert to milliseconds
    return eastern_time_ms


def convert_unix_to_eastern(unix_timestamp):
    eastern_time = datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return eastern_time


def format_date(input_str):
    # Parse the input string as a datetime object
    input_datetime = datetime.fromisoformat(input_str.replace("Z", "+00:00"))

    # Convert the datetime object to Eastern Time
    utc_timezone = pytz.timezone("UTC")
    eastern_timezone = pytz.timezone("US/Eastern")
    input_datetime = input_datetime.astimezone(utc_timezone)
    eastern_datetime = input_datetime.astimezone(eastern_timezone)

    # Format the output string
    output_str = eastern_datetime.strftime("%Y-%m-%d at %I:%M%p %Z")
    return output_str




def calculate_setup(df):
    setup_count = 0
    for i in range(3, len(df)):
        if df['Close'][i] > df['Close'][i-3]:  # Assuming 'c' is the close price column
            setup_count += 1
        else:
            setup_count = 0
        
        if setup_count >= 9:
            return True
    return False
def calculate_countdown(df):
    countdown_count = 0
    for i in range(4, len(df)):
        if df['High'][i] > df['High'][i-2]:  # Assuming 'h' is the high price column
            countdown_count += 1
        else:
            countdown_count = 0
        
        if countdown_count >= 9:
            return True

def _calc_slope(series: pd.Series) -> float:
    """Return the slope of a series using simple linear regression."""
    y = series.values
    x = np.arange(len(y))
    if len(y) < 2:
        return 0.0
    slope, _ = np.polyfit(x, y, 1)
    return slope

def is_bull_flag(df: pd.DataFrame, lookback: int = 8) -> bool:
    """Basic bull flag detection on the latest candles."""
    data = df.tail(lookback).reset_index(drop=True)
    if data.shape[0] < 5:
        return False

    pole_len = lookback // 2
    pole = data.iloc[:pole_len]
    flag = data.iloc[pole_len:]

    if _calc_slope(pole['Close']) <= 0:
        return False

    if _calc_slope(flag['Close']) >= 0:
        return False

    pole_move = pole['Close'].iloc[-1] - pole['Close'].iloc[0]
    if pole_move <= 0:
        return False

    retrace = pole['Close'].iloc[-1] - flag['Low'].min()
    if retrace / pole_move > 0.5:
        return False

    return True

def is_bear_flag(df: pd.DataFrame, lookback: int = 8) -> bool:
    """Basic bear flag detection on the latest candles."""
    data = df.tail(lookback).reset_index(drop=True)
    if data.shape[0] < 5:
        return False

    pole_len = lookback // 2
    pole = data.iloc[:pole_len]
    flag = data.iloc[pole_len:]

    if _calc_slope(pole['Close']) >= 0:
        return False

    if _calc_slope(flag['Close']) <= 0:
        return False

    pole_move = pole['Close'].iloc[0] - pole['Close'].iloc[-1]
    if pole_move <= 0:
        return False

    retrace = flag['High'].max() - pole['Close'].iloc[-1]
    if retrace / pole_move > 0.5:
        return False

    return True
        
from typing import Tuple
def detect_candlestick_patterns(df: pd.DataFrame, lookback_range: Tuple[int, int] = (4, 7)) -> pd.DataFrame:
    """
    Scans the most recent candles to detect candlestick patterns like uptrend, downtrend,
    bull/bear engulfing, shooting star, and evening star. Tags each pattern as 'bullish' or 'bearish'.
    
    Args:
    df (pd.DataFrame): DataFrame containing OHLC data and other relevant columns.
    lookback_range (Tuple[int, int]): Range of candles to scan (default is 4 to 7 candles).
    
    Returns:
    pd.DataFrame: DataFrame with detected patterns and their respective tags.
    """
    
    # Ensure we are working with the latest candles
    df = df.tail(lookback_range[1]).copy()

    signals = []

    for i in range(len(df) - lookback_range[0] + 1):
        recent_df = df.iloc[i:i+lookback_range[0]]

        # Initialize the signal
        signal = {"pattern": None, "tag": None, "start_date": recent_df.iloc[0]['Timestamp'], "end_date": recent_df.iloc[-1]['Timestamp']}

        # Detect trends
        if recent_df['Close'].is_monotonic_increasing:
            signal["pattern"] = "uptrend"
            signal["tag"] = "bullish"
        elif recent_df['Close'].is_monotonic_decreasing:
            signal["pattern"] = "downtrend"
            signal["tag"] = "bearish"

        # Detect Bullish Engulfing
        if recent_df.iloc[-2]['Close'] < recent_df.iloc[-2]['Open'] and recent_df.iloc[-1]['Close'] > recent_df.iloc[-1]['Open'] and \
            recent_df.iloc[-1]['Close'] > recent_df.iloc[-2]['Open'] and recent_df.iloc[-1]['Open'] < recent_df.iloc[-2]['Close']:
            signal["pattern"] = "bullish engulfing"
            signal["tag"] = "bullish"
        
        # Detect Bearish Engulfing
        if recent_df.iloc[-2]['Close'] > recent_df.iloc[-2]['Open'] and recent_df.iloc[-1]['Close'] < recent_df.iloc[-1]['Open'] and \
            recent_df.iloc[-1]['Close'] < recent_df.iloc[-2]['Open'] and recent_df.iloc[-1]['Open'] > recent_df.iloc[-2]['Close']:
            signal["pattern"] = "bearish engulfing"
            signal["tag"] = "bearish"
        
        # Detect Shooting Star
        if recent_df.iloc[-1]['High'] > recent_df.iloc[-1]['Open'] and recent_df.iloc[-1]['High'] > recent_df.iloc[-1]['Close'] and \
            recent_df.iloc[-1]['Close'] > recent_df.iloc[-1]['Low'] and recent_df.iloc[-1]['Open'] > recent_df.iloc[-1]['Low'] and \
            recent_df.iloc[-1]['Close'] < recent_df.iloc[-1]['Open']:
            signal["pattern"] = "shooting star"
            signal["tag"] = "bearish"

        # Detect Evening Star
        if recent_df.iloc[-3]['Close'] > recent_df.iloc[-3]['Open'] and \
            recent_df.iloc[-2]['Close'] > recent_df.iloc[-2]['Open'] and recent_df.iloc[-2]['Close'] < recent_df.iloc[-3]['Close'] and \
            recent_df.iloc[-1]['Close'] < recent_df.iloc[-1]['Open'] and recent_df.iloc[-1]['Close'] < recent_df.iloc[-2]['Open']:
            signal["pattern"] = "evening star"
            signal["tag"] = "bearish"

        # Detect Bull/Bear Flags
        if is_bull_flag(recent_df):
            signal["pattern"] = "bull flag"
            signal["tag"] = "bullish"
        elif is_bear_flag(recent_df):
            signal["pattern"] = "bear flag"
            signal["tag"] = "bearish"

        if signal["pattern"]:
            signals.append(signal)

    return pd.DataFrame(signals)
