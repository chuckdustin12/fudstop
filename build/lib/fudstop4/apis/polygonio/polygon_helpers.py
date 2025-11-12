import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

import pandas as pd
def flatten_nested_dict(row, column_name):
    # Check if the value in the specified column is a dictionary
    if isinstance(row[column_name], dict):
        # Create new column names by combining the original column name and the nested keys
        for key, value in row[column_name].items():
            new_column_name = f"{key}"
            row[new_column_name] = value
        # Drop the original column with nested dictionary
        row = row.drop(column_name)
    return row

import xml.etree.ElementTree as ET

import asyncio
#FORM_4

import asyncpg

# Modified function to recursively parse XML elements and prepare them for database insertion
import os

YOUR_API_KEY = os.environ.get("YOUR_POLYGON_KEY")

import re
import pandas as pd
from polygonio.mapping import OPTIONS_EXCHANGES,option_condition_dict,STOCK_EXCHANGES,stock_condition_dict,TAPES

from colorsys import rgb_to_hsv
from bs4 import BeautifulSoup
import requests
from asyncio import Semaphore, TimeoutError
from typing import List, Union, Any, Tuple, Dict
from datetime import datetime
import pytz
import pandas as pd

# Function to format selected columns in a DataFrame
headers_sec = {'User-Agent': 'Fudstop https://discord.gg/fudstop', 'Content-Type': 'application/json'}

def format_value(value):
    return f"**{round(float(value), 2)}**" if value is not None else "**N/A**"

async def check_macd_sentiment(ticker, timespan, hist: list):
    if hist is not None:
        if hist is not None and len(hist) >= 3:
            
            last_three_values = hist[:3]
            if abs(last_three_values[0] - (-0.02)) < 0.04 and all(last_three_values[i] > last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                return 'bullish'

            if abs(last_three_values[0] - 0.02) < 0.04 and all(last_three_values[i] < last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                return 'bearish'
    else:
        return None
    
def flatten_object(obj, parent_key='', separator='_'):
    items = {}
    for k, v in obj.__dict__.items():
        new_key = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_object(v, new_key, separator=separator))
        elif hasattr(v, '__dict__'):  # Check if v is an object with attributes
            items.update(flatten_object(v, new_key, separator=separator))
        else:
            items[new_key] = v
    return items
def format_large_numbers_in_dataframe(df, columns_to_format):
    """
    Formats selected columns in a DataFrame to readable large numbers.
    """
    formatted_df = df.copy()
    for column in columns_to_format:
        if column in formatted_df.columns:
            formatted_df[column] = formatted_df[column].apply(format_large_number)
    return formatted_df
# Example function to format selected keys in a dictionary
def format_large_numbers_in_dict(data_dict, keys_to_format):
    """
    Formats selected keys in a dictionary to readable large numbers.
    """
    formatted_dict = data_dict.copy()
    for key in keys_to_format:
        if key in formatted_dict:
            formatted_dict[key] = format_large_number(formatted_dict[key])
    return formatted_dict
def format_large_number(num):
    if isinstance(num, list):
        return [format_large_number(n) for n in num]
    elif isinstance(num, str):
        try:
            num = float(num)
        except ValueError:
            return num  # or however you want to handle this case
    if num is None:
        return
    if abs(num) >= 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.2f} T"
    elif abs(num) >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f} B"
    elif abs(num) >= 1_000_000:
        return f"{num / 1_000_000:.2f} M"
    elif abs(num) >= 1_000:
        return f"{num / 1_000:.2f} K"
    else:
        return str(num)
    
def calculate_td9_series(df):
    setup_count = 0
    td9_series = pd.Series(index=df.index, dtype='Int64')  # Initialize a series with the same index as df

    for i in range(4, len(df)):
        if df['Close'][i] > df['Close'][i - 4]:
            setup_count += 1
        else:
            setup_count = 0

        if setup_count >= 9:
            td9_series.at[df.index[i]] = setup_count  # Store the count

    df['TD9'] = td9_series  # Add the series to the DataFrame

    return df

def calculate_setup(df):
    setup_count = 0
    for i in range(4, len(df)):
        if df['Close'][i] > df['Close'][i-4]:  # Assuming 'c' is the close price column
            setup_count += 1
        else:
            setup_count = 0
        
        if setup_count >= 9:
            return True
    return False
def calculate_countdown(df):
    countdown_count = 0
    for i in range(2, len(df)):
        if df['High'][i] > df['High'][i-2]:  # Assuming 'h' is the high price column
            countdown_count += 1
        else:
            countdown_count = 0
        
        if countdown_count >= 9:
            return True
    return False
def send_to_discord(image_path, webhook_url):
    with open(image_path, 'rb') as f:
        files = {'file': ('image.png', f)}
        payload = {
            "embeds": [
                {
                    "title": "TD9 Chart",
                    "image": {
                        "url": "attachment://image.png"
                    }
                }
            ]
        }
        headers = {'Content-Type': 'multipart/form-data', 'Authorization': f"Bearer {os.environ.get('YOUR_DISCORD_HTTP_TOKEN')}"}
        r = requests.post(webhook_url, headers=headers, files=files, json=payload)
        if r.status_code == 204:
            print("Successfully sent image to Discord.")
        else:
            print(f"Failed to send image to Discord. Status Code: {r.status_code}")

def human_readable(string):
    try:
        match = re.search(r'(\w{1,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)', string) #looks for the options symbol in O: format
        underlying_symbol, year, month, day, call_put, strike_price = match.groups()
    except TypeError:
        underlying_symbol = f"AMC"
        year = "23"
        month = "02"
        day = "17"
        call_put = "CALL"
        strike_price = "380000"
    
    expiry_date = month + '/' + day + '/' + '20' + year
    if call_put == 'C':
        call_put = 'Call'
    else:
        call_put = 'Put'
    strike_price = '${:.2f}'.format(float(strike_price)/1000)
    return "{} {} {} Expiring {}".format(underlying_symbol, strike_price, call_put, expiry_date)

@staticmethod
def get_human_readable_string(string):
    result = {}
    try:
        match = re.search(r'(\w{1,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)', string)
        underlying_symbol, year, month, day, call_put, strike_price = match.groups()
    except TypeError:
        underlying_symbol = "AMC"
        year = "23"
        month = "02"
        day = "17"
        call_put = "CALL"
        strike_price = "380000"

    expiry_date = '20' + year + '-' + month + '-' + day
    call_put = 'call' if call_put == 'C' else 'put'
    strike_price = float(strike_price) / 1000
    result['underlying_symbol'] = underlying_symbol
    result['strike_price'] = strike_price
    result['call_put'] = call_put
    result['expiry_date'] = expiry_date
    return result

def flatten(item, parent_key='', separator='_'):
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
    return [flatten(item) for item in lst]




def is_current_candle_td9(df: pd.DataFrame) -> bool:
    """
    Check if the latest (current) candle in the DataFrame is a TD9.
    """
    # Ensure we have enough data for the check.
    if df.shape[0] < 10:  # We need 10 candles to compare the 9th with the 0th
        return False

    # Get the first 10 candles (assuming the DataFrame is sorted in descending order by time).
    first_ten = df.head(10).reset_index(drop=True)
    
    # Check for a Buy Setup TD9
    buy_setup = first_ten.loc[0, 'Close'] < first_ten.loc[9, 'Close']
    
    # Check for a Sell Setup TD9
    sell_setup = first_ten.loc[0, 'Close'] > first_ten.loc[9, 'Close']

    return buy_setup or sell_setup
# ... [your code] ...

# Dynam


async def check_macd_sentiment(ticker, timespan, hist: list):
    if hist is not None:
        if hist is not None and len(hist) >= 3:
            
            last_three_values = hist[:3]
            if abs(last_three_values[0] - (-0.02)) < 0.04 and all(last_three_values[i] > last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                return 'bullish'

            if abs(last_three_values[0] - 0.02) < 0.04 and all(last_three_values[i] < last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                return 'bearish'
    else:
        return None
    
def convert_to_ns_datetime(unix_timestamp_str):
    # Convert Unix timestamp string to integer and then to seconds
    unix_timestamp = int(unix_timestamp_str) / 1000.0
    
    # Convert to datetime object in UTC
    dt_utc = datetime.utcfromtimestamp(unix_timestamp)
    
    # Localize to UTC
    dt_utc = pytz.utc.localize(dt_utc)
    
    # Convert to Eastern Time
    dt_et = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    # Remove the timezone offset information, if you want to
    dt_et = dt_et.replace(tzinfo=None)
    
    return dt_et   
def convert_to_datetime(unix_timestamp_str):
    # Convert Unix timestamp string to integer
    unix_timestamp = int(unix_timestamp_str)
    
    # Convert to datetime object in UTC
    dt_utc = datetime.utcfromtimestamp(unix_timestamp)
    
    # Localize to UTC
    dt_utc = pytz.utc.localize(dt_utc)
    
    # Convert to Eastern Time
    dt_et = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    # Remove the timezone offset information, if you want to
    dt_et = dt_et.replace(tzinfo=None)
    

    return dt_et

def convert_to_datetime_or_str(input_str):
    try:
        # If it's a Unix timestamp, convert it
        unix_timestamp = int(input_str)
        dt_utc = datetime.utcfromtimestamp(unix_timestamp)
        dt_utc = pytz.utc.localize(dt_utc)
        dt_et = dt_utc.astimezone(pytz.timezone('US/Eastern'))
        return dt_et.replace(tzinfo=None)
    except ValueError:
        # If it's a date string, parse it
        return datetime.strptime(input_str, '%B %d, %Y')

def convert_datetime_list(timestamps, unit='ms'):
    """
    Convert a list of Unix timestamps to datetime objects.

    Parameters:
    - timestamps: list of Unix timestamps
    - unit: the unit of the timestamp (default is 's' for seconds)

    Returns:
    - list of datetime objects
    """
    dt_series = pd.Series(pd.to_datetime(timestamps, unit=unit, utc=True))
    dt_series = dt_series.dt.tz_localize(None)
    return dt_series.tolist()

# Function to convert timestamps to Eastern Time (ET)
def convert_to_et(timestamp):
    # Assuming timestamps are in UTC, you can convert them to ET
    utc_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    et_timezone = pytz.timezone('US/Eastern')  # Eastern Time (ET)
    et_time = utc_time.astimezone(et_timezone)
    return et_time.strftime('%Y-%m-%d %H:%M:%S %Z')
def calculate_days_to_expiry(expiry_str, timestamp):
    expiry = datetime.strptime(expiry_str, '%m/%d/%Y').date()
    return (expiry - timestamp.date()).days
def calculate_price_to_strike(price, strike):
    return price / strike if strike != 0 else 0


def map_conditions(conditions):
    if isinstance(conditions, list):
        return [option_condition_dict.get(cond, cond) for cond in conditions]
    else:
        return conditions
def map_stock_conditions(conditions):
    if isinstance(conditions, list):
        return [stock_condition_dict.get(cond, cond) for cond in conditions]
    else:
        return conditions
def count_itm_otm(group):
    underlying_price = group['underlying_price'].iloc[0]  # Assuming the underlying price is the same within each expiry group
    itm_call = len(group[(group['call_put'] == 'call') & (group['strike'] < underlying_price)])
    otm_call = len(group[(group['call_put'] == 'call') & (group['strike'] >= underlying_price)])
    itm_put = len(group[(group['call_put'] == 'put') & (group['strike'] > underlying_price)])
    otm_put = len(group[(group['call_put'] == 'put') & (group['strike'] <= underlying_price)])

    return pd.Series({
        'ITM_calls': itm_call,
        'OTM_calls': otm_call,
        'ITM_puts': itm_put,
        'OTM_puts': otm_put
    })
def calculate_candlestick(data, interval):
    open_price = data[0]['open_price']
    close_price = data[-1]['close_price']
    high_price = max(item['high_price'] for item in data)
    low_price = min(item['low_price'] for item in data)
    volume = sum(item['volume'] for item in data)

    return {
        'open_price': open_price,
        'close_price': close_price,
        'high_price': high_price,
        'low_price': low_price,
        'volume': volume
    }

def to_unix_timestamp_eastern(timestamp_ns):
    timestamp_eastern = to_datetime_eastern(timestamp_ns)
    return int(timestamp_eastern.timestamp())
def to_datetime_eastern(timestamp_ns):
    # Convert the timestamp to a pandas datetime object in UTC
    timestamp_utc = pd.to_datetime(timestamp_ns, unit='ns').tz_localize('UTC')

    # Convert the timestamp to the US Eastern timezone
    timestamp_eastern = timestamp_utc.tz_convert('US/Eastern')

    return timestamp_eastern



# Function to traverse the XML tree and collect unique tags and keys
def traverse_tree(element, unique_tags, unique_keys):
    unique_tags.add(element.tag)
    for key in element.keys():
        unique_keys.add(key)
    for child in element:
        traverse_tree(child, unique_tags, unique_keys)

# Function to traverse the XML tree and extract information
def traverse_and_extract(element, target_tags, extracted_data):
    if element.tag in target_tags:
        extracted_data[element.tag] = element.text
    for child in element:
        traverse_and_extract(child, target_tags, extracted_data)
# Function to recursively parse XML elements and return them in a dictionary format
def parse_element(element, parsed=None):
    if parsed is None:
        parsed = {}
    
    for child in element:
        # Skip elements without values
        if child.text is None or child.text.strip() == '':
            continue

        # Handle duplicate tags by converting them into lists
        if child.tag in parsed:
            if not isinstance(parsed[child.tag], list):
                parsed[child.tag] = [parsed[child.tag]]
            parsed[child.tag].append(parse_element(child, {}))
        else:
            parsed[child.tag] = parse_element(child, {})
        
        # Store the element value if it has one
        if child.text.strip():
            parsed[child.tag]['value'] = child.text.strip()
            
    return parsed

def download_xml_file(url, file_path):
    response = requests.get(url, headers=headers_sec)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"File downloaded successfully at {file_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")



def prepare_data_for_insertion(element, parsed=None, current_table=None, current_record=None):
    if parsed is None:
        parsed = {'ownershipDocument': [], 'issuer': [], 'reportingOwner': [], 
                  'reportingOwnerAddress': [], 'nonDerivativeTransaction': [], 
                  'transactionAmounts': [], 'postTransactionAmounts': [], 'footnote': []}
    
    if current_table and current_record is not None:
        # Add the element value to the current table's last record
        if element.text and element.text.strip():
            current_record[element.tag] = element.text.strip()

    # Determine the current table based on the element tag
    if element.tag in parsed:
        current_table = element.tag
        new_record = {}
        parsed[current_table].append(new_record)
        current_record = new_record

    for child in element:
        # Recursive call to handle nested elements
        prepare_data_for_insertion(child, parsed, current_table, current_record)
            
    return parsed

def safe_divide(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b

def safe_subtract(a, b):
    if a is None or b is None:
        return None
    return a - b

def safe_multiply(a, b):
    if a is None or b is None:
        return None
    return a * b

def safe_max(a, b):
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)



# Function to get image URL from a webpage
def get_first_image_url(webpage_url):
    # Download the webpage
    response = requests.get(webpage_url)
    if response.status_code != 200:
        return None  # Failed to download

    # Parse the HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the first image tag
    img_tag = soup.find('img')
    
    if img_tag is None:
        return None  # No image found

    # Extract the image URL
    img_url = img_tag.get('src')

    return img_url
def parse_to_dataframe(data):
    # Extracting relevant fields from the JSON response
    results = data['results'] if 'results' in data else data
    parsed_data = []
    if results is not data:
        for result in results:
            parsed_data.append({
                "article_url": result.get("article_url", None),
                "author": result.get("author", None),
                "description": result.get("description", None),
                "id": result.get("id", None),
                "published_utc": result.get("published_utc", None),
                "publisher_name": result.get("publisher", {}).get("name", None),
                "tickers": ", ".join(result.get("tickers", [])),
                "title": result.get("title", None)
            })
        
        # Create DataFrame
        df = pd.DataFrame(parsed_data)
        return df
    
    # Initialize an empty list to store flattened dictionaries
    flattened_data = []

    # Iterate through the data
    for item in data:
        flat_item = {}

        # Recursively flatten nested dictionaries
        def flatten_dict(d, parent_key=''):
            for key, value in d.items():
                new_key = parent_key + '_' + key if parent_key else key
                if isinstance(value, dict):
                    flatten_dict(value, new_key)
                else:
                    # Convert list values to strings
                    if isinstance(value, list):
                        value = str(value)
                    flat_item[new_key] = value

        flatten_dict(item)

        # Append the flattened dictionary to the list
        flattened_data.append(flat_item)

    # Create a DataFrame from the flattened data
    df = pd.DataFrame(flattened_data)

    # Display the DataFrame
    print(df)
        

def get_first_index_from_dict(data_dict):
    first_index_data_dict = {}
    for key, value in data_dict.items():
        if value:  # Check if the value (which should be a list) is not empty
            first_index_data_dict[key] = value[0]
        else:
            first_index_data_dict[key] = None  # or some default value
    return first_index_data_dict


def calculate_percent_decrease(open_price, close_price):
    percent_decrease = ((open_price - close_price) / close_price) * 100
    return percent_decrease


import aiohttp
import asyncio

from urllib.parse import urlencode
# Fetch all URLs

async def fetch_url(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            return data
        else:
            print(f"Error: {response.status}")
            return None




async def process_contract(data, db):
        


    if data is not None:
        for contract in data:
            try:

                option_symbol = contract.get('ticker')
                expiry = contract.get('details').get('expiration_date')
                strike = contract.get('details').get('strike_price')
                call_put = contract.get('details').get('contract_type')
                underlying_symbol = contract.get('underlying_asset').get('ticker')
                underlying_price = contract.get('underlying_asset').get('price')
                change_to_breakeven = contract.get('underlying_asset').get('change_to_breakeven')

                

                trade_size = contract.get('last_trade').get('size')
                trade_price = contract.get('last_trade').get('price')
                trade_conditions = contract.get('last_trade', None).get('conditions', None)
                sip_timestamp = contract.get('last_trade', None).get('sip_timestamp', None)
                if sip_timestamp is not None:
                    sip_timestamp = datetime.fromtimestamp(sip_timestamp / 1e9)
                trade_conditions = [option_condition_dict.get(c) for c in trade_conditions] if trade_conditions is not None else []
                trade_conditions = trade_conditions[0]
                trade_exchange = OPTIONS_EXCHANGES.get(contract.get('last_trade').get('exchange', None))
                break_even_price = contract.get('break_even_price', None)
                implied_volatility = contract.get('implied_volatility', None)
                open_interest = contract.get('open_interest', None)
                name = contract.get('name', None)
                open = contract.get('session').get('open', None)
                high = contract.get('session').get('high', None)
                low = contract.get('session').get('low', None)
                close = contract.get('session').get('close', None)
                volume = contract.get('session').get('volume', None)
                prev_close = contract.get('session').get('previous_close')
                early_trading_change_percent = contract.get('session').get('early_trading_change_percent')
                change = contract.get('session').get('change')
                early_trading_change = contract.get('session').get('early_trading_change')
                change_percent = contract.get('session').get('change_percent', None)

                delta = contract.get('greeks').get('delta')
                vega = contract.get('greeks').get('vega')
                theta = contract.get('greeks').get('theta')
                gamma = contract.get('greeks').get('gamma')

                ask = contract.get('last_quote').get('ask')
                bid = contract.get('last_quote').get('bid')
                ask_size = contract.get('last_quote').get('ask_size')
                bid_size = contract.get('last_quote').get('bid_size')
                ask_exchange = OPTIONS_EXCHANGES.get(contract.get('last_quote').get('ask_exchange'))
                bid_exchange = OPTIONS_EXCHANGES.get(contract.get('last_quote').get('bid_exchange'))


                data_dict = {
                    'option_symbol': option_symbol,
                    'expiry': expiry,
                    'strike': strike,
                    'call_put': call_put,
                    'underlying_symbol': underlying_symbol,
                    'underlying_price': underlying_price,
                    'change_to_breakeven': change_to_breakeven,
                    'trade_size': trade_size,
                    'trade_price': trade_price,
                    'trade_conditions': trade_conditions,
                    'sip_timestamp': sip_timestamp,
                    'trade_exchange': trade_exchange,
                    'break_even_price': break_even_price,
                    'implied_volatility': implied_volatility,
                    'open_interest': open_interest,
                    'name': name,
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume,
                    'prev_close': prev_close,
                    'early_trading_change_percent': early_trading_change_percent,
                    'change': change,
                    'early_trading_change': early_trading_change,
                    'change_percent': change_percent,
                    'delta': delta,
                    'vega': vega,
                    'theta': theta,
                    'gamma': gamma,
                    'ask': ask,
                    'bid': bid,
                    'ask_size': ask_size,
                    'bid_size': bid_size,
                    'ask_exchange': ask_exchange,
                    'bid_exchange': bid_exchange
                }


                return data_dict
            except Exception as e:
                # Handle any other exceptions that might occur
                print(f"An error occurred: {e}")
from asyncio import Semaphore
sema = Semaphore(5)


async def fetch_page(url):
    try:
        async with aiohttp.ClientSession() as session, session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except TimeoutError:
        print(f"Timeout when accessing {url}")
    except aiohttp.ClientResponseError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

async def paginate_concurrent(url, as_dataframe=False, concurrency=25):
    all_results = []

    
    async with aiohttp.ClientSession() as session:
        pages_to_fetch = [url]
        
        while pages_to_fetch:
            tasks = []
            
            for _ in range(min(concurrency, len(pages_to_fetch))):
                next_url = pages_to_fetch.pop(0)
                tasks.append(fetch_page(next_url))
                
            results = await asyncio.gather(*tasks)
            if results is not None:
                for data in results:
                    if data is not None:
                        if "results" in data:
                            all_results.extend(data["results"])
                            
                        next_url = data.get("next_url")
                        if next_url:
                            next_url += f'&{urlencode({"apiKey": "iWivQU_dAR5CZHKpd15bEBApfWfBZgJ5"})}'
                            pages_to_fetch.append(next_url)
                    else:
                        break
    if as_dataframe:
        import pandas as pd
        return pd.DataFrame(all_results)
    else:
        return all_results
    
def describe_color(rgb_tuple):
    red, green, blue = rgb_tuple
    # Normalize the RGB values
    red, green, blue = red / 255.0, green / 255.0, blue / 255.0

    # Convert RGB to HSV
    hue, saturation, value = rgb_to_hsv(red, green, blue)
    
    # Determine the color based on hue
    hue_degree = hue * 360
    if saturation < 0.1 and value > 0.9:
        return "White"
    elif saturation < 0.2 and value < 0.2:
        return "Black"
    elif saturation < 0.2:
        return "Gray"
    elif hue_degree >= 0 and hue_degree < 12:
        return "Red"
    elif hue_degree >= 12 and hue_degree < 35:
        return "Orange"
    elif hue_degree >= 35 and hue_degree < 85:
        return "Yellow"
    elif hue_degree >= 85 and hue_degree < 170:
        return "Green"
    elif hue_degree >= 170 and hue_degree < 260:
        return "Blue"
    elif hue_degree >= 260 and hue_degree < 320:
        return "Purple"
    else:
        return "Red"
def parse_operation(operation):
    # Initialize an empty dictionary to store the parsed data
    parsed_data = {}

    # Extract the main attributes from the operation dictionary
    main_attrs = [
        'operationId', 'auctionStatus', 'operationDate', 'settlementDate', 
        'maturityDate', 'operationType', 'operationMethod', 'settlementType',
        'termCalenderDays', 'term', 'releaseTime', 'closeTime', 'note',
        'lastUpdated', 'participatingCpty', 'acceptedCpty', 'totalAmtSubmitted',
        'totalAmtAccepted'
    ]
    for attr in main_attrs:
        parsed_data[attr] = operation.get(attr)

    # Check if the 'details' key exists and is a non-empty list
    details = operation.get('details')
    if details and isinstance(details, list) and len(details) > 0:
        # Assuming details is a list and you're working with the first item
        details_dict = details[0]
        # Extract the attributes from the details dictionary
        detail_attrs = [
            'securityType', 'amtSubmitted', 'amtAccepted',
            'percentOfferingRate', 'percentAwardRate'
        ]
        for attr in detail_attrs:
            parsed_data[attr] = details_dict.get(attr)

    return parsed_data    
# Extend the existing function
def decimal_to_color(decimal_color):
    # Convert the decimal number to hexadecimal
    hex_color = hex(decimal_color)[2:].zfill(6).upper()
    
    # Extract the RGB components
    red = int(hex_color[0:2], 16)
    green = int(hex_color[2:4], 16)
    blue = int(hex_color[4:6], 16)
    
    # Describe the color
    color_name = describe_color((red, green, blue))
    
    return color_name

async def paginate_tickers(url, as_dataframe=False, concurrency=5):
    all_results = []

    
    async with aiohttp.ClientSession() as session:
        pages_to_fetch = [url]
        
        while pages_to_fetch:
            tasks = []
            
            for _ in range(min(concurrency, len(pages_to_fetch))):
                next_url = pages_to_fetch.pop(0)
                tasks.append(fetch_page(next_url))
                
            results = await asyncio.gather(*tasks)
            if results is not None:
                for data in results:
                    if data is not None:
                        if "tickers" in data:
                            all_results.extend(data["tickers"])
                            
                        next_url = data.get("next_url")
                        if next_url:
                            next_url += f'&{urlencode({"apiKey": YOUR_API_KEY})}'
                            pages_to_fetch.append(next_url)
                    
    if as_dataframe:
        import pandas as pd
        return pd.DataFrame(all_results)
    else:
        return all_results









async def fetch_and_parse_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            results = data.get('results', [])
            
            # Flatten nested dictionaries
            flattened_results = [flatten_dict(result) for result in results]
            
            return flattened_results 

def flatten_dict(d, parent_key='', sep='.'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
def rename_keys(original_dict, key_mapping):
    return {key_mapping.get(k, k): v for k, v in original_dict.items()}

async def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

selected_ticker = None

import time
def last_unix_interval(threshold) -> int:
    now = datetime.now()
    minute = now.minute

    # Round down to the nearest 30-minute mark
    if minute >= threshold:
        rounded_minute = 30
    else:
        rounded_minute = 0

    # Construct the datetime object for the last 30-minute interval
    last_30_min_interval = now.replace(minute=rounded_minute, second=0, microsecond=0)

    # Convert to Unix timestamp
    unix_timestamp = int(time.mktime(last_30_min_interval.timetuple()))

    return unix_timestamp



def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)
def convert_str_to_datetime(date_time_str):
    # Parsing time and date from the string
    date_time_str = str(date_time_str)
    time_str, am_pm, _, _, month, day_with_comma, year = date_time_str.split()

    # Debug print for inspection
    print(f"Debug: {date_time_str}")

    hour, minute = map(int, time_str.split(':'))

    # Adjusting for AM/PM
    if am_pm == 'PM' and hour != 12:
        hour += 12
    elif am_pm == 'AM' and hour == 12:
        hour = 0

    # Removing comma from the day
    day = int(day_with_comma.replace(",", ""))

    # Constructing datetime object with the US/Eastern timezone
    dt_string = f"{year}-{month}-{day:02} {hour:02}:{minute:02}"
    dt_et = datetime.strptime(dt_string, '%Y-%B-%d %H:%M')
    eastern = pytz.timezone('US/Eastern')
    dt_et = eastern.localize(dt_et)

    # Convert to desired timezone or manipulate further if necessary
    return dt_et
def map_months(month_str):
    month_dict = {
        "January": "01",
        "February": "02",
        "March": "03",
        "April": "04",
        "May": "05",
        "June": "06",
        "July": "07",
        "August": "08",
        "September": "09",
        "October": "10",
        "November": "11",
        "December": "12"
    }
    return month_dict.get(month_str, "Invalid month")



def current_time_to_unix() -> int:
    # Get current time
    now = datetime.now()
    
    # Convert to Unix timestamp
    unix_timestamp = int(time.mktime(now.timetuple()))
    
    return unix_timestamp
    
def convert_timestamp_to_human_readable(url: str) -> str:
    try:
        timestamp_str = url.split("timestamp=")[-1]
        timestamp = int(timestamp_str)
    except (ValueError, IndexError):
        return "Invalid URL or timestamp"

    dt_object = datetime.fromtimestamp(timestamp)
    human_readable_date = dt_object.strftime('%Y-%m-%d %H:%M:%S')

    return human_readable_date

def convert_to_yymmdd(expiry_str):
    expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
    return expiry_date.strftime('%y%m%d')


def parse_element(element, parent_key='', parsed_dict={}):
    children = list(element)
    if parent_key:
        parent_key += '.'
    
    if children:
        for child in children:
            parse_element(child, parent_key + child.tag, parsed_dict)
    else:
        parsed_dict[parent_key[:-1]] = element.text
def shorten_form4_keys(data_dict):
    shortened_dict = {}
    for key, value in data_dict.items():
        new_key = key
        # Remove common prefixes for 'issuer'
        new_key = new_key.replace('issuer_', '')
        
        # Remove common prefixes for 'reportingOwner'
        new_key = new_key.replace('reportingOwner_', '')
        new_key = new_key.replace('reportingOwnerId_', '')
        new_key = new_key.replace('reportingOwnerAddress_', '')
        new_key = new_key.replace('reportingOwnerRelationship_', '')
        
        # Remove common prefixes for 'nonDerivativeTable' and 'nonDerivativeTransaction'
        new_key = new_key.replace('nonDerivativeTable_', '')
        new_key = new_key.replace('nonDerivativeTransaction_', '')
        
        # Remove common prefixes for 'transactionCoding', 'transactionAmounts', 'postTransactionAmounts', and 'ownershipNature'
        new_key = new_key.replace('transactionCoding_', '')
        new_key = new_key.replace('transactionAmounts_', '')
        new_key = new_key.replace('postTransactionAmounts_', '')
        new_key = new_key.replace('ownershipNature_', '')
        
        # Remove common prefixes for 'securityTitle', 'transactionDate', 'transactionTimeliness', etc.
        new_key = new_key.replace('securityTitle_', '')
        new_key = new_key.replace('transactionDate_', '')
        new_key = new_key.replace('transactionTimeliness_', '')
        
        # Remove common prefixes for 'footnotes' and 'ownerSignature'
        new_key = new_key.replace('footnotes_', '')
        new_key = new_key.replace('ownerSignature_', '')
        
        # Add more replacements as needed
        
        shortened_dict[new_key] = value
    return shortened_dict


# Function to recursively find all fields and values in an XML ElementTree
def extract_fields_recursive(element, parent_key='', results={}):
    for child in element:
        key = f"{parent_key}.{child.tag}" if parent_key else child.tag
        if len(child) > 0:
            extract_fields_recursive(child, key, results)
        else:
            results[key] = child.text
    return results
import matplotlib.pyplot as plt
def save_df_as_image(df, image_path):
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.axis('tight')
    ax.axis('off')
    plt.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    plt.savefig(image_path)
    plt.close(fig)


import natsort
from PIL import Image

def autocrop_image(image: Any, border=0) -> Any:
    """Crop empty space from PIL image

    Parameters
    ----------
    image : Image
        PIL image to crop
    border : int, optional
        scale border outwards, by default 0

    Returns
    -------
    Image
        Cropped image
    """
    bbox = image.getbbox()
    image = image.crop(bbox)
    (width, height) = image.size
    width += border * 2
    height += border * 2
    cropped_image = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    cropped_image.paste(image, (border, border))
    return cropped_image


conversion_mapping = {
    "K": 1_000,
    "M": 1_000_000,
}



# Approach 2: Fill Missing Values
def fill_missing_values(data_dict):
    max_length = max(len(v) for v in data_dict.values())
    for k, v in data_dict.items():
        if len(v) < max_length:
            data_dict[k] = v + [None] * (max_length - len(v))



all_units = "|".join(conversion_mapping.keys())
float_re = natsort.numeric_regex_chooser(natsort.ns.FLOAT | natsort.ns.SIGNED)
unit_finder = re.compile(rf"({float_re})\s*({all_units})", re.IGNORECASE)
from kaleido.scopes.plotly import PlotlyScope

scope = PlotlyScope()
import io
import plotly.graph_objects as go
import uuid
def save_image(filename: str, fig: go.Figure = None, bytesIO: io.BytesIO = None) -> str:
    """Takes go.Figure or io.BytesIO object, adds uuid to filename, autocrops, and saves

    Parameters
    ----------
    filename : str
        Name to save image as
    fig : go.Figure, optional
        Table object to autocrop and save, by default None
    bytesIO : io.BytesIO, optional
        BystesIO object to autocrop and save, by default None

    Returns
    -------
    str
        filename with UUID added to use for bot processing

    Raises
    ------
    Exception
        Function requires a go.Figure or BytesIO object
    """
    imagefile = "image.jpg"

    if fig:
        # Transform Fig into PNG with Running Scope. Returns image bytes
        fig = scope.transform(fig, scale=3, format="png")
        imgbytes = io.BytesIO(fig)
    elif bytesIO:
        imgbytes = bytesIO
    else:
        raise Exception("Function requires a go.Figure or io.BytesIO object")

    image = Image.open(imgbytes)
    image = autocrop_image(image, 0)
    imgbytes.seek(0)
    image.save(imagefile, "jpg", quality=100)
    image.close()

    return imagefile

def flatten_dict(d, parent_key='', sep='.'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
def normalize_columns(data, columns_to_normalize):
    # Convert dict to DataFrame if necessary
    if isinstance(data, dict):
        df = pd.DataFrame.from_dict(data)
    else:
        df = data
    
    # Apply the helper function to each row and each specified column
    for column in columns_to_normalize:
        if column in df.columns:
            df = df.apply(lambda row: flatten_nested_dict(row, column), axis=1)
        else:
            # Handle the case where the column does not exist
            # For example, you can print a warning message
            print(f"Warning: Column '{column}' does not exist in the dataframe and will be skipped.")
    
    return df
