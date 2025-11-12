import xml.etree.ElementTree as ET
import csv
import asyncio
#FORM_4
from datetime import datetime, timedelta
import asyncpg

# Modified function to recursively parse XML elements and prepare them for database insertion
import os

YOUR_API_KEY = os.environ.get("YOUR_POLYGON_KEY")

import re
import pandas as pd

import numpy as np
from colorsys import rgb_to_hsv
from bs4 import BeautifulSoup
import requests
from asyncio import Semaphore, TimeoutError
from typing import List, Union, Any, Tuple, Dict, Callable
from datetime import datetime, timezone
import pytz
import pandas as pd

# Function to format selected columns in a DataFrame
headers_sec = {'User-Agent': 'Fudstop https://discord.gg/fudstop', 'Content-Type': 'application/json'}

def format_value(value):
    return f"**{round(float(value), 2)}**" if value is not None else "**N/A**"

async def run_in_batches(async_func: Callable[..., Any], args_list: List[Any], batch_size: int = 5):
    """
    Runs an async function concurrently in batches.
    
    :param async_func: The asynchronous function to run.
    :param args_list: A list of arguments to pass to the async function.
    :param batch_size: The number of concurrent executions allowed per batch.
    :return: A list of results in the same order as the input arguments.
    """
    semaphore = asyncio.Semaphore(batch_size)  # Limit concurrency

    async def sem_task(*args):
        """Wraps the async function with a semaphore to limit concurrency."""
        async with semaphore:
            return await async_func(*args)

    results = []
    for i in range(0, len(args_list), batch_size):
        batch = args_list[i:i+batch_size]
        batch_results = await asyncio.gather(*(sem_task(*args) for args in batch))
        results.extend(batch_results)
    
    return results
async def check_macd_sentiment(ticker, timespan, hist: list):
    if hist is not None:
        if hist is not None and len(hist) >= 3:
            
            last_three_values = hist[:3]
            if abs(last_three_values[0] - (-0.02)) < 0.04 and all(last_three_values[i] > last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                return 'bullish'

            if abs(last_three_values[0] - 0.02) < 0.04 and all(last_three_values[i] < last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                return 'bearish'
    else:
        return 'no signal'
    
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

def camel_to_snake_case(columns):
    """
    Convert a list of camelCase strings to snake_case.
    
    Args:
        columns (list): List of strings in camelCase format.

    Returns:
        list: List of strings in snake_case format.
    """
    snake_case_columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in columns]
    return snake_case_columns

def format_large_numbers_in_dataframe2(df, exclude_columns=[]):
    """
    Automatically formats all numeric columns in a DataFrame to readable large numbers,
    excluding specified columns.
    """
    formatted_df = df.copy()
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    for column in numeric_columns:
        if column not in exclude_columns:
            formatted_df[column] = formatted_df[column].apply(format_large_number)
    return formatted_df
def format_large_number(number):
    """
    Formats a number into a human-readable format (e.g., 1K, 1M, 1B, etc.) including negative numbers.
    Handles NoneType to avoid TypeError.
    """
    if number is None:
        return 0.0  # or any placeholder you prefer for None values
    prefix = "-" if number < 0 else ""
    abs_number = abs(number)
    
    if abs_number < 1000:
        return f"{prefix}{abs_number}"
    elif abs_number < 1000000:
        return f"{prefix}{abs_number/1000:.1f}K"
    elif abs_number < 1000000000:
        return f"{prefix}{abs_number/1000000:.1f}M"
    else:
        return f"{prefix}{abs_number/1000000000:.1f}B"
def lowercase_columns(df):
    # Rename columns to lowercase after converting them to strings
    df.columns = map(lambda x: str(x).lower(), df.columns)
    return df

def format_large_numbers_in_dataframe(df):
    """
    Dynamically formats all numeric columns in a DataFrame to readable large numbers.
    """
    try:
        formatted_df = df.copy()
        numeric_columns = formatted_df.select_dtypes(include=['number']).columns

        for column in numeric_columns:
            formatted_df[column] = formatted_df[column].apply(format_large_number)
        
        return formatted_df
    except Exception as e:
        print(e)
def chunk_data(data, chunk_size):
    """Yield successive chunk_size chunks from data."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]
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
            
    except Exception as e:
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

def create_option_symbol(ticker: str, strike: str, call_put: str, expiry: str) -> str:
    """
    Convert ticker, strike, call_put, and expiry into an option symbol string.
    
    Parameters:
    - ticker: The stock ticker symbol (e.g., 'SPY' or 'C')
    - strike: The strike price as a string (e.g., '650' or '40')
    - call_put: Either 'call' or 'put'
    - expiry: The expiry date in the format 'YYYY-MM-DD' or 'YYMMDD'
    
    Returns:
    - The formatted option symbol string, e.g., 'SPY251219C00650000' or 'C241101C00040000'
    """
    # Convert call_put to single character representation
    call_put_char = 'C' if call_put.lower() == 'call' else 'P'
    
    # Extract expiry in the format YYMMDD
    # Assuming expiry is provided as 'YYYY-MM-DD'
    expiry_match = re.match(r"(\d{4})-(\d{2})-(\d{2})", expiry)
    if expiry_match:
        year, month, day = expiry_match.groups()
        expiry_str = f"{year[2:]}{month}{day}"  # Convert to YYMMDD format
    else:
        # Assume it's already in YYMMDD if format is not 'YYYY-MM-DD'
        expiry_str = expiry

    # Format the strike price to be 8 characters long with leading zeros
    # The strike is represented in dollars, multiplied by 1000, and formatted to 8 digits
    try:
        strike_float = float(strike)
        strike_str = f"{int(strike_float * 1000):08d}"  # E.g., 40 becomes '00040000'
    except ValueError:
        raise ValueError("Invalid strike value. Please provide a valid number.")

    # Combine everything into the option symbol
    option_symbol = f"{ticker.upper()}{expiry_str}{call_put_char}{strike_str}"

    return option_symbol
def csv_to_dict(file_path):
    """
    Converts a CSV file into a dictionary based on user-selected columns.

    :param file_path: Path to the CSV file
    :return: Dictionary where keys and values are dynamically selected columns
    """
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            columns = reader.fieldnames
            
            if not columns:
                print("Error: The CSV file is empty or does not contain a header row.")
                return {}

            print(f"Available columns: {columns}")
            
            # Prompt the user to select the key and value columns
            key_column = input("Enter the column to use as keys: ").strip()
            value_column = input("Enter the column to use as values: ").strip()
            
            if key_column not in columns or value_column not in columns:
                print("Error: Invalid column names provided.")
                return {}

            # Build the dictionary
            data_dict = {row[key_column].strip(): row[value_column].strip() for row in reader}
            return data_dict

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return {}

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None
def get_next_trading_day():
    holidays = [
        "240101", "240201", "240527", "240704", "240901", "240911", "241011", "241126", "241225"  # Format: YYMMDD
    ]
    today = datetime.now()
    weekday = today.weekday()
    today_str = today.strftime('%y%m%d')

    # If today is Saturday (5) or Sunday (6), set to next Monday
    if weekday == 5:  # Saturday
        next_day = today + timedelta(days=2)  # Monday
    elif weekday == 6:  # Sunday
        next_day = today + timedelta(days=1)  # Monday
    elif today_str in holidays:  # Check if today is a holiday
        # If today is a holiday, move to next business day
        next_day = today + timedelta(days=1)
        while next_day.strftime('%y%m%d') in holidays or next_day.weekday() >= 5:  # Keep moving to next weekday if it's holiday or weekend
            next_day += timedelta(days=1)
    else:
        next_day = today + timedelta(days=1)  # Default to tomorrow

    return next_day.strftime('%Y-%m-%d')  # Return in YYYY-MM-DD format
def format_option_symbol(row, option_type):
    ticker = row['ticker']
    expiration_date = pd.to_datetime(row['expirationdate']).strftime('%y%m%d')
    strike = f"{row['strike']:.8f}".replace('.', '')
    return f"O:{ticker}{expiration_date}{option_type}{strike}"

def clean_html(html_content):
    # Parse HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract text from the parsed HTML
    text = soup.get_text(separator=' ')
    
    # Replace HTML entities and other non-alphanumeric characters
    text = re.sub(r'&#\d+;', '', text)  # Remove HTML encoded numerics like '&#160;'
    text = re.sub(r'[\r\n\t]+', '\n', text)  # Replace multiple newlines/tabs with a single newline
    text = re.sub(r' +', ' ', text)  # Replace multiple spaces with a single space
    text = re.sub(r'\n +', '\n', text)  # Remove spaces following newlines
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)  # Remove non-ASCII characters
    text = re.sub(r'Page \d+', '', text)  # Optional: Remove page numbers
    
    # Clean up specific case-related wording
    text = text.replace('P>', '').replace('<P', '').strip()  # Removing remnants of <P> tags if any

    return text
@staticmethod
def get_human_readable_string(string):
    result = {}
    try:
        match = re.search(r'(\w{1,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)', string)
        underlying_symbol, year, month, day, call_put, strike_price = match.groups()
        expiry_date = '20' + year + '-' + month + '-' + day
        call_put = 'call' if call_put == 'C' else 'put'
        strike_price = float(strike_price) / 1000
        result['underlying_symbol'] = underlying_symbol
        result['strike_price'] = strike_price
        result['call_put'] = call_put
        result['expiry_date'] = expiry_date
        return result
    except Exception as e:
        print(e)
def camel_to_snake(name: str) -> str:
    """Convert CamelCase or camelCase string to snake_case."""
    # Insert underscore between a lowercase letter and an uppercase letter, then lower the result.
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    snake = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return snake


etf_list = pd.read_csv('files/etf_list.csv')


def is_etf(symbol):
    """Check if a symbol is an ETF."""
    return symbol in etf_list['Symbol'].values
def make_option_symbol(underlying_symbol: str, strike_price: float, call_put: str, expiry_date: str) -> str:
    """
    Convert option details into an option symbol string prefixed with "O:".
    
    Parameters:
      underlying_symbol (str): The ticker of the underlying asset (e.g., "AAPL").
      strike_price (float): The strike price (e.g., 15.0).
      call_put (str): Option type, either "call" or "put" (case-insensitive).
      expiry_date (str): The expiration date in the format "YYYY-MM-DD".
      
    Returns:
      str: An option symbol string in the format:
           O:{underlying_symbol}{YY}{MM}{DD}{C/P}{strike_int}
           where strike_int = int(round(strike_price * 1000)) and YY is the last two digits of the year.
    """
    try:
        # Parse the expiry date.
        parts = expiry_date.split("-")
        if len(parts) != 3:
            raise ValueError("Expiry date must be in format YYYY-MM-DD")
        year_full, month, day = parts
        year = year_full[2:]  # Take last two digits.
        
        # Determine the option type letter.
        letter = "C" if call_put.lower() == "call" else "P"
        
        # Convert strike price into integer form by multiplying by 1000.
        strike_int = int(round(strike_price * 1000))
        
        # Build the option symbol with the "O:" prefix.
        option_symbol = f"O:{underlying_symbol}{year}{month}{day}{letter}{strike_int}"
        return option_symbol
    except Exception as e:
        print("Error in make_option_symbol:", e)
        return ""

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




async def is_current_candle_td9(df: pd.DataFrame) -> str:
    """
    Check if the latest (current) candle in the DataFrame is a TD9.
    Return "buy" for a Buy Setup TD9, "sell" for a Sell Setup TD9, or "no signal" if neither.
    """
    # Ensure we have enough data for the check (at least 13 candles).
    if df.shape[0] < 13:
        return "no signal"

    # Function to check if there's an active TD9 setup in the prior candles
    def is_prior_td9_setup(start_index: int, direction: str) -> bool:
        # For a sell setup, all close prices should be above the close 4 periods earlier
        # For a buy setup, all close prices should be below the close 4 periods earlier
        comparison_op = (lambda a, b: a > b) if direction == "sell" else (lambda a, b: a < b)
        return all(comparison_op(df.iloc[i]['close'], df.iloc[i+4]['close']) for i in range(start_index, start_index + 9))
    
    # Start from the most recent data and check the TD9 criteria without an ongoing setup.
    # Check for a Sell Setup (9 consecutive closes above the close 4 periods prior).
    if is_prior_td9_setup(0, "sell") and not is_prior_td9_setup(9, "sell"):
        return "sell"

    # Check for a Buy Setup (9 consecutive closes below the close 4 periods prior).
    if is_prior_td9_setup(0, "buy") and not is_prior_td9_setup(9, "buy"):
        return "buy"

    return "no signal"
# ... [your code] ...

# Dynam


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
# Function to convert nanosecond timestamp to formatted string in ET (without timezone info)
def convert_to_eastern_time(ns_timestamp):
    # Convert nanoseconds to seconds
    timestamp_in_seconds = ns_timestamp / 1e9
    # Convert to datetime in UTC
    dt_utc = datetime.utcfromtimestamp(timestamp_in_seconds)
    # Convert to Eastern Time
    eastern = pytz.timezone('US/Eastern')
    dt_eastern = dt_utc.replace(tzinfo=pytz.utc).astimezone(eastern)
    # Return the datetime in the desired format without timezone info
    return dt_eastern.strftime('%Y-%m-%d %H:%M:%S')


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





# Example conversion function
def convert_to_est(timestamp_str):
    # Step 1: Parse the string into a datetime object (assuming UTC)
    utc_time = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    # Step 2: Convert to EST
    est_time = utc_time.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('US/Eastern'))
    
    # Step 3: Remove the timezone info
    est_time = est_time.replace(tzinfo=None)
    
    # Step 4: Format the datetime object as a string
    est_time_str = est_time.strftime("%Y-%m-%d %H:%M:%S")
    
    return est_time_str



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



from fudstop4._markets.list_sets.dicts import healthcare,energy,industrials,utilities,etfs,technology,consumer_cyclical,consumer_defensive,communication_services,financial_services,real_estate,basic_materials
async def identify_sector(ticker):
    if ticker in healthcare:
        sector = 'Healthcare'
    elif ticker in energy:
        sector = 'Energy'
    elif ticker in industrials:
        sector = 'Industrials'
    elif ticker in utilities:
        sector = 'Utilities'
    elif ticker in etfs:
        sector = 'ETFs'
    elif ticker in technology:
        sector = 'Technology'
    elif ticker in consumer_cyclical:
        sector = 'ConsumerCyclical'
    elif ticker in consumer_defensive:
        sector = 'ConsumerDefensive'
    elif ticker in communication_services:
        sector = 'CommunicationServices'
    elif ticker in financial_services:
        sector = 'FinancialServices'
    elif ticker in real_estate:
        sector = 'RealEstate'
    elif ticker in basic_materials:
        sector = 'BasicMaterials'
    else:
        sector = 'Unknown'


    return sector



US_MARKET_HOLIDAYS = [
    # Fixed-date holidays (adjusted if they fall on weekends)
    "01-01",  # New Year's Day
    "07-04",  # Independence Day
    "12-25",  # Christmas Day
]

# Dynamic holidays (calculated per year)
def dynamic_holidays(year):
    """Generate US federal holidays that are based on rules, e.g., Thanksgiving (4th Thursday of November)."""
    holidays = []
    # Martin Luther King Jr. Day: 3rd Monday of January
    holidays.append(nth_weekday_of_month(year, 1, 0, 3))  # January, Monday, 3rd occurrence
    # Presidents' Day: 3rd Monday of February
    holidays.append(nth_weekday_of_month(year, 2, 0, 3))  # February, Monday, 3rd occurrence
    # Good Friday: 2 days before Easter (can be calculated with an external library like dateutil)
    holidays.append(easter_date(year) - timedelta(days=2))  # Good Friday
    # Memorial Day: Last Monday of May
    holidays.append(last_weekday_of_month(year, 5, 0))  # May, Monday
    # Labor Day: 1st Monday of September
    holidays.append(nth_weekday_of_month(year, 9, 0, 1))  # September, Monday, 1st occurrence
    # Thanksgiving: 4th Thursday of November
    holidays.append(nth_weekday_of_month(year, 11, 3, 4))  # November, Thursday, 4th occurrence
    return holidays

def nth_weekday_of_month(year, month, weekday, n):
    """Find the nth occurrence of a weekday in a given month and year.
    weekday: Monday=0, Tuesday=1, ..., Sunday=6
    """
    first_day = datetime(year, month, 1)
    first_weekday = first_day.weekday()
    delta_days = (weekday - first_weekday) % 7 + (n - 1) * 7
    return first_day + timedelta(days=delta_days)

def last_weekday_of_month(year, month, weekday):
    """Find the last occurrence of a weekday in a given month and year.
    weekday: Monday=0, Tuesday=1, ..., Sunday=6
    """
    next_month = datetime(year, month, 28) + timedelta(days=4)  # Go to the next month's start
    last_day_of_month = next_month - timedelta(days=next_month.day)
    last_weekday = last_day_of_month.weekday()
    delta_days = (last_weekday - weekday) % 7
    return last_day_of_month - timedelta(days=delta_days)

def easter_date(year):
    """Calculate the date of Easter Sunday for a given year using the 'Anonymous Gregorian algorithm'."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return datetime(year, month, day)

def next_trading_day(start_date=None):
    """Get the next trading day as a string in YYYY-MM-DD format."""
    if start_date is None:
        start_date = datetime.utcnow().date()  # Default to today's date
    elif isinstance(start_date, str):
        # Convert string to a datetime.date object
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    next_day = start_date + timedelta(days=1)

    # Generate dynamic holidays for the given year
    current_year = next_day.year
    all_holidays = [
        datetime.strptime(f"{current_year}-{holiday}", "%Y-%m-%d").date()
        for holiday in US_MARKET_HOLIDAYS
    ] + dynamic_holidays(current_year)

    # Adjust for holidays observed on adjacent weekdays (e.g., July 4th on a weekend)
    observed_holidays = []
    for holiday in all_holidays:
        if holiday.weekday() == 5:  # Saturday observed on Friday
            observed_holidays.append(holiday - timedelta(days=1))
        elif holiday.weekday() == 6:  # Sunday observed on Monday
            observed_holidays.append(holiday + timedelta(days=1))
        else:
            observed_holidays.append(holiday)

    # Iterate to find the next valid trading day
    while next_day.weekday() >= 5 or next_day in observed_holidays:  # Skip weekends and holidays
        next_day += timedelta(days=1)

    return next_day.strftime("%Y-%m-%d")  # Return as a string in YYYY-MM-DD

def last_trading_day(start_date=None):
    """Get the last trading day as a string in YYYY-MM-DD format."""
    if start_date is None:
        start_date = datetime.utcnow().date()  # Default to today's date
    elif isinstance(start_date, str):
        # Convert string to a datetime.date object
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    previous_day = start_date - timedelta(days=1)

    # Generate dynamic holidays for the given year
    current_year = start_date.year
    all_holidays = [
        datetime.strptime(f"{current_year}-{holiday}", "%Y-%m-%d").date()
        for holiday in US_MARKET_HOLIDAYS
    ] + dynamic_holidays(current_year)

    # Adjust for holidays observed on adjacent weekdays (e.g., July 4th on a weekend)
    observed_holidays = []
    for holiday in all_holidays:
        if holiday.weekday() == 5:  # Saturday observed on Friday
            observed_holidays.append(holiday - timedelta(days=1))
        elif holiday.weekday() == 6:  # Sunday observed on Monday
            observed_holidays.append(holiday + timedelta(days=1))
        else:
            observed_holidays.append(holiday)

    # Iterate backward to find the last valid trading day
    while previous_day.weekday() >= 5 or previous_day in observed_holidays:  # Skip weekends and holidays
        previous_day -= timedelta(days=1)

    return previous_day.strftime("%Y-%m-%d")  # Return as a string in YYYY-MM-DD





import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import imageio
import os
from scipy.stats import linregress



import numpy as np
from numba import njit
import pandas as pd
import sys
from pathlib import Path
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
def compute_macd_histogram(prices: np.ndarray) -> np.ndarray:
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
    
    fast = ema_njit(prices, 12)
    slow = ema_njit(prices, 26)
    macd_line = fast - slow
    signal_line = ema_njit(macd_line, 9)
    hist = macd_line - signal_line
    return hist

def add_parabolic_sar_signals(
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

def compute_volume_profile(df_intraday, num_bins=100):
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

def add_td9_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds two columns:
      - td_buy_count: increments if c < c[i-4], else reset to 0
      - td_sell_count: increments if c > c[i-4], else reset to 0
    Continues beyond 9 indefinitely.
    """
    df = df.copy()
    df['td_buy_count'] = 0
    df['td_sell_count'] = 0

    for i in range(len(df)):
        if i < 4:
            continue  # Not enough bars to look back 4
        c_now = df.at[df.index[i], 'c']
        c_4_ago = df.at[df.index[i - 4], 'c']

        # BUY condition
        if c_now < c_4_ago:
            prev_buy = df.at[df.index[i - 1], 'td_buy_count']
            df.at[df.index[i], 'td_buy_count'] = prev_buy + 1 if prev_buy > 0 else 1
        else:
            df.at[df.index[i], 'td_buy_count'] = 0

        # SELL condition
        if c_now > c_4_ago:
            prev_sell = df.at[df.index[i - 1], 'td_sell_count']
            df.at[df.index[i], 'td_sell_count'] = prev_sell + 1 if prev_sell > 0 else 1
        else:
            df.at[df.index[i], 'td_sell_count'] = 0

    return df



# ============================================================================
# OTHER TECHNICAL INDICATORS
# ============================================================================

def compute_bollinger_bands(df: pd.DataFrame, window: int = 20, num_std: float = 2) -> pd.DataFrame:
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

def compute_stochastic_oscillator(df: pd.DataFrame, k_window: int = 14, d_window: int = 3) -> pd.DataFrame:
    """
    Compute the Stochastic Oscillator (%K and %D).
    Adds columns: 'stoch_k', 'stoch_d'
    """
    low_min = df['l'].rolling(window=k_window).min()
    high_max = df['h'].rolling(window=k_window).max()

    df['stoch_k'] = ((df['c'] - low_min) / (high_max - low_min)) * 100
    df['stoch_d'] = df['stoch_k'].rolling(window=d_window).mean()
    return df

def compute_atr(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
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

def compute_supertrend(df: pd.DataFrame, atr_multiplier: float = 3.0, atr_period: int = 10) -> pd.DataFrame:
    """
    Compute the Supertrend indicator.
    Adds columns: 'supertrend', 'supertrend_direction'
    """
    # First compute ATR if not present
    if 'atr' not in df.columns:
        df = compute_atr(df, atr_period)
    
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


def compute_adx(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """
    Compute the Average Directional Index (ADX).
    Adds columns: '+DI', '-DI', 'adx'.
    """
    # Ensure ATR is computed
    if 'atr' not in df.columns:
        df = compute_atr(df, window)

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
                                         but the low is NOT strictly above it (i.e., high > upper band, low <= upper band).
      - 'candle_completely_below_lower': True if the ENTIRE candle is below the lower band
                                         (i.e., candle high < lower band).
      - 'candle_partially_below_lower':  True if the candle's low is below the lower band
                                         but the high is NOT strictly below it (i.e., low < lower band, high >= lower band).
    
    Parameters:
        df (pd.DataFrame): DataFrame with at least columns "ts" (timestamp) and "c" (close price).
                           Optionally, it may include "h" (high) and "l" (low) for full candle data.
        window (int): Window size for rolling mean and std.
        num_std (float): Number of standard deviations for the upper/lower bands.
        trend_points (int): Number of rows to use for computing the trend.
        
    Returns:
        pd.DataFrame: DataFrame with added Bollinger bands, trend columns, and candle flags.
    """
    # Work on a copy sorted in ascending order (oldest first) so that rolling works properly.
    df_sorted = df.copy().sort_values("ts").reset_index(drop=True)
    
    # Calculate the rolling statistics.
    df_sorted["middle_band"] = df_sorted["c"].rolling(window=window, min_periods=window).mean()
    df_sorted["std"] = df_sorted["c"].rolling(window=window, min_periods=window).std()
    df_sorted["upper_band"] = df_sorted["middle_band"] + (num_std * df_sorted["std"])
    df_sorted["lower_band"] = df_sorted["middle_band"] - (num_std * df_sorted["std"])
    
    # Merge the calculated bands back into the original DataFrame based on timestamp.
    df = df.merge(
        df_sorted[["ts", "middle_band", "upper_band", "lower_band"]],
        on="ts",
        how="left"
    )
    
    # Sort descending so that the first row is the most recent.
    df = df.sort_values("ts", ascending=False).reset_index(drop=True)
    
    # Initialize trend columns.
    df["upper_bb_trend"] = pd.Series([None] * len(df), dtype="object")
    df["lower_bb_trend"] = pd.Series([None] * len(df), dtype="object")
    
    # Only compute trends if there are at least 'trend_points' rows.
    if len(df) >= trend_points:
        # Use the most recent `trend_points` rows.
        subset_upper = df["upper_band"].head(trend_points)
        subset_lower = df["lower_band"].head(trend_points)
        
        # Compute trends.
        upper_trend = compute_trend(subset_upper)
        lower_trend = compute_trend(subset_lower)
        
        # Assign prefixed trend values.
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
        # If there's not enough data to compute the trend, default to flattening.
        df.at[0, "upper_bb_trend"] = "flattening"
        df.at[0, "lower_bb_trend"] = "flattening"
    
    # Existing flags: candle_above_upper, candle_below_lower
    # If available, use 'h' and 'l'. Otherwise, fallback to 'c'.
    if {"h", "l"}.issubset(df.columns):
        df["candle_above_upper"] = df["h"] > df["upper_band"]
        df["candle_below_lower"] = df["l"] < df["lower_band"]
    else:
        df["candle_above_upper"] = df["c"] > df["upper_band"]
        df["candle_below_lower"] = df["c"] < df["lower_band"]
    
    # NEW FLAGS: completely above/below, partially above/below
    df["candle_completely_above_upper"] = False
    df["candle_partially_above_upper"] = False
    df["candle_completely_below_lower"] = False
    df["candle_partially_below_lower"] = False
    
    if {"h", "l"}.issubset(df.columns):
        # COMPLETELY ABOVE: l > upper_band
        df.loc[df["l"] > df["upper_band"], "candle_completely_above_upper"] = True
        
        # PARTIALLY ABOVE: h > upper_band but l <= upper_band
        df.loc[
            (df["h"] > df["upper_band"]) & (df["l"] <= df["upper_band"]),
            "candle_partially_above_upper"
        ] = True
        
        # COMPLETELY BELOW: h < lower_band
        df.loc[df["h"] < df["lower_band"], "candle_completely_below_lower"] = True
        
        # PARTIALLY BELOW: l < lower_band but h >= lower_band
        df.loc[
            (df["l"] < df["lower_band"]) & (df["h"] >= df["lower_band"]),
            "candle_partially_below_lower"
        ] = True
    else:
        # Fallback for data lacking 'h'/'l' columns (single price per row).
        # COMPLETELY ABOVE: c > upper_band
        df.loc[df["c"] > df["upper_band"], "candle_completely_above_upper"] = True
        
        # There's no partial concept if we only have close price,
        # but we can still set partial columns to False by default.
        
        # COMPLETELY BELOW: c < lower_band
        df.loc[df["c"] < df["lower_band"], "candle_completely_below_lower"] = True
        # partial flags remain False in this fallback scenario.
    
    return df



def add_atr(df, window=14):
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


def add_stochastic_oscillator(df, window=14, smooth_window=3):
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


def add_cci(df, window=20):
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
def filter_regular_trading_hours(df: pd.DataFrame, tz='US/Eastern') -> pd.DataFrame:
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


def add_obv(df: pd.DataFrame) -> pd.DataFrame:
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




# ─── ADJUST PROJECT DIRECTORY IMPORTS ─────────────────────────────────────────
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

# ─── IMPORT PROJECT MODULES ───────────────────────────────────────────────────
from fudstop4.apis.webull.webull_ta import WebullTA

from fudstop4._markets.list_sets.ticker_lists import most_active_tickers

# ─── GLOBAL OBJECTS AND CONSTANTS ─────────────────────────────────────────────
# Lower concurrency can help reduce overhead. Adjust as desired.
SEM = Semaphore(60)
ticker_id_cache: Dict[str, int] = {}
ticker_cache_lock = Lock()


ta = WebullTA()


# ─── UTILITY: RETRY AIOHTTP REQUESTS ─────────────────────────────────────────
async def fetch_with_retries(
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

# ──────────────────────────────────────────────────────────────────────────────
# BULLISH/BEARISH ENGULFING DETECTION
# ──────────────────────────────────────────────────────────────────────────────
def add_engulfing_patterns(df: pd.DataFrame) -> pd.DataFrame:
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




def add_volume_metrics(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
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


import hashlib
import random
import string
import time
def generate_webull_headers(access_token:str='dc_us_tech1.19a6fcda12a-2e07bf88dbc248e9b0fbf07298960da3'):
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
        "access_token": os.environ.get('ACCESS_TOKEN'),
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