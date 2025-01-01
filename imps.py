from fudstop.apis.polygonio.polygon_database import PolygonDatabase
from fudstop.apis.webull.webull_trading import WebullTrading
from fudstop.apis.webull.webull_option_screener import WebullOptionScreener
from fudstop.apis.occ.occ_sdk import occSDK
from fudstop.apis.polygonio.polygon_options import PolygonOptions
from fudstop.apis.webull.webull_options.webull_options import WebullOptions
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
from fudstop.apis.master.master_sdk import MasterSDK
from fudstop.apis.newyork_fed.newyork_fed_sdk import FedNewyork
from fudstop.discord_.bot_menus.pagination import AlertMenus
from fudstop.all_helpers import chunk_string
from fudstop.apis.helpers import format_large_numbers_in_dataframe2
from fudstop._markets.list_sets.ticker_lists import most_active_tickers
from fudstop.apis.helpers import chunks
from openai import OpenAI
import disnake
from disnake.ext import commands
import json
from tabulate import tabulate
from fudstop.apis.helpers import format_large_numbers_in_dataframe2
poly_opts = PolygonOptions()
import os
import dotenv
import time
from dotenv import load_dotenv
load_dotenv()
fed = FedNewyork()
from datetime import datetime, timedelta
master = MasterSDK()
todays_date = datetime.now().strftime('%Y-%m-%d')
today = datetime.now().strftime('%Y-%m-%d')
now = datetime.now()
five_mins_ago = now - timedelta(minutes=5)
thirty_mins_ago = now - timedelta(minutes=30)
sixty_mins_ago = now - timedelta(minutes=60)
one_twenty_mins_ago = now - timedelta(minutes=120)
two_forty_mins_ago = now - timedelta(minutes=240)
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
five_days_from_now = (datetime.now() + timedelta(days=5)).strftime('%Y-%m-%d')
two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d') 
ten_days_ago = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d') 
poly = Polygon()
db = PolygonDatabase()
occ = occSDK(host='localhost', user='chuck', database='market_data', password='fud', port=5432)
trading = WebullTrading()
screen = WebullOptionScreener()
wb_opts = WebullOptions()
openai_key = os.environ.get('OPENAI_KEY')
from fudstop._markets.list_sets.ticker_lists import all_tickers
import asyncio
import pandas as pd
import httpx
from openai import OpenAI
# Function to determine if a given date is a trading day
def is_trading_day(date, holidays=None):
    if holidays is None:
        holidays = []
    return date.weekday() < 5 and date not in holidays

# Function to get the next trading day
def get_next_trading_day(start_date=None, holidays=None):
    if start_date is None:
        start_date = datetime.today()
    if holidays is None:
        holidays = []
    next_day = start_date
    while True:
        if is_trading_day(next_day, holidays):
            return next_day
        next_day += timedelta(days=1)
client = OpenAI(api_key=openai_key)

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "access_token": "dc_us_tech1.193f1ba4ca7-5bd586952af0445ea4e4883003c577b1",
    "app": "global",
    "app-group": "broker",
    "appid": "wb_web_app",
    "device-type": "Web",
    "did": "w35fbki4nv4n4i6fjbgjca63niqpo_22",
    "hl": "en",
    "origin": "https://app.webull.com",
    "os": "web",
    "osv": "i9zh",
    "platform": "web",
    "priority": "u=1, i",
    "referer": "https://app.webull.com/",
    "reqid": "h15qdhcy99l2sidi00t2sox8y748h_35",
    "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "tz": "America/Chicago",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "ver": "5.2.1",
    "x-s": "9ad7d1393ca5c705bfe6469622b6805384a53b27c86fcaf5966061737c602747",
    "x-sv": "xodp2vg9"
}

async def async_get_td9(ticker, interval):
    try:
        timeStamp = None
        if ticker == 'I:SPX':
            ticker = 'SPXW'
        elif ticker =='I:NDX':
            ticker = 'NDX'
        elif ticker =='I:VIX':
            ticker = 'VIX'
        elif ticker == 'I:RUT':
            ticker = 'RUT'
        elif ticker == 'I:XSP':
            ticker = 'XSP'
        



        tickerid = await trading.get_webull_id(ticker)
        if timeStamp is None:
            # if not set, default to current time
            timeStamp = int(time.time())

        base_fintech_gw_url = f'https://quotes-gw.webullfintech.com/api/quote/charts/query?tickerIds={tickerid}&type={interval}&timestamp={timeStamp}&count=800&extendTrading=1'

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

        timespan = interval_mapping.get(interval, 'minute')

        async with httpx.AsyncClient(headers=headers) as client:
            data = await client.get(base_fintech_gw_url)
            r = data.json()
            if r and isinstance(r, list) and 'data' in r[0]:
                data = r[0]['data']
                if data is not None:
                    parsed_data = []
                    for entry in data:
                        values = entry.split(',')
                        if values[-1] == 'NULL':
                            values = values[:-1]
                        parsed_data.append([float(value) if value != 'null' else 0.0 for value in values])
                    
                    sorted_data = sorted(parsed_data, key=lambda x: x[0], reverse=True)
                    
                    columns = ['Timestamp', 'Open', 'Close', 'High', 'Low', 'N', 'Volume', 'Vwap'][:len(sorted_data[0])]
                    
                    df = pd.DataFrame(sorted_data, columns=columns)
                    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s', utc=True)
                    df['Timestamp'] = df['Timestamp'].dt.tz_convert('US/Eastern').dt.tz_localize(None)
                    df['Ticker'] = ticker
                    df['timespan'] = interval


                    return df
                
    except Exception as e:
        print(e)
async def check_macd_sentiment(hist: list):
    try:
        if hist is not None:
            if hist is not None and len(hist) >= 3:
                
                last_three_values = hist[:3]
                if abs(last_three_values[0] - (-0.02)) < 0.04 and all(last_three_values[i] > last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                    return 'bullish'

                if abs(last_three_values[0] - 0.02) < 0.04 and all(last_three_values[i] < last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                    return 'bearish'
            else:
                return '-'
    except Exception as e:
        print(e)


async def is_current_candle_td9(df: pd.DataFrame) -> str:
    """
    Check if the latest (current) candle in the DataFrame is a TD9.
    Return "BUY" for a Buy Setup TD9, "SELL" for a Sell Setup TD9, or "NONE" if neither.
    """
    # Ensure we have enough data for the check.
    if df.shape[0] < 13:  # We need at least 13 candles to make the comparison (9 + 4)
        return "NONE"

    # We will check if there are 9 consecutive candles above or below the closure of four candles prior
    for i in range(9):
        if not (df.iloc[i]['Close'] > df.iloc[i+4]['Close']):
            break
    else:
        return "SELL"

    for i in range(9):
        if not (df.iloc[i]['Close'] < df.iloc[i+4]['Close']):
            break
    else:
        return "BUY"



etf_list = pd.read_csv('files/etf_list.csv')

def is_etf(symbol):
    """Check if a symbol is an ETF."""
    return symbol in etf_list['Symbol'].values





async def check_td9_conditions(ticker):
    timespans = ['m60', 'm120', 'm240', 'd', 'w', 'm']
    td9_info = [async_get_td9(ticker, timespan) for timespan in timespans]
    results = await asyncio.gather(*td9_info)

    interval_dict = {'m60': '1hour', 'm120': '2hour', 'm240': '4hour', 'd': 'day', 'w': 'week', 'm': 'month'}

    td9_checks = {
        timespan: results[i] if len(results) > i and not isinstance(results[i], Exception) else None
        for i, timespan in enumerate(timespans)
    }

    td9_signals = {timespan: is_current_candle_td9(td9_checks[timespan]) for timespan in timespans}
    td9_signals = await asyncio.gather(*td9_signals.values())

    td9_dict = {interval_dict[timespan]: td9_signals[i] for i, timespan in enumerate(timespans)}
    return td9_dict
