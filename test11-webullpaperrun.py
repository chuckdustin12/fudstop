#!/usr/bin/env python3
import sys
from pathlib import Path

project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from fudstop4.apis.helpers import add_td9_counts, add_bollinger_bands, compute_wilders_rsi, macd_curvature_label, add_obv, add_volume_metrics, generate_webull_headers, add_parabolic_sar_signals
import aiohttp
import asyncio
from asyncio import Semaphore, Lock
import numpy as np
import uuid
import pandas as pd
import time
from numba import njit
import logging
from typing import Dict, Tuple, Any, Union, Optional

# ─── SETUP LOGGING ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ─── IMPORT MARKET HOURS FILTER (YOUR LOCAL MODULE) ──────────────────────────



# ─── ADJUST PROJECT DIRECTORY IMPORTS ─────────────────────────────────────────
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

# ─── IMPORT PROJECT MODULES ───────────────────────────────────────────────────
# Make sure these exist in your environment.

from fudstop4.apis.webull.webull_ta import WebullTA
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers



# ─── GLOBAL OBJECTS AND CONSTANTS ─────────────────────────────────────────────
SEM = Semaphore(50)
ticker_id_cache: Dict[str, int] = {}
ticker_cache_lock = Lock()

# Initialize DB & TA
db = PolygonOptions()
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
            async with session.get(url, headers=generate_webull_headers(), timeout=aiohttp.ClientTimeout(total=10)) as response:
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



# ─── ASYNC FETCH & PROCESS ────────────────────────────────────────────────────
async def fetch_data_for_timespan(
    session: aiohttp.ClientSession,
    ticker: str,
    timespan: str,
    rsi_window: int = 14
) -> Optional[pd.DataFrame]:
    """
    Fetch data for a given ticker/timespan, compute RSI, MACD, TD9, etc.
    Keeping final DataFrame in ascending order so columns line up properly.
    """
    try:
        async with SEM:
            # Check the cache for ticker -> ticker_id
            async with ticker_cache_lock:
                if ticker in ticker_id_cache:
                    ticker_id = ticker_id_cache[ticker]
                else:
                    ticker_id = await ta.get_webull_id(ticker)
                    ticker_id_cache[ticker] = ticker_id

            # Increase `count=` so we have enough bars for TD9
            url = (
                "https://quotes-gw.webullfintech.com/api/quote/charts/query-mini"
                f"?type={timespan}&count=150&restorationType=1&loadFactor=1"
                f"&extendTrading=0&tickerId={ticker_id}"
            )
            data_json = await fetch_with_retries(session, url, headers=generate_webull_headers())

        if not data_json or not isinstance(data_json, list) or len(data_json) == 0:
            logging.warning("No data returned for %s %s", ticker, timespan)
            return None

        raw_data = data_json[0].get('data', [])
        if not raw_data:
            logging.warning("Empty 'data' field for %s %s", ticker, timespan)
            return None

        # Parse
        split_data = [row.split(",") for row in raw_data]
        df = pd.DataFrame(split_data, columns=['ts','o','c','h','l','a','v','vwap'])

        # Convert timestamp
        df['ts'] = pd.to_numeric(df['ts'], errors='coerce')
        df['ts'] = pd.to_datetime(df['ts'], unit='s', utc=True)

        # Webull often returns newest first => reverse to ascending
        df = df.iloc[::-1].reset_index(drop=True)
        print(df)
        for col in ['o','c','h','l','v','vwap','a']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        # We don't need 'a' column
        df.drop(columns=['a'], inplace=True, errors='ignore')


        # Compute technical indicators (all in`` ascending order)
        df = compute_wilders_rsi(df, window=rsi_window)
        df = add_bollinger_bands(df, window=20, num_std=2.0)
        df = add_td9_counts(df)
        df = add_parabolic_sar_signals(df)
        # Add MACD curvature label
        closes_asc = df['c'].to_numpy(dtype=np.float64)
        macd_flag = macd_curvature_label(closes_asc)
        df['macd_curvature'] = macd_flag

        df['timespan'] = timespan
        df['ticker'] = ticker

        return df

    except Exception as e:
        logging.error("Error fetching data for %s %s: %s", ticker, timespan, e)
        return None

async def fetch_and_store(
    session: aiohttp.ClientSession,
    ticker: str,
    timespan: str,
    results: Dict[Tuple[str, str], Optional[pd.DataFrame]]
) -> None:
    df = await fetch_data_for_timespan(session, ticker, timespan)
    results[(ticker, timespan)] = df

async def fetch_all_tickers(tickers: list, timespans: list) -> Dict[Tuple[str, str], Optional[pd.DataFrame]]:
    results: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}
    connector = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(fetch_and_store(session, t, ts, results))
            for t in tickers for ts in timespans
        ]
        await asyncio.gather(*tasks)
    return results

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
async def main() -> None:
    try:
        await db.connect()
        cycle = 0
        # You can choose whichever timespans you prefer
        timespans = ['m1','m5','m30','m15', 'm60','d','w','m','m120','m240']
        sleep_interval = 3  # in seconds

        while True:
            cycle += 1
            cycle_start = time.time()
            logging.info("Starting cycle %d", cycle)

            all_data = await fetch_all_tickers(most_active_tickers, timespans)

            for (ticker, tspan), df in all_data.items():
                if df is None or df.empty:
                    continue

                # The DataFrame is in ascending order: oldest at index=0, newest at index=-1
                # Let's look at the last few rows to see if a TD9 setup is forming.
                logging.info("Ticker: %s | Timespan: %s", ticker, tspan)
                logging.info("Last 3 bars (ascending):\n%s", df.tail(3)[
                    ['ts','o','h','l','c','td_buy_count','td_sell_count','macd_curvature']
                ])

                # The very last row is your newest bar
                newest_row = df.iloc[-1:].copy()
                logging.info("Newest row:\n%s", newest_row)

                # Convert ts to string for DB
                newest_row["ts"] = newest_row["ts"].astype(str)

                # Upsert into DB
                await db.batch_upsert_dataframe(
                    newest_row,
                    table_name='plays',
                    unique_columns=['ticker','timespan']
                )

            cycle_end = time.time()
            total_time = cycle_end - cycle_start
            logging.info("Cycle %d done in %.2f seconds.", cycle, total_time)
            logging.info("Sleeping %d seconds...\n", sleep_interval)
            await asyncio.sleep(sleep_interval)

    except asyncio.CancelledError:
        logging.info("Main loop cancelled.")
    except Exception as e:
        logging.error("Unexpected error in main loop: %s", e)
    finally:
        await db.disconnect()




from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions()
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions

opts = WebullOptions()


seen_records = set()



async def target_strike(ticker: str, target_strike: float):
    """Fetches all option data for a given strike, analyzes volume, and visualizes it (Calls vs Puts)."""
    ticker = ticker.upper()

    x = await opts.multi_options(ticker=ticker, headers=generate_webull_headers())
    df = x.as_dataframe

    if not {'strike', 'expiry', 'option_id', 'call_put'}.issubset(df.columns):
        print(f"Error: Missing required columns in dataframe for {ticker}")
        return

    target_strike = float(target_strike)
    strike_df = df[df['strike'] == target_strike].reset_index(drop=True)

    if strike_df.empty:
        print(f"No data found for strike {target_strike} in {ticker}.")
        return

    # Separate Calls and Puts
    calls_df = strike_df[strike_df['call_put'] == 'call']
    puts_df = strike_df[strike_df['call_put'] == 'put']

    # Get option IDs separately for calls and puts
    call_option_ids = calls_df['option_id'].dropna().astype(str).tolist()
    put_option_ids = puts_df['option_id'].dropna().astype(str).tolist()

    async def fetch_option_data(session, option_id):
        """Fetches option data for a single option_id asynchronously."""
        url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryDeals?count=1000&tickerId={option_id}"
        async with session.get(url) as resp:
            return await resp.json()

    # Process all IDs concurrently
    async with aiohttp.ClientSession(headers=generate_webull_headers()) as session:
        call_tasks = [fetch_option_data(session, option_id) for option_id in call_option_ids]
        put_tasks = [fetch_option_data(session, option_id) for option_id in put_option_ids]

        call_results = await asyncio.gather(*call_tasks)
        put_results = await asyncio.gather(*put_tasks)

    # Flatten trade data
    call_datas = [i.get('datas', []) for i in call_results if isinstance(i, dict)]
    put_datas = [i.get('datas', []) for i in put_results if isinstance(i, dict)]
    call_datas = [item for sublist in call_datas for item in sublist]
    put_datas = [item for sublist in put_datas for item in sublist]

    trade_data = {f"{t} Calls": {"total_volume": 0, "total_value": 0, "transaction_count": 0} for t in ["Buy", "Sell", "Neutral", "Unknown"]}
    trade_data.update({f"{t} Puts": {"total_volume": 0, "total_value": 0, "transaction_count": 0} for t in ["Buy", "Sell", "Neutral", "Unknown"]})

    def process_trade_data(data, suffix):
        """Processes trade data and updates the trade_data dictionary."""
        for i in data:
            price = i.get('deal')
            volume = i.get('volume')
            flag = i.get('tradeBsFlag')

            try:
                price = float(price) if price is not None and price != "" else None
                volume = float(volume) if volume is not None and volume != "" else None
            except ValueError:
                continue

            if price is None or volume is None:
                continue

            trade_type = "Buy" if flag == "B" else "Sell" if flag == "S" else "Neutral" if flag == "N" else "Unknown"
            trade_key = f"{trade_type} {suffix}"

            trade_data[trade_key]["total_volume"] += volume
            trade_data[trade_key]["total_value"] += price * volume
            trade_data[trade_key]["transaction_count"] += 1

    # Process Calls and Puts separately
    process_trade_data(call_datas, "Calls")
    process_trade_data(put_datas, "Puts")

    # Compute average prices
    trade_summary = {}
    for trade_type, data in trade_data.items():
        total_volume = data["total_volume"]
        total_value = data["total_value"]
        avg_price = total_value / total_volume if total_volume > 0 else 0

        trade_summary[f"{trade_type} Volume"] = total_volume
        trade_summary[f"{trade_type} Avg Price"] = round(avg_price, 2)
        trade_summary[f"{trade_type} Transactions"] = data["transaction_count"]

    df_summary = pd.DataFrame([trade_summary])


    return df_summary



import requests
from fudstop4.apis.occ.occ_sdk import occSDK
occ = occSDK()
seen_ids = set()
seen_tickers = set()
async def main():
    await db.connect()
    while True:
        try:
            query = f"""SELECT ticker, timespan, candle_completely_above_upper, candle_completely_below_lower from plays where (candle_completely_above_upper = 't' or candle_completely_below_lower = 't') and timespan in ('m1', 'm5', 'm15', 'm30') order by insertion_timestamp desc limit 1"""

            
            results = await db.fetch(query)

            ticker = [i.get('ticker') for i in results]
            candle_below = [i.get('candle_completely_below_lower') for i in results]
            candle_below = candle_below[0]
            candle_above = [i.get('candle_completely_above_upper') for i in results]
            candle_above = candle_above[0]
            ticker = ticker[0]
            

            if candle_above:
                play_type = 'put'
            elif candle_below:
                play_type = 'call'
            print(play_type)
            try:
                
                if ticker not in seen_tickers:
                    seen_tickers.add(ticker)

                    data = await opts.multi_options(ticker, headers=generate_webull_headers())
                

                        # Ensure `skew_df` is a DataFrame
                    skew_df = data.as_dataframe

                    # Check if required columns exist
                    if all(col in skew_df.columns for col in ['iv', 'strike', 'expiry']):
                        # Group by expiration_date and find the strike price with the lowest IV for each expiry
                        lowest_iv_strikes = skew_df.loc[skew_df.groupby('expiry')['iv'].idxmin(), ['expiry', 'strike', 'iv']]

                        strike = lowest_iv_strikes['strike'].to_list()[0]


                        trade_summary = requests.get(f'https://www.fudstop.io/api/target_strike?ticker={ticker}&target_strike={strike}').json()
                        trade_summary = trade_summary[0]
                        if play_type == 'call':
                            if trade_summary.get('Buy Calls Volume') > trade_summary.get('Sell Calls Volume') and trade_summary.get('Buy Calls Transactions') > trade_summary.get('Sell Calls Transactions'):

                                call_id = trade_summary.get('call_ids').split(',')[0]

                                if call_id not in seen_ids:
                            
                                    payload = {"accountId":24724802,"timeInForce":"DAY","quantity":1,"action":"BUY","tickerId":call_id,"orders":[{"action":"BUY","quantity":1,"tickerId":call_id,"tickerType":"OPTION"}],"checkOrPlace":"PLACE","paperId":1,"tickerType":"OPTION","optionStrategy":"Single","serialId":str(uuid.uuid4())}

                                    trade_call = requests.post("https://act.webullfintech.com/webull-paper-center/api/paper/v1/order/optionPlace", headers=generate_webull_headers(), json=payload)
                                    print(trade_call)

                                    seen_ids.add(call_id)
                        if play_type == 'put':
                            if trade_summary.get('Buy Puts Volume') > trade_summary.get('Sell Puts Volume') and trade_summary.get('Buy Puts Transactions') > trade_summary.get('Sell Puts Transactions'):

                                put_id = trade_summary.get('put_ids').split(',')[0]

                                if put_id not in seen_ids:
                                
                                    payload = {"accountId":24724802,"timeInForce":"DAY","orderType": "MKT", "quantity":1,"action":"BUY","tickerId":put_id,"orders":[{"action":"BUY","quantity":1,"tickerId":put_id,"tickerType":"OPTION"}],"checkOrPlace":"PLACE","paperId":1,"tickerType":"OPTION","optionStrategy":"Single","serialId":str(uuid.uuid4())}

                                    trade_put = requests.post("https://act.webullfintech.com/webull-paper-center/api/paper/v1/order/optionPlace", headers=generate_webull_headers(), json=payload)

                                    print(trade_put)

                                    seen_ids.add(put_id)


            except Exception as e:
                print(e)
        except Exception as e:
            print(e)
asyncio.run(main())

