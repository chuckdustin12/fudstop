from fudstop4.apis.occ.occ_sdk import occSDK
from fudstop4.apis.webull.webull_markets import WebullMarkets
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
opts = WebullOptions()
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions()
wbm = WebullMarkets()
import asyncio
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
occ = occSDK()
from random import randint
from aiohttp.client_exceptions import ClientError

from fudstop4.apis.helpers import generate_webull_headers
from fudstop4.apis.helpers import format_large_numbers_in_dataframe2
import pandas as pd
import requests


# Limit concurrency (e.g., 3-5 tickers at a time)
CONCURRENT_REQUESTS = 5

async def fetch_strike_data(semaphore, ticker):
    """Fetch all strikes for a given ticker with concurrency control and error handling."""
    try:
        async with semaphore:  # Limits concurrent execution
            retries = 3
            for attempt in range(retries):
                try:
                    print(f"Fetching {ticker} (Attempt {attempt+1})...")
                    headers = generate_webull_headers()
                    df = await opts.target_all_strikes(ticker=ticker, headers=headers)

                    if df is not None and not df.empty:
                        df['ticker'] = ticker  # Add ticker for reference

                        await db.batch_upsert_dataframe(df, table_name='strike_analysis', unique_columns=['ticker', 'strike'])
                        return df
                    else:
                        print(f"Warning: No data for {ticker}.")
                        return None

                except (ClientError, asyncio.TimeoutError) as e:
                    print(f"Error fetching {ticker}: {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt + randint(0, 2))  # Exponential backoff
                    else:
                        print(f"Skipping {ticker} after {retries} failed attempts.")
                        return None
    except Exception as e:
        print(e)

async def main():
    """Run batch processing of tickers with concurrency limit."""
    await db.connect()
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)  # Control concurrency
    tasks = [fetch_strike_data(semaphore, ticker) for ticker in most_active_tickers]
    
    results = await asyncio.gather(*tasks)  # Run concurrently
    
    # Filter out None results (failed tickers)
    results = [df for df in results if df is not None]

    if results:
        final_df = pd.concat(results, ignore_index=True)
        print(final_df.head())  # Display first few rows
        
        # Save to CSV or database
        final_df.to_csv("options_data.csv", index=False)
        print("Data saved to options_data.csv")
    else:
        print("No valid data retrieved.")

# Run the async function in an event loop
if __name__ == "__main__":
    asyncio.run(main())