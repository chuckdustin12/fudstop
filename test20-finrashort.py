import pandas as pd
import aiohttp
import asyncio
from datetime import datetime, timedelta
from io import StringIO

# Define max concurrent requests
MAX_CONCURRENT_REQUESTS = 5

async def download_finra_data(session, date: str, semaphore):
    """Asynchronously downloads FINRA short volume data for a given date."""
    url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"

    async with semaphore:  # Limits concurrent requests
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return date, await response.text()
                else:
                    print(f"❌ Failed to download data for {date} (HTTP {response.status})")
                    return date, None
        except Exception as e:
            print(f"⚠️ Error fetching data for {date}: {e}")
            return date, None

async def parse_finra_data(raw_data: str):
    """Converts raw FINRA data to a Pandas DataFrame (executed in a thread)."""
    return await asyncio.to_thread(pd.read_csv, StringIO(raw_data), sep='|')

async def fetch_finra_data(start_date: str, end_date: str):
    """Asynchronously fetches FINRA short volume data between two dates."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    dates = [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range((end - start).days + 1)]
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # Limit concurrent downloads

    async with aiohttp.ClientSession() as session:
        tasks = [download_finra_data(session, date, semaphore) for date in dates]
        results = await asyncio.gather(*tasks)

    # Filter out failed downloads
    raw_data_list = [(date, raw_data) for date, raw_data in results if raw_data]

    if not raw_data_list:
        print("❌ No data fetched.")
        return None

    # Parse all data concurrently
    parse_tasks = [parse_finra_data(raw_data) for _, raw_data in raw_data_list]
    parsed_dfs = await asyncio.gather(*parse_tasks)

    # Combine into one DataFrame
    final_df = pd.concat(parsed_dfs, ignore_index=True)
    return final_df


from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions()
async def main():
    today = datetime.today().strftime("%Y%m%d")
    prior_date = (datetime.today() - timedelta(days=250)).strftime("%Y%m%d")
    await opts.connect()
    final_df = await fetch_finra_data(prior_date, today)
    if final_df is not None:
        # Convert column names to lowercase and replace spaces with underscores
        final_df = final_df.rename(columns={'Date': 'date', 'Symbol': 'symbol', 'ShortVolume': 'short_volume', 'ShortExemptVolume': 'short_exempt_volume', 'TotalVolume': 'total_volume', 'Market': 'market'})

        # Add 'percent_shorted' column
        final_df["percent_shorted"] = (final_df["short_volume"] / final_df["total_volume"]) * 100
        await opts.batch_upsert_dataframe(final_df, table_name='short_vol', unique_columns=['symbol'])

# Run the asyncio event loop
asyncio.run(main())
