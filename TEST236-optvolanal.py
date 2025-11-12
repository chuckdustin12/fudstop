import asyncio
import aiohttp
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.helpers import generate_webull_headers
import pandas as pd
poly_opts = PolygonOptions()
wb_opts = WebullOptions()










async def fetch_option_data(session: aiohttp.ClientSession, url: str) -> dict:
    """
    Makes an HTTP GET request to the given URL using the session.
    Returns the parsed JSON response.
    """
    async with session.get(url) as response:
        response.raise_for_status()  # Raises an error for 4xx/5xx status
        data = await response.json()
        return data


def parse_webull_data(json_data: dict) -> pd.DataFrame:
    """
    Given the JSON structure returned by the queryVolumeAnalysis endpoint,
    parse it into a pandas DataFrame and explode the dates column.
    """
    if "datas" not in json_data:
        return pd.DataFrame()  # Return an empty DataFrame if data is missing

    # Convert "datas" to DataFrame
    df = pd.DataFrame(json_data["datas"])

    # Convert string columns to numeric
    df["price"] = pd.to_numeric(df["price"])
    df["buy"]   = pd.to_numeric(df["buy"])
    df["sell"]  = pd.to_numeric(df["sell"])
    df["volume"] = pd.to_numeric(df["volume"])
    df["ratio"] = pd.to_numeric(df["ratio"], errors="coerce")

    # Attach additional info from the top-level of the JSON
    df["avgPrice"]       = json_data.get("avgPrice")
    df["buyVolume"]      = json_data.get("buyVolume")
    df["sellVolume"]     = json_data.get("sellVolume")
    df["neutralVolume"]  = json_data.get("neutralVolume")
    df["tickerId"]       = json_data.get("tickerId")

    # Set the "dates" column to the list of dates
    df["dates"] = [json_data.get("dates", [])] * len(df)

    # -- Explode dates into multiple rows --
    # Each list entry in "dates" is now its own row, while preserving other columns.
    df = df.explode("dates").reset_index(drop=True)

    # Optionally convert them to real datetimes if they are valid date strings
    # df["dates"] = pd.to_datetime(df["dates"], errors="coerce")

    return df



MAX_CONCURRENT_REQUESTS = 500

async def worker(name, session, queue):
    while True:
        option_id = await queue.get()
        if option_id is None:
            break  # shutdown signal

        try:
            url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=2000&tickerId={option_id}"
            result = await fetch_option_data(session=session, url=url)
            if result:
                result= parse_webull_data(result)


                components_query = f"""SELECT ticker,strike,call_put,expiry from wb_opts where option_id = '{option_id}'"""


                results = await poly_opts.fetch(components_query)
                components_df = pd.DataFrame(results, columns=['ticker', 'strike', 'call_put', 'expiry'])
                ticker, strike, call_put, expiry = components_df['ticker'].to_list()[0], components_df['strike'].to_list()[0], components_df['call_put'].to_list()[0], components_df['expiry'].to_list()[0]
                result = result.rename(columns={'neutralVolume': 'neutral_volume', 'tickerId': 'option_id', 'avgPrice': 'avg_price', 'sellVolume': 'sell_volume', 'buyVolume': 'buy_volume', 'dates': 'date'})
                result['strike'] = strike
                result['call_put'] = call_put
                result['expiry'] = expiry
                result['ticker'] = ticker
                result['volume'] = result['volume'].astype(int)
                result['price'] = result['price'].astype(float)
                await poly_opts.batch_upsert_dataframe(result, table_name='volopt_anal', unique_columns=['option_id'])




        except Exception as e:
            print(f"[{name}] ❌ Option {option_id} failed: {e}")
        finally:
            queue.task_done()

async def main():
    await poly_opts.connect()
    queue = asyncio.Queue()
    async with aiohttp.ClientSession(headers=generate_webull_headers()) as session:
        # Launch worker pool
        workers = [
            asyncio.create_task(worker(f"Worker-{i+1}", session, queue))
            for i in range(MAX_CONCURRENT_REQUESTS)
        ]

        # Stream in data from generator, enqueue as you go
        async for options in wb_opts.all_options_generator():
            option_ids = options["option_id"].tolist()
            for opt_id in option_ids:
                await queue.put(opt_id)

        # Wait for all tasks to complete
        await queue.join()

        # Send shutdown signal to workers
        for _ in range(MAX_CONCURRENT_REQUESTS):
            await queue.put(None)

        await asyncio.gather(*workers)

asyncio.run(main())
