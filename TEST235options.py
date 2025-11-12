import asyncio
import aiohttp
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.helpers import generate_webull_headers
import pandas as pd
poly_opts = PolygonOptions()
wb_opts = WebullOptions()
MAX_CONCURRENT_REQUESTS = 50

async def worker(name, session, queue):
    while True:
        option_id = await queue.get()
        if option_id is None:
            break  # shutdown signal

        try:
            result = await wb_opts.fetch_option_data(session=session, option_id=option_id)
            if result:
                print(f"[{name}] ✅ {option_id}")
                datas = result.get('datas')

                print(datas)


                df = pd.DataFrame(datas)
                if not df.empty:
                    df['option_id'] = option_id

                    pair_query = f"""SELECT ticker, strike, call_put, expiry from wb_opts where option_id = '{option_id}'"""
                    results = await poly_opts.fetch(pair_query)
                    pair_df = pd.DataFrame(results, columns=['ticker', 'strike', 'call_put', 'expiry'])
                    ticker,strike,call_put,expiry = pair_df['ticker'].to_list()[0], pair_df['strike'].to_list()[0], pair_df['call_put'].to_list()[0], pair_df['expiry'].to_list()[0]
                    df = df.rename(columns={'tickerId': 'option_id', 'tradeTime': 'timestamp', 'deal': 'price', 'tradeBsFlag':'flag', 'trdEx': 'exchange'})
                    df['strike'] = strike
                    df['call_put'] = call_put
                    df['expiry'] = expiry
                    df['ticker']= ticker
                    df['volume'] = df['volume'].astype(int)
                    df['price'] = df['price'].astype(float)
                    await poly_opts.batch_upsert_dataframe(df, table_name='opt_anal', unique_columns=['option_id'])






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
