from apis.polygonio.async_polygon_sdk import Polygon

poly = Polygon(host='localhost', user='chuck', database='fudstop3', password='fud', port=5432)

import asyncio


async def ticker_snapshot():

    ticker_data = await poly.get_all_tickers(save_all_tickers=True)


    print(f"Number of tickers found: {len(ticker_data)}")

    print(ticker_data)

asyncio.run(ticker_snapshot())