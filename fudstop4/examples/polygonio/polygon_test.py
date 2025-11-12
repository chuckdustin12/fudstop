from apis.polygonio.async_polygon_sdk import Polygon
import os

import asyncio

poly = Polygon(host='localhost', user='chuck', database='fudstop3', password='fud', port=5432)


# Creating a database configuration dictionary using os.environ.get for environment variables
db_config = {
    "DB_HOST": os.environ.get('DB_HOST', 'localhost'),
    "DB_PASSWORD": os.environ.get('DB_PASSWORD'),
    "DB_NAME": os.environ.get('COMPANY_INFO_STRING'),
    "DB_PORT": os.environ.get('DB_PORT', 5432),
    "DB_USER": os.environ.get('DB_USER')
}



async def ticker_snapshot():

    ticker_data = await poly.get_all_tickers(save_all_tickers=True)


    print(f"Number of tickers found: {len(ticker_data)}")

    #easily print attributes / work with attributes using dot notation:

    for attribute in ticker_data:


        #printing the parents
        print(f"Last Trade:",attribute.last_trade)
        print(f"Last Quote:",attribute.stock_last_quote)
        print(f"Day:",attribute.stock_day)
        print(f"Prev Day:",attribute.prev_day)
        print(f"Minute:", attribute.stock_minute_bar)


        #printing the children within a parent

        """
        >>> Last trade example:
        """

        last_trade = attribute.last_trade.conditions
        print(last_trade)


asyncio.run(ticker_snapshot())