from apis.polygonio.async_polygon_sdk import Polygon
import os

import asyncio

poly = Polygon(host='localhost', user='chuck', database='fudstop3', password='fud', port=5432)


# Creating a database configuration dictionary using os.environ.get for environment variables
db_config = {
    "DB_HOST": os.environ.get('DB_HOST', 'localhost'),
    "DB_PASSWORD": os.environ.get('DB_PASSWORD'),
    "DB_NAME": os.environ.get('NEWS_STRING'),
    "DB_PORT": os.environ.get('DB_PORT', 5432),
    "DB_USER": os.environ.get('DB_USER')
}



async def market_news():

    news = await poly.market_news(limit='100')

    print(news.author)
    print(news.as_dataframe)


asyncio.run(market_news())