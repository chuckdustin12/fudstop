from apis.polygonio.async_polygon_sdk import Polygon
import os

import asyncio

poly = Polygon(host='localhost', user='chuck', database='market_data', password='fud', port=5432)


# Creating a database configuration dictionary using os.environ.get for environment variables
db_config = {
    "DB_HOST": os.environ.get('DB_HOST', 'localhost'),
    "DB_PASSWORD": os.environ.get('DB_PASSWORD'),
    "DB_NAME": os.environ.get('COMPANY_INFO_STRING'),
    "DB_PORT": os.environ.get('DB_PORT', 5423),
    "DB_USER": os.environ.get('DB_USER')
}



async def company_info():

    news = await poly.company_info('AAPL')



    print(news.address1)

    print(news.description)

    print(news)

asyncio.run(company_info())