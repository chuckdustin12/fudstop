from apis.polygonio.async_polygon_sdk import Polygon
import os
import asyncio
poly = Polygon(host='localhost', user='chuck', database='market_data', password='fud', port=5432)


# Creating a database configuration dictionary using os.environ.get for environment variables
db_config = {
    "DB_HOST": os.environ.get('DB_HOST'),
    "DB_PASSWORD": os.environ.get('DB_PASSWORD'),
    "DB_NAME": os.environ.get('DB_NAME'),
    "DB_PORT": os.environ.get('DB_PORT', 5432),
    "DB_USER": os.environ.get('DB_USER')
}



async def main():
    x = await poly.aggregates('I:SPX', timespan='second', date_from='2023-10-27', date_to='2023-10-27', limit=50000)

    await x.create_pool(db_config=db_config)
    await x.create_table()
    
    # Get the data dictionary
    data_dict = x.data_dict

    # Transform data_dict into a list of dictionaries for insertion
    data_dicts = []
    for o, h, l, c, t in zip(data_dict['Open'], data_dict['High'], data_dict['Low'], data_dict['Close'], data_dict['Timestamp']):
        single_record = {
            'Ticker': 'I:SPX',  # Or data_dict['Ticker'] if it's in the dictionary
            'Open': o,
            'High': h,
            'Low': l,
            'Close': c,
            'Timestamp': t
        }
        data_dicts.append(single_record)

    # Insert the list of dictionaries into the database
    await x.insert_data(data_dicts)

asyncio.run(main())