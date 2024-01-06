import asyncio

from apis.webull.webull_markets import WebullMarkets
import os

connection_string = os.environ.get('CONNECTION_STRING')

markets = WebullMarkets()


async def all_market_functions():

    await markets.ipos()
    await markets.highs_and_lows()
    await markets.get_all_etfs(markets.etf_types)

    await markets.get_all_gainers_losers('gainers')
    await markets.get_all_etfs(types=markets.etf_types)

    await markets.get_all_gainers_losers('losers')



    await markets.get_top_options(rank_type='volume')

    await markets.earnings(start_date='2023-10-27')

    await markets.get_forex()


#asyncio.run(all_market_functions())

db_markets = WebullMarkets(connection_string)
async def test():

    x = await db_markets.get_top_options()
    print(x.dtypes)
    

asyncio.run(test())