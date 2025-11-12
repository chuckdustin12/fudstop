from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from fudstop4.apis.helpers import generate_webull_headers

opts = WebullOptions()
import asyncio



async def multi_opts(ticker):

    x = await opts.multi_options(ticker='AMC', headers=generate_webull_headers())


    ids = x.tickerId

    print(ids)


asyncio.run(multi_opts(ticker='AMC'))