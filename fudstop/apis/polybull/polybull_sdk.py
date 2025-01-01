from fudstop.apis.polygonio.polygon_database import PolygonDatabase
from fudstop.apis.webull.webull_options.webull_options import WebullOptions
from fudstop.apis.polygonio.polygon_options import PolygonOptions



class PolybullSDK:
    def __init__(self):
        self.db = PolygonDatabase()
        self.opts = WebullOptions(database='market_data', user='chuck')
        self.poly_opts = PolygonOptions(database='market_data')


    async def get_atm_snapshots(self, ticker:str="AAPL"):
       # _, __, data = await self.opts.all_options(ticker)

        #price = _.close

        #lower_strike = float(price) * 0.10

       # upper_strike = float(price) * 1.10


        options = await self.poly_opts.get_option_chain_all(underlying_asset=ticker, expiration_date='2024-06-03')

        print(options.implied_volatility)
