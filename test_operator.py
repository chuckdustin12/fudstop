import os

from dotenv import load_dotenv

load_dotenv()


import asyncio


from fudstop.apis.polygonio.async_polygon_sdk import Polygon
from fudstop.apis.polygonio.polygon_options import PolygonOptions

opts = PolygonOptions()

poly = Polygon()

async def main():
    data = await opts.get_option_chain_all('GME')

    deltas = data.delta
    thetas = data.theta
    strikes = data.strike

    for i in range(len(strikes)):
        if i % 5 == 0:
            # Check if deltas[i] and thetas[i] are not None
            if deltas[i] is not None and thetas[i] is not None:
                if abs(deltas[i]) > 0.5 and thetas[i] < -0.02:
                    print(f"Strike {strikes[i]}: High Delta {deltas[i]}, Negative Theta {thetas[i]}")
                # Additional operations
            else:
                print(f"Data not available for index {i}")


asyncio.run(main())