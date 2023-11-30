import os
from dotenv import load_dotenv

load_dotenv()



import asyncio


from apis.polygonio.polygon_options import PolygonOptions


opts = PolygonOptions()




async def main():
    x = await opts.get_near_the_money_data('SPY', exp_greater_than='2023-11-10', exp_less_than='2023-12-15')
    
    # Assuming 'x' is a pandas DataFrame and contains 'expiration' and 'velocity' columns.
    # Group by 'expiration' and then sort each group by 'velocity'
    grouped = x.groupby('expiry', group_keys=False).apply(lambda g: g.sort_values('velocity', ascending=True))
    
    print(grouped)



asyncio.run(main())