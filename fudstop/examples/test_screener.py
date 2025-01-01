from fudstop.apis.webull.webull_paper_trader import WebulScreener


screener = WebulScreener()



import asyncio
from fudstop.apis.webull.webull_options import WebullOptionsData
import requests

async def main():
    r = requests.get("https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds=1040230807,1040231298,1040230811,1040231397,1040231421,1040230589,1040231323,1040230617,1040230680,1040230731,1040230704,1040230491,1040229914,1040230151,1040230172,1040231333,1040230662", headers=screener.headers).json()
   
    
    data = WebullOptionsData(r)


    print(data.askVolume)
    


asyncio.run(main())