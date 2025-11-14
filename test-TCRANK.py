import asyncio
import aiohttp
import json
import pandas as pd
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
time_horizons = ['Short', 'Middle', 'Long']
rank_types = [1, 2]  # Types to fetch

async def fetch(session, rank_type, time_horizon):
    """
    Fetches the JSON response from Webull API for a given type and time horizon.
    """
    url = f"https://quotes-gw.webullfintech.com/api/wlas/ranking/tc-rank?regionId=6&supportBroker=8&type={rank_type}&rankType=technicalEvents.tc{time_horizon}&pageIndex=1&pageSize=30"
    
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                data = data['data']
                ticker = [i.get('ticker') for i in data]
                values = [i.get('values') for i in data]

                score = [float(i.get('score')) for i in values]
                change_pct = [round(float(i.get('changeRatio'))*100,2) for i in values]
                price = [float(i.get('close')) for i in values]
                ticker = [i.get('symbol') for i in ticker]
                latest_signal = [i.get('lastestSignal').replace(' ','_').replace('-','_').replace('/','_') for i in values]

                dict = { 
                    'ticker': ticker,
                    'signal': latest_signal,
                    'sentiment': rank_type,
                    'time_horizon': time_horizon,
                    'score': score,
                    'price': price,
                    'change_pct': change_pct,
                }

                df= pd.DataFrame(dict)

                return df

            else:
                print(f"❌ Type {rank_type} - {time_horizon}: Error {resp.status}")
                return {(rank_type, time_horizon): None}
    except Exception as e:
        print(f"❌ Type {rank_type} - {time_horizon}: Exception {str(e)}")
        return {(rank_type, time_horizon): None}

async def get_all_signals():
    """
    Fetches data for all combinations of rank types (1,2) and time horizons (Short, Middle, Long).
    """
    results = {}
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, rtype, horizon) for rtype in rank_types for horizon in time_horizons]
        responses = await asyncio.gather(*tasks)
        
        # for response in responses:
        #     results.update(response)

    for df in responses:
        print(df)

async def main():
    all_data = await get_all_signals()



if __name__ == "__main__":
    asyncio.run(main())
