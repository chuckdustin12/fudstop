import requests
from datetime import datetime
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
from fudstop4.apis.helpers import format_large_number
import asyncio
from discord_webhook import AsyncDiscordWebhook,DiscordEmbed
from fudstop_middleware.fudstop_channels import ticker_webhooks
from fudstop4._markets.list_sets.dicts import hex_color_dict
import pytz
import aiohttp
import asyncio
from fudstop4.apis.helpers import generate_webull_headers
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.webull.webull_trading import WebullTrading
trading = WebullTrading()
opts = PolygonOptions()
import pandas as pd
session = requests.session()
ticker_ids = [trading.ticker_to_id_map.get(i) for i in most_active_tickers]
print(ticker_ids)

ticker_webhooks.update({'DJT': 'https://discord.com/api/webhooks/1350186343871025213/GL263tEHviDUXkohwkNMUW_cBrjJ_tRI0bKw4CVTT7HN4rtPY_q8mSyGSxooMLUnIRia', 'LYFT': 'https://discord.com/api/webhooks/1350186821044142253/lhxupjrrnn92IuEhgOLPP4xSKXZWB-9ShQwaJs1H-R9xI9Bmu_PbOmcwCzldapuB5jDM', 'CSX': 'https://discord.com/api/webhooks/1350189195242508328/4xS03aDkfGu_3dJwBhZ3O24J4Cu8EYVHRn7bmvcPLcjYpyQRVh7HrB6VurOQHlxJGxdj', 'AMC': 'https://discord.com/api/webhooks/1345559959290515568/iTgikJE_U-95LngpX0QM5h0l-8XY188ulK_Qd1_QaRIRzViThMIuO1nmIl3VVKb4nQJc'})
processed_ids = set()
async def main():
    await opts.connect()
    alter_type_dict = { 
        1: 'large_buy',
        2: 'large_sell',
        5: 'falling_bid',
        4: 'rising_bid',
        8: 'large_rising_volume',
        9: 'large_falling_volume',
        16: 'rebound',
        17: 'top_reversal',
        18: 'sharp_increase',
        19: 'sharp_decrease',
        20: 'rapid_increase',
        21: 'rapid_decrease',
        22: 'rise_by_7pct',
        23: 'fall_by_7pct',
        

    }
    tasks =[]
    while True:
        payload = {"supportBroker":8,"regionId":6,"sId":0,"limit":200,"tickerIds":[913323987, 913255489, 913257027, 950181408, 913256303, 913255598, 913303928, 913324489, 925418520, 913255353, 950126602, 913256192, 925377113, 913323467, 925284417, 913255490, 913243750, 913255509, 913323997, 950993116, 913255369, 913243249, 913256091, 950988322, 913255289, 913243581, 913243251, 913324459, 913256162, 913254998, 913257561, 913323878, 913255495, 950173560, 913254235, 913324096, 913323709, 913255447, 950979350, 913256180, 913324504, 950177837, 913255053, 913243250, 913256135, 913255414, 950064710, 913255327, 913254891, 950178015, 950136998, 913257299, 913303964, 913324077, 913324002, 913243073, 950178219, 913324421, 950136918, 913247475, 925376726, 913732468, 913254746, 913257435, 913254883, 950172475, 950102542, 913256407, 913255108, 913323778, 913323554, 913255163, 913324114, 925179279, 925415088, 950052670, 913254679, 913323815, 913324123, 913324495, 913255443, 913255007, 913244722, 950145440, 913256248, 913246626, 913324585, 913256043, 913324503, 913323809, 913255993, 950121423, 913254558, 913254903, 916040682, 913257472, 950118595, 913244089, 913255192, 950187715, 950989569, 913424717, 950178170, 950095560, 913255171, 950186274, 913323915, 913253512, 913255162, 913255309, 913244544, 913244725, 913255341, 913324537, 913254872, 913246740, 950153166, 913254303, 913246449, 913256419, 913244796, 913255055, 913243611, 913254895, 950172451, 913324551, 950125342, 913255105, 913424716, 913255033, 913255505, 913255467, 913324070, 913255864, 950179680, 913323901, 913254406, 913324337, 913324497, 913323750, 913255218, 950118597, 913255465, 913255501, 950169475, 913257268, 913255666, 913243154, 950977519, 950169866, 925418532, 913256672, 913324509, 913244540, 913256359, 913323796, 913254999, 913323786, 913255363, 950188842, 913255078, 913324525, 913244915, 950171618, 950178653, 913353636, 913243231, 913256042, 913243139, 913324118, 913255508, 913244503, 913255803, 913244105, 913323300, 913243555, 913255253, 913730084, 913324095, 913243122, 916040691, 913324397, 913243879, 913255266, 913323925, 913243128, 913256217, 913247399, 913354362, 925377660, 925323875, 950188431, 913254559]}
        async with aiohttp.ClientSession(headers=generate_webull_headers()) as session:
            async with session.post("https://quotes-gw.webullfintech.com/api/wlas/portfolio/changes", json=payload) as resp:
                r= await resp.json()
                print(r)

        altert_type = [i.get('alertType') for i in r]
        sid = [i.get('sid') for i in r]
        symbol = [i.get('symbol') for i in r]
        time = [i.get('timeTS') for i in r]
        volume = [float(i['volume']) if i.get('volume') is not None else None for i in r]
        changeRatio = [round(float(i.get('changeRatio'))*100,2) if i.get('changeRatio') is not None else None for i in r]

        # Convert to UTC datetime
        # Convert timestamps to Eastern Time (ET)
        eastern_times = [
            datetime.fromtimestamp(ts / 1000.0, tz=pytz.utc)
            .astimezone(pytz.timezone('US/Eastern'))
            .strftime('%Y:%m:%d %H:%M:%S')
            for ts in time
        ]

        for s, alert, times, v ,cr, id in zip(symbol, altert_type, eastern_times, volume, changeRatio, sid):
            dict = { 
                'ticker': s,
                'alert': alert,
                'volume': v,
                'change_ratio': cr,
                'sid': id
            }
            df = pd.DataFrame(dict, index=[0])
            await opts.batch_upsert_dataframe(df, table_name='alerts', unique_columns=['sid'])
            if id in processed_ids:
                continue

            alerts = alter_type_dict.get(alert)


            hook = ticker_webhooks.get(s)

            if alerts in ['sell', 'decrease', 'falling', 'fall', 'reversal']:

                color = hex_color_dict.get('red')
            else:
                color = hex_color_dict.get('green')

            embed = DiscordEmbed(title=f'{alert} - {s}', description=f"```py\n{s} just alerted with an alert of {alerts}!```", color=color)

            if v is not None and v >= 0.0:
                embed.add_embed_field(name=f"Volume:", value=f"> **{format_large_number(v)}** @ {times}")
            elif cr is not None:
                embed.add_embed_field(name=f"Change%:", value=f"> **{cr}%** @ {times}")


            embed.set_timestamp()
            embed.set_footer(text='Implemented by F.U.D.STOP')


            webhook = AsyncDiscordWebhook(hook)

            webhook.add_embed(embed)


            asyncio.create_task(webhook.execute())

            print(f"Hook has just been sent to the {s} channel using webhook {hook}!")

            processed_ids.add(id)
 
asyncio.run(main())

