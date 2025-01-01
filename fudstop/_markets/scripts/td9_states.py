import os
from dotenv import load_dotenv
load_dotenv()
import asyncio
from apis.webull.webull_trading import WebullTrading
from apis.webull.webull_helpers import calculate_countdown,calculate_setup
from discord_webhook import AsyncDiscordWebhook,DiscordEmbed
from apis.polygonio.async_polygon_sdk import Polygon
from list_sets.dicts import hex_color_dict
from list_sets.ticker_lists import most_active_tickers
from discord_webhook import AsyncDiscordWebhook
from aiohttp.client_exceptions import ClientConnectorError, ClientOSError
import httpx




min1_td9= os.environ.get('min1_td9')
min5_td9=os.environ.get('min5_td9')
min15_td9= os.environ.get('min15_td9')
min20_td9=os.environ.get('min20_td9')
min30_td9= os.environ.get('min30_td9')
min60_td9= os.environ.get('min60_td9')
min120_td9=os.environ.get('min120_td9')
min240_td9= os.environ.get('min240_td9')
day_td9=os.environ.get('day_td9')



polygon = Polygon(host='localhost', user='chuck', database='market_data', password='fud', port=5432)



async def scan_for_td9(dataframe):
    if len(dataframe) < 13:
        return False
    for i in range(4, 13):
        if not all(dataframe['Close'].iloc[i] < dataframe['Close'].iloc[i - 4] for i in range(4, 13)):
            return False
    return True
most_active_tickers = set(most_active_tickers)


trading = WebullTrading()




timeframes = ['m1','m5','m15','m20','m30','m60','m120','m240','d1']


async def scan_bars(ticker):
    while True:
        
        all_results=[]
        tasks = []
        for timeframe in timeframes:
            try:
                bars = await trading.get_bars(ticker, timeframe)


                last_thirteen_candles = bars[:13]
                result = await scan_for_td9(last_thirteen_candles)

                if result == True:
                        # Determine the correct timespan for RSI based on the TD9 timespan
                    if timeframe == 'm1':
                        rsi_timespan = 'minute'
                    elif timeframe in ['m5', 'm10', 'm15', 'm20', 'm30', 'm60']:
                        rsi_timespan = 'hour'
                    else:
                        rsi_timespan = 'day'  # or any default you deem appropriate

                    # Fetch the RSI with the determined timespan
                    rsi = await polygon.rsi(ticker, timespan=rsi_timespan, limit=1)

                    rsi = rsi.rsi_value[0]

                    if rsi <= 30:
                        color = hex_color_dict['green']

                    elif rsi >= 70:
                        color = hex_color_dict['red']

                    else:
                        color = hex_color_dict['grey']

                    embed = DiscordEmbed(title=f"TD9 | {ticker} | {timeframe}", description=f"```py\nTD9 is a technical analysis indicator that signals potential trend reversal after 9 consecutive price bars close higher or lower than the close 4 bars earlier. Traders capitalize on it by anticipating reversals, buying after bearish TD9, and selling after bullish TD9 setups. These embeds are color-coded to signify oversold or overbought RSI + TD9.```", color=color)
                    embed.add_embed_field(name=f"RSI: {rsi_timespan}", value=f"> **{rsi}**")
                    if result == True and timeframe == 'm1':
                        hook = AsyncDiscordWebhook(min1_td9, content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()

                    if result == True and timeframe == 'm5':
                        hook = AsyncDiscordWebhook(min5_td9, content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()


                    # elif result == True and timeframe == 'm10':
                    #     hook = AsyncDiscordWebhook(min, content=f"> # TD9 - **{ticker} | {timeframe}**")
                    #     await hook.execute()

                    

                    if result == True and timeframe == 'm15':
                        hook = AsyncDiscordWebhook(min15_td9,content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()


                    elif result == True and timeframe == 'm20':
                        hook = AsyncDiscordWebhook(min20_td9,content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()


                    if result == True and timeframe == 'm30':
                        hook = AsyncDiscordWebhook(min30_td9, content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()


                    if result == True and timeframe == 'm60':
                        hook = AsyncDiscordWebhook(min60_td9, content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()


                    if result == True and timeframe == 'm120':
                        hook = AsyncDiscordWebhook(min120_td9, content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()

                    if result == True and timeframe == 'm240':
                        hook = AsyncDiscordWebhook(min240_td9, content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()


                    if result == True and timeframe == 'd1':
                        hook = AsyncDiscordWebhook(day_td9, content=f"<@375862240601047070>")
                        hook.add_embed(embed)
                        await hook.execute()
            except (TypeError, AttributeError, KeyError, ClientConnectorError,ClientOSError, httpx.ConnectError, httpx.ReadError):
                continue

        await asyncio.gather(*tasks)
                
        

async def scan_all_bars():
    tickers = ['AAPL', 'MSFT', 'SPX', 'SPY', 'U', 'W', 'NVDA', 'AMD', 'SPXU', 'QQQ', 'TSLA', 'AVGO', 'RBLX', 'TQQQ', 'SOXS', 'SPXL', 'MSFT', 'GOOG', 'GOOGL', 'TAL', 'EDU', 'GOTU', 'TUYA']


    tasks = [scan_bars(i) for i in most_active_tickers]


    return await asyncio.gather(*tasks)




asyncio.run(scan_all_bars())