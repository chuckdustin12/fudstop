import os
from dotenv import load_dotenv
load_dotenv()
from discord_webhook import AsyncDiscordWebhook ,DiscordEmbed
import pandas as pd


from apis.polygonio.mapping import option_condition_desc_dict, stock_condition_desc_dict
from _markets.list_sets.dicts import hex_color_dict as hex_colors
fudstop = os.environ.get('fudstop_logo')
import asyncpg
from asyncpg.exceptions import UniqueViolationError
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions(database='fudstop3')
import aiohttp
vwap_diff=os.environ.get('vwap_diff')
five_1k=os.environ.get('five_1k')
onek_tenk=os.environ.get('one_tenk')
tenk_fiftyk=os.environ.get('tenk_fiftyk')
fiftyk_plus=os.environ.get('fiftyk_plus')
specials=os.environ.get('specials')
from fudstop4.apis.webull.webull_trading import WebullTrading as AsyncWebullSDK

import anyio
from apis.helpers import format_large_number
import httpcore
from datetime import datetime
import pytz

# Get the current time in UTC
now_utc = datetime.now(pytz.utc)

# Convert UTC time to Eastern Time
eastern = pytz.timezone('US/Eastern')
now_eastern = now_utc.astimezone(eastern)
db = PolygonOptions(host='localhost', user='chuck', database='fudstop3', password='fud',port=5432)
# Format the datetime object to the desired format (YYYY-MM-DD HH:MM:SS)
formatted_time = now_eastern.strftime('%Y-%m-%d %H:%M:%S')

webull = AsyncWebullSDK()


fire_sale = os.environ.get('fire_sale')
accumulation = os.environ.get('accumulation')
neutral_zone = os.environ.get('neutral_zone')
specials2 = os.environ.get('specials_2')


import asyncio

class Embeds(DiscordEmbed):
    def __init__(self, data=None):
        self.data = data

        self.underlying_symbol = data.get('underlying_symbol')
        self.strike = data.get('strike')
        self.expiry = data.get('expiry')
        self.call_put = data.get('call_put')
        self.option_symbol = data.get('trade_option_symbol')
    async def trade_embed(self, webhook_url):
 
        hook = AsyncDiscordWebhook(webhook_url)
        underlying_symbol = self.underlying_symbol
        embed = DiscordEmbed(title=f"{underlying_symbol} ${self.strike} {self.call_put} {self.expiry}", description=f"```py\nA large trade was just conducted for **{self.option_symbol}**```", color=hex_colors['yellow'])

        embed.add_embed_field(name=f"Trade Info:", value=f"> Size: **{self.data.get('size')}**\n> Price: **{self.data.get('price')}")
        embed.add_embed_field(name=f"Exchange:", value=f"> **{self.data.get('exchange')}**")
        embed.add_embed_field(name=f"Conditions:", value=f"> **{self.data.get('conditions')}**")


        hook.add_embed(embed)
        asyncio.create_task(hook.execute())


async def create_rsi_embed(symbol, timespan,webhook_url, color, rsi, status, conn):

    webhook = AsyncDiscordWebhook(webhook_url, content="<@375862240601047070>")
    embed = DiscordEmbed(title=f"RSI | {status} - {symbol} | {timespan}", description=f'```py\n{symbol} is currently trading with an RSI of {rsi}```', color=color)
    embed.add_embed_field(name=f"-", value='```py\nPair with other channels.```')
    embed.set_thumbnail(url=fudstop)
    embed.set_footer(text=f'{symbol} Data by Polygon.io | Implemented by FUDSTOP')

    embed.set_timestamp()
    timestamp = embed.timestamp
    embed.set_footer(text=f'{symbol} | {rsi} | {timespan} | {status}')
    webhook.add_embed(embed)
    asyncio.create_task(webhook.execute())
    try:
        # Splitting the timestamp into date and time
        date, time = timestamp.split('T')
        time = time.split('.')[0]  # Removing milliseconds if present
        datetime_string = date + ' ' + time
        datetime_object = pd.to_datetime(datetime_string)

        # Define the UTC and Eastern time zones
        utc_zone = pytz.timezone('UTC')
        eastern_zone = pytz.timezone('US/Eastern')

        # Assume the original time is in UTC, convert it to Eastern Time
        eastern_datetime = datetime_object.tz_localize(utc_zone).tz_convert(eastern_zone)
        # Convert the timezone-aware datetime to a timezone-naive datetime in UTC
        naive_utc_datetime = eastern_datetime.astimezone(pytz.utc).replace(tzinfo=None)
        try:
            await conn.execute('''
                INSERT INTO rsi_status(timespan, ticker, rsi, status)
                VALUES($1, $2, $3, $4)
                ''', timespan, symbol, rsi[0], status)

        except Exception as e:
            print(f"An error occurred: {e}")



    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle other exceptions
    except UniqueViolationError:
        # Handle the case where the insert would violate a unique constraint
        pass
    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle other exceptions

async def td9_embed(hook, ticker, state,
                     timeframe):

    


    webhook = AsyncDiscordWebhook(hook, content=f"<@375862240601047070>")
    embed = DiscordEmbed(title=f"TD9 Signal - {ticker}", description=f"```py\nComponents: The TD9 indicator consists of two main parts: the TD" "Setup and the TD Countdown. Both parts are designed to help traders identify exhaustion points in a trend.```" 

    "```py\nTD Setup: This phase consists of nine consecutive price bars, each closing higher (for a buy setup) or lower (for a sell setup) than" "the close four bars earlier. A buy setup occurs after a downtrend, signaling potential bullish reversal, while a sell setup occurs after an" "uptrend, signaling potential bearish reversal.```"

    "```py\nTD Countdown: Following the completion of the setup, the countdown phase begins, consisting of thirteen price bars. The countdown helps to refine the timing of a potential reversal.```")
    embed.add_embed_field(name=f"Current TD9:", value=f"> **{state}**\n> **{timeframe}**")

    embed.set_timestamp()
    embed.set_footer(text=f"{ticker} | {state} | {timeframe}")
    webhook.add_embed(embed)
    await webhook.execute()


async def stock_conditions_embed(webhook_url, data):
    conditions = data.get('trade_conditions')
    if 'cash sale' in conditions:
        description = f"```py\nA transaction which requires delivery of securities and payment on the same day the trade takes place.```"
    size = data.get('trade_size')
    price = data.get('trade_price')
    exchange = data.get('exchange')
    ticker = data.get('stock_symbol')

    hook = AsyncDiscordWebhook(webhook_url)
    embed = DiscordEmbed(title=f"{ticker} - {conditions}", description=f"{stock_condition_desc_dict.get(conditions[0])}")

    embed.add_embed_field(name=f"Trade Info:", value=f"> Size: **{data.get('size')}**\n> Price: **{data.get('price')}")
    embed.add_embed_field(name=f"Exchange:", value=f"> **{data.get('exchange')}**")
    embed.add_embed_field(name=f"Conditions:", value=f"> **{data.get('conditions')}**")


    hook.add_embed(embed)
    asyncio.create_task(hook.execute())



async def create_volume_analysis_embed(symbol, type, description, hook, color, r):
    #logo = await poly.get_polygon_logo(symbol)
    h = r.web_stock_high
    l = r.web_stock_low
    o = r.web_stock_open
    c = r.web_stock_close
    f2l = r.fifty_low
    f2h = r.fifty_high
    vol = r.web_stock_vol
    change_ratio = r.web_change_ratio
    webhook = AsyncDiscordWebhook(hook, content=f"<@375862240601047070>")

    embed = DiscordEmbed(title=f"Volume Analysis - {type}", description=description,color=color)
    embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${o}**\n> High: **${h}**\n> Low: **${l}**\n> Now: **${c}**\n> Vol: **{vol}**\n> Change: **{change_ratio}**", inline=False)
    embed.add_embed_field(name=f"52 Stats:", value=f"> High: **${f2h}**\n> Now: **${c}**\n> Low: **${f2l}**")

    embed.set_footer(text=f"{symbol} | {type} | {c} | {f2h} | {f2l}")
    webhook.add_embed(embed)
    await webhook.execute()
        


async def itm_otm_embed(
    ticker, 
    expiry1, itm_otm1, call_put1, total_volume1, total_open_interest1, dollar_amount_oi1, oi_change1,
    expiry2=None, itm_otm2=None, call_put2=None, total_volume2=None, total_open_interest2=None, dollar_amount_oi2=None, oi_change2=None
):
    embed = DiscordEmbed(title=f"{ticker} - ITM vs. OTM by Expiry", description=f'```py\nYou are viewing ITM/OTM contracts by expiration for {ticker}. Each page of data represents one expiration date with accompanying CALL/PUT data.```', color=hex_colors['yellow'])
    
    # First row data
    embed.add_embed_field(name=f"{itm_otm1}", value=f"{call_put1}", inline=False)
    embed.add_embed_field(name=f"Total Volume:", value=f"> {total_volume1}")
    embed.add_embed_field(name=f"Total Open Interest:", value=f"> {total_open_interest1}")
    embed.add_embed_field(name=f"Dollar Amount OI:", value=f"> {dollar_amount_oi1}")
    embed.add_embed_field(name=f"OI Change:", value=f"> {oi_change1}")
    embed.set_timestamp()
    embed.set_footer(text=f'Expiry: **{expiry1}**')
    
    # Second row data, if available
    if expiry2 is not None:
        embed.add_embed_field(name=f"{itm_otm2}", value=f"{call_put2}", inline=False)
        embed.add_embed_field(name=f"Total Volume:", value=f"> {total_volume2}")
        embed.add_embed_field(name=f"Total Open Interest:", value=f"> {total_open_interest2}")
        embed.add_embed_field(name=f"Dollar Amount OI:", value=f"> {dollar_amount_oi2}")
        embed.add_embed_field(name=f"OI Change:", value=f"> {oi_change2}")
        embed.set_footer(text=f'Expiry: **{expiry1}**, **{expiry2}**')

    return embed

    

async def send_td9_embed(timespan, hook, ticker, state):

    webhook = AsyncDiscordWebhook(hook, content=f"<@375862240601047070>")

    embed = DiscordEmbed(title=f"TD9 Signal - {ticker}", description=f"```py\nComponents: The TD9 indicator consists of two main parts: the TD" "Setup and the TD Countdown. Both parts are designed to help traders identify exhaustion points in a trend.```" 

    "```py\nTD Setup: This phase consists of nine consecutive price bars, each closing higher (for a buy setup) or lower (for a sell setup) than" "the close four bars earlier. A buy setup occurs after a downtrend, signaling potential bullish reversal, while a sell setup occurs after an" "uptrend, signaling potential bearish reversal.```"

    "```py\nTD Countdown: Following the completion of the setup, the countdown phase begins, consisting of thirteen price bars. The countdown helps to refine the timing of a potential reversal.```")
    embed.add_embed_field(name=f"Current TD9:", value=f"> **{state}**\n> **{timespan}**")
    embed.set_timestamp()
    # if image_path is not None:
    #     with open(image_path, "rb") as f:
    #         file_content = f.read()
        # webhook.add_file(file=file_content, filename="screenshot.jpg")
    # embed.set_image(url="attachment://screenshot.jpg")
    embed.set_footer(text=f"{ticker} | {timespan} | {state} | data by polygon.io | Implemented by FUDSTOP", icon_url=fudstop)
    webhook.add_embed(embed)

    asyncio.create_task()
    asyncio.create_task(webhook.execute())



async def upside_downside_embed(ticker, hook, sentiment, timespan, color, rsi, conn):
    
    webhook = AsyncDiscordWebhook(hook, content=f"<@375862240601047070>")
    more_data = await webull.get_webull_stock_data(ticker)
    avg10d_vol = more_data.avg_10d_vol
    stock_vol = more_data.web_stock_vol
    avg3m_vol = more_data.avg_vol3m

    #rsi_snapshot = await poly.rsi_snapshot(ticker)
    title = f"💫Momentum💫Scalps💫 {ticker} {timespan}"
    description=f"```py\nThis feed represents a {sentiment} setup - where the RSI is {sentiment} as well as an iminent {sentiment} cross. This feed triggered for the {timespan} timeframe.```"
    embed = DiscordEmbed(title=title, description=description, color=color)
    embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${more_data.web_stock_open}**\n> High: **${more_data.web_stock_high}**\n> Low: **${more_data.web_stock_low}**\n> Last: **${more_data.web_stock_close}**")
    if avg10d_vol is not None and stock_vol is not None and avg3m_vol is not None:
        embed.add_embed_field(name=f"Volume Snapshot:", value=f"> Today: **{float(more_data.web_stock_vol):,}**\n> Avg 10D: **{float(more_data.avg_10d_vol):,}**\n> Avg 3M: **{float(more_data.avg_vol3m):,}**")
        embed.add_embed_field(name=f"52 week Stats:", value=f"> High: **${more_data.fifty_high}**\n> Now: **${more_data.web_stock_close}**\n> Low: **${more_data.fifty_low}**")
        #embed.add_embed_field(name=f"RSI Snapshot:", value=f"```py\n{rsi_snapshot}```")
        #embed.set_thumbnail(await poly.get_polygon_logo(ticker))
        webhook.add_embed(embed)
        embed.set_timestamp()
        embed.set_footer(text=f'{ticker}')
        asyncio.create_task(webhook.execute())

        try:
            
            
            # Insert data into the market_data table
            await conn.execute('''
                INSERT INTO momentum_scalps(ticker, timeframe, move, insertion_timestamp)
                VALUES($1, $2, $3, NOW())
                ''', ticker, timespan, sentiment)
            
            # Close the connection if you're not using a connection pool
            await conn.close()
        except UniqueViolationError:
            pass

async def create_unusual_embed(hook, datas):
    webhook = AsyncDiscordWebhook(hook, content='<@375862240601047070>')
    datas.changeRatio = None

    if datas.changeRatio is not None:

        if round(float(datas.changeRatio)*100,2) > 0:
            color = hex_colors['green']

        elif round(float(datas.changeRatio)*100,2) < 0:
            color = hex_colors['red']

        elif round(float(datas.changeRatio)*100,2) == 0:
            color = hex_colors['grey']

        strike = datas.strikePrice
        expiry = datas.expireDate
        contract_type = datas.direction
        underlying_ticker = datas.unSymbol
        ask = datas.askPrice

        gamma = datas.gamma
        delta = datas.delta
        theta = datas.theta
        vega = datas.vega
        open = datas.open
        close = datas.close
        high = datas.high
        low = datas.low
        volume = datas.volume
        open_interest = datas.openInterest
        implied_volatility = datas.impVol
        bid = datas.bidPrice
        oi_change = datas.openIntChange

        if delta is not None and gamma is not None and vega is not None and theta is not None:
            embed = DiscordEmbed(title=f"Unusual Options - {datas.unSymbol}", description=f"```py\n{underlying_ticker} ${strike} {contract_type} {expiry} is considered UNUSUAL because there is more volume than open interest for this option. This indicates unusual money-flow into the option.```", color=color)
            embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${open}**\n> High: **${high}**\n> Low: **${low}**\n> Close: **${close}**")
            embed.add_embed_field(name=f"IV vs. OI:", value=f"> Volume: **{volume}**\n> OI: **{open_interest}**")

            embed.add_embed_field(name=f"OI Change:", value=f"> **{oi_change}**")
            embed.add_embed_field(name=f"Bid/Ask:", value=f"> Bid: **${bid}**\n> Ask: **${ask}**")
            embed.add_embed_field(name=f"Greeks:", value=f"> Delta: **{round(float(delta),2)}**\n> Gamma: **{round(float(gamma),2)}**\n> Theta: **{round(float(theta),2)}**\n> Vega: **{round(float(vega),2)}**\n> IV: **{round(float(implied_volatility),2)}**")
            embed.set_footer(text=f"UOA | {underlying_ticker} | {strike} | {contract_type} | {expiry} | {volume} | vs | {open_interest} | {oi_change}")
            embed.set_timestamp()
            webhook.add_embed(embed)
            await webhook.execute()



async def create_stock_embed(symbol, stock_data, type, webhook):
    embed = None
    #logo = await poly.get_polygon_logo(symbol)
    
    f2h = stock_data.fifty_high
    f2l = stock_data.fifty_low
    av3m = stock_data.avg_vol3m
    av10d = stock_data.avg_10d_vol
    o=stock_data.web_stock_open
    h=stock_data.web_stock_high
    l=stock_data.web_stock_low
    c=stock_data.web_stock_close
    cr=f"{round(float(stock_data.web_change_ratio)*100,2)}"
    name=stock_data.web_name
    vr=stock_data.web_vibrate_ratio
    v=stock_data.web_stock_vol
    
    if type == '52 high':
        hook = AsyncDiscordWebhook(webhook)
     
        embed=DiscordEmbed(title=f"New 52 High - {symbol}", description=f"```py\n{symbol} is currently pushing its' 52-week high of ${f2h} at the time of this feed.```", color=hex_colors['red'])
        embed.add_embed_field(name=f"Day Stats:", value=f"> O: **${o}**\n> H: **${h}**\n> L: **${l}**\n> C: **${c}**\n> Change: **{cr}%**")
        embed.add_embed_field(name=f"Volume Snapshot:", value=f"> Day: **{float(v):,}**\n> Avg.10D: **{float(av10d):,}**\n> Avg.3M: **{float(av3m):,}**")
        embed.add_embed_field(name=f"Vibration:", value=f"> **{vr}**")
        embed.add_embed_field(name=f"52 week stats:", value=f"> High: **${f2h}**\n> Now: **${c}**\n> Low: **${f2l}**")
        #embed.set_thumbnail(logo)
        embed.set_timestamp()
        embed.set_footer(text=symbol)

        hook.add_embed(embed)
        await asyncio.sleep(0.6)
        await hook.execute()

    elif type == 'Above Avg Vol':
        hook = AsyncDiscordWebhook(webhook)
        embed=DiscordEmbed(title=f"Above Average Volume - {symbol}", description=f"```py\n{symbol} is currently trading above its average volume by at least 2.5 times.```", color=hex_colors['red'])
        embed.add_embed_field(name=f"Day Stats:", value=f"> O: **${o}**\n> H: **${h}**\n> L: **${l}**\n> C: **${c}**\n> Change: **{cr}%**")
        embed.add_embed_field(name=f"Volume Snapshot:", value=f"> Day: **{float(v):,}**\n> Avg.10D: **{float(av10d):,}**\n> Avg.3M: **{float(av3m):,}**")
        embed.add_embed_field(name=f"Vibration:", value=f"> **{vr}**")
        embed.add_embed_field(name=f"52 week stats:", value=f"> High: **${f2h}**\n> Now: **${c}**\n> Low: **${f2l}**")
        #embed.set_thumbnail(logo)
        embed.set_timestamp()
        embed.set_footer(text=symbol)

        hook.add_embed(embed)
        await hook.execute()  



    elif type == 'Below Avg Vol':
        hook = AsyncDiscordWebhook(webhook)
        embed=DiscordEmbed(title=f"Below Average Volume - {symbol}", description=f"```py\n{symbol} is currently trading BELOW its average volume by at least 2.5 times.```", color=hex_colors['green'])
        embed.add_embed_field(name=f"Day Stats:", value=f"> O: **${o}**\n> H: **${h}**\n> L: **${l}**\n> C: **${c}**\n> Change: **{cr}%**")
        embed.add_embed_field(name=f"Volume Snapshot:", value=f"> Day: **{float(v):,}**\n> Avg.10D: **{float(av10d):,}**\n> Avg.3M: **{float(av3m):,}**")
        embed.add_embed_field(name=f"Vibration:", value=f"> **{vr}**")
        embed.add_embed_field(name=f"52 week stats:", value=f"> High: **${f2h}**\n> Now: **${c}**\n> Low: **${f2l}**")
        #embed.set_thumbnail(logo)
        embed.set_timestamp()
        embed.set_footer(text=symbol)

        hook.add_embed(embed)
        await asyncio.sleep(0.6)
        await hook.execute()  



     
    elif type == 'near 52 high':
        hook = AsyncDiscordWebhook(webhook)

        embed=DiscordEmbed(title=f"NEAR 52 HIGH - {symbol}", description=f"```py\n{symbol} is currently within its' 52-week high of ${f2h} by a margin of 2.5% or less at the time of this feed.```", color=hex_colors['red'])
        embed.add_embed_field(name=f"Day Stats:", value=f"> O: **${o}**\n> H: **${h}**\n> L: **${l}**\n> C: **${c}**\n> Change: **{cr}%**")
        embed.add_embed_field(name=f"Volume Snapshot:", value=f"> Day: **{float(v):,}**\n> Avg.10D: **{float(av10d):,}**\n> Avg.3M: **{float(av3m):,}**")
        embed.add_embed_field(name=f"Vibration:", value=f"> **{vr}**")
        embed.add_embed_field(name=f"52 week stats:", value=f"> High: **${f2h}**\n> Now: **${c}**\n> Low: **${f2l}**")
        #embed.set_thumbnail(logo)
        embed.set_timestamp()
        embed.set_footer(text=symbol)

        hook.add_embed(embed)

        await hook.execute()
        await asyncio.sleep(0.6)
    elif type == 'near 52 high':
        hook = AsyncDiscordWebhook(webhook)

        embed=DiscordEmbed(title=f"NEAR 52 LOW - {symbol}", description=f"```py\n{symbol} is currently within its' 52-week low of ${f2l} by a margin of 2.5% or less at the time of this feed.```", color=hex_colors['green'])
        embed.add_embed_field(name=f"Day Stats:", value=f"> O: **${o}**\n> H: **${h}**\n> L: **${l}**\n> C: **${c}**\n> Change: **{cr}%**")
        embed.add_embed_field(name=f"Volume Snapshot:", value=f"> Day: **{float(v):,}**\n> Avg.10D: **{float(av10d):,}**\n> Avg.3M: **{float(av3m):,}**")
        embed.add_embed_field(name=f"Vibration:", value=f"> **{vr}**")
        embed.add_embed_field(name=f"52 week stats:", value=f"> High: **${f2h}**\n> Now: **${c}**\n> Low: **${f2l}**")
        #embed.set_thumbnail(logo)
        embed.set_timestamp()
        embed.set_footer(text=symbol)




        hook.add_embed(embed)
        await hook.execute()
        await asyncio.sleep(0.6)

        

async def option_condition_embed(price_to_strike,conditions, option_symbol, underlying_symbol, strike, call_put, expiry, price, size, exchange, volume_change, price_change, weekday, hour_of_day, hook):
    """Scans real-time option conditions"""

    if price_to_strike is not None:
        price_to_strike = round(float(price_to_strike),2)
 
    webhook = AsyncDiscordWebhook(hook, content=f"<@375862240601047070>")

    embed = DiscordEmbed(title=f"{underlying_symbol} | ${strike} | {call_put} {expiry}", description=f"```py\n{conditions}```", color=hex_colors['yellow'])

    
    embed.add_embed_field(name=f"Trade Info:", value=f"> Price: **${price}**\n> Size: **{size}**\n> Exchange: **{exchange}**\n> Volume CHange: **{volume_change}**")
    embed.add_embed_field(name=f"Trade Info:", value=f"> Price to Strike: **{price_to_strike}**\n> Price Change: **{price_change}**")

    embed.add_embed_field(name=f"Condition:", value=f"> **{conditions}**")
    embed.add_embed_field(name=f"Time Info:", value=f"> Weekday: **{weekday}**\n> Hour of Day: **{hour_of_day}**")
    embed.set_footer(text=f"{option_symbol} | {underlying_symbol} | {strike} | {call_put} | {expiry} | {conditions}")
    embed.set_timestamp()
    webhook.add_embed(embed)


    asyncio.create_task(webhook.execute())
    

    
    # # Insert data into the market_data table
    # await conn.execute('''
    #     INSERT INTO conditions(option_symbol, ticker, strike, expiry,call_put,size,price,conditions, timestamp)
    #     VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)
    #     ''', option_symbol,underlying_symbol,strike,expiry,call_put,size,price,conditions)
    

async def sized_trade_embed(dollar_cost,expiry, option_symbol, call_put, strike, underlying_symbol, price, price_change, size, volume_change, conditions, exchange, price_to_strike, hour_of_day, weekday):


    
    expiry
    option_symbol
    call_put
    strike
    underlying_symbol
    price 
    price_change 
    size
    volume_change
    conditions
    exchange
    price_to_strike
    hour_of_day
    weekday


    if size >= 500 and size <= 999 and size is not None:
        webhook_url = five_1k
        size_type = '500 to 1k'
    
    if size >= 1000 and size <= 9999 and size is not None:
        webhook_url = onek_tenk
        size_type = '1k to 10k'

    if size >= 10000 and size <= 49999 and size is not None:
        size_type = '10k to 50k'
        webhook_url = tenk_fiftyk

    if size >= 50000 and size is not None:
        webhook_url = fiftyk_plus
        size_type = '50k +'

    if size_type is not None:
        webhook = AsyncDiscordWebhook(webhook_url, content=f"<@375862240601047070>")
        embed = DiscordEmbed(title=f"Large Trade - {size_type} - {underlying_symbol}", description=f"```py\n{underlying_symbol} | ${strike} | {call_put} | {expiry} with {size} volume has just been traded at the price of ${price}```", color=hex_colors['purple'])

        embed.add_embed_field(name=f"Exchange:", value=f"> **{exchange}**")
        embed.add_embed_field(name=f"Condition:", value=f"> **{conditions}**")
        embed.add_embed_field(name=f"Size & Price:", value=f"> **{float(size):,}**\n> **${price}**")
        embed.add_embed_field(name=f"Dollar Cost:", value=f"> **${format_large_number(dollar_cost)}**")
        embed.add_embed_field(name=f"Price to Strike:", value=f"> **{price_to_strike}**")
        embed.set_timestamp()
        embed.set_footer(text=f'{underlying_symbol} | {strike} | {call_put} | {expiry} | {dollar_cost} | {size_type}')


        webhook.add_embed(embed)
        asyncio.create_task(webhook.execute())

            # # Check if the 'option_trades' table exists
            # result = await conn.fetchval('''
            #     SELECT EXISTS (
            #         SELECT 1
            #         FROM information_schema.tables
            #         WHERE table_name = 'option_trades'
            #     );
            # ''')

            # if not result:
            #     # The table does not exist, so create it
            #     await conn.execute('''
            #         CREATE TABLE option_trades (
            #             ticker VARCHAR(255),
            #             strike FLOAT,
            #             expiry DATE,
            #             call_put VARCHAR(10),
            #             option_symbol VARCHAR(255),
            #             size INT,
            #             dollar_cost FLOAT,
            #             timestamp TIMESTAMP,
            #             size_type VARCHAR(255)
            #         );
            #     ''')
            # Insert data into the market_data table
        #     await conn.execute('''
        #         INSERT INTO option_trades(ticker, strike, expiry, call_put,option_symbol,size,dollar_cost,timestamp, size_type)
        #         VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)
        #         ''', underlying_symbol, strike, expiry, call_put, option_symbol, size, dollar_cost, timestamp, size_type)

        # except UniqueViolationError:
        #     print(f'Already Exists - skipping')


async def sized_agg_embed(data):


    
    expiry = data.get('expiry')
    option_symbol = data.get('option_symbol')
    call_put = data.get('call_put')
    strike = data.get('strike')
    underlying_symbol = data.get('underlying_symbol')
    option_symbol = data.get('option_symbol')
    price = data.get('price')
    price_change = data.get('price_change')
    size = data.get('size')
    volume_change = data.get('volume_change')
    conditions = data.get('conditions')
    exchange = data.get('exchange')
    price_to_strike = data.get('price_to_strike')
    hour_of_day = data.get('hour_of_day')
    weekday = data.get('weekday')
    timestamp = data.get('timestamp')
    dollar_cost = (100 * price) * size    
    
    if size >= 500 and size <= 999:
        webhook_url = five_1k
        size_type = '500 to 1k'
    
    elif size >= 1000 and size <= 9999:
        webhook_url = onek_tenk
        size_type = '1k to 10k'

    elif size >= 10000 and size <= 49999:
        size_type = '10k to 50k'
        webhook_url = tenk_fiftyk

    elif size >= 50000:
        webhook_url = fiftyk_plus
        size_type = '50k +'

    webhook = AsyncDiscordWebhook(webhook_url, content=f"<@375862240601047070>")
    embed = DiscordEmbed(title=f"Large Trade - {size_type} - {underlying_symbol}", description=f"```py\n{underlying_symbol} | ${strike} | {call_put} | {expiry} with {size} volume has just been traded at the price of ${price}```", color=hex_colors['purple'])

    embed.add_embed_field(name=f"Exchange:", value=f"> **{exchange}**")
    embed.add_embed_field(name=f"Condition:", value=f"> **{conditions}**")
    embed.add_embed_field(name=f"Size & Price:", value=f"> **{float(size):,}**\n> **${price}**")
    embed.add_embed_field(name=f"Dollar Cost:", value=f"> **${format_large_number(dollar_cost)}**")
    embed.add_embed_field(name=f"Price & Volume Change:", value=f"> **{price_change}**\n> **{volume_change}**")
    embed.set_timestamp()


    embed.set_footer(text=f'{underlying_symbol} | {strike} | {call_put} | {expiry} | {dollar_cost} | {size_type}')
    webhook.add_embed(embed)
    await webhook.execute()

lightbolt="<a:_:1042674986474807388>"
async def create_vwap_diff_embed(data):
    webhook = AsyncDiscordWebhook(vwap_diff, content=f"<@375862240601047070>")
    expiry = data.get('expiry')
    call_put = data.get('call_put')
    if call_put == 'Call':
        color = hex_colors['lime']
    else:
        color = hex_colors['red']
    strike = data.get('strike')
    underlying_symbol = data.get('underlying_symbol')
    option_symbol = data.get('option_symbol')    
    total_volume = data.get('total_volume')
    volume = data.get('volume')
    vwap = data.get('day_vwap')
    official_open = data.get('official_open')
    last_price = data.get('last_price')
    open = data.get('open')
    price_diff = data.get('price_diff')
    moneyness = round(float(data.get('moneyness', 0)),2)
    price_vwap_diff = round(float(data.get('price_vwap_diff', 0)),2)
    price_percent_change = data.get('price_percent_change')
    volume_percent_total = round(float(data.get('volume_percent_total')),2)
    volume_to_price = round(float(data.get('volume_to_price')),2)
    timestamp = data.get('timestamp') 
    embed = DiscordEmbed(title=f"{lightbolt} VWAP Differential {lightbolt} - {underlying_symbol}", description=f"```py\n{underlying_symbol} | ${strike} {call_put} {expiry} is currently trading with a large VWAP differential. This means the current price of the option is well below its' volume weighted average price for the day.```", color=color)
    embed.add_embed_field(name=f"Vol. VS Total:", value=f"> **{volume}**\n> **{total_volume}**")
    embed.add_embed_field(name=f"Volume % of Total:", value=f"> **{volume_percent_total}**")
    embed.add_embed_field(name=f"Contract Price:", value=f"> Open: **${official_open}**\n> Now: **${last_price}**")
    embed.add_embed_field(name=f"Moneyness:", value=f"> **{moneyness}**")
    embed.add_embed_field(name=f"Volume to Price:", value=f"> **{volume_to_price}**")
    embed.add_embed_field(name=f"Price VS. VWAP:", value=f"> **${last_price}** vs. **${vwap}**", inline=False)
    embed.add_embed_field(name=f"Price VWAP Diff:", value=f"> **{price_vwap_diff}**")
    embed.set_timestamp()
    embed.set_footer(text=f"{underlying_symbol} | {strike} | {call_put} | {expiry}")
    webhook.add_embed(embed)
    await webhook.execute()


fireworkds="<a:_:1104966851504652348>"
async def specials_embed(buyvol_percent, midvol_percent, underlyingmultivol_percent,sellvol_percent,sweepvol_percent,neutvol_percent,floorvol_percent,crossvol_percent,multivol_percent,va, data):
    expiry = data.get('expiry')
    call_put = data.get('call_put')
    strike = data.get('strike')
    underlying_symbol = data.get('underlying_symbol')
    option_symbol = data.get('option_symbol')    
    total_volume = data.get('total_volume', 0)
    volume = data.get('volume')
    vwap = data.get('day_vwap')
    official_open = data.get('official_open', 0)
    last_price = data.get('last_price')
    open = data.get('open')
    price_diff = data.get('price_diff', 0)
    moneyness = data.get('moneyness')
    price_vwap_diff = data.get('price_vwap_diff', 0)
    price_percent_change = data.get('price_percent_change', 0)
    volume_percent_total = data.get('volume_percent_total', 0)
    volume_to_price = data.get('volume_to_price', 0)
    timestamp = data.get('timestamp')  
    webhook = AsyncDiscordWebhook(specials, content=f"@everyone")

    embed = DiscordEmbed(title=f"{fireworkds} OPTION SPECIAL - {underlying_symbol} {fireworkds}", description=f"```py\nAn OPTION SPECIAL has triggered for {underlying_symbol} | ${strike} | {call_put} | {expiry}```\n\n> Filters: **VOLUME > 3k**\n> **PRICE TRADED CLOSER TO LOW PRICE**\n> BUY VOLUME % > 50\n> **VOLUME > OI**", color=hex_colors['gold'])
    embed.add_embed_field(name=f"Volume Analysis:",value=f"> Buy: **{buyvol_percent}%**\n> Neut: **{neutvol_percent}%**\n> Sell: **{sellvol_percent}%**\n> Floor: **{floorvol_percent}%**\n> Cross: **{crossvol_percent}%**\n> Multi: **{multivol_percent}%**\n> UnderlyingMulti: **{underlyingmultivol_percent}%**\n> Sweep: **{sweepvol_percent}%**\n> Mid: **{midvol_percent}%**", inline=False)
    embed.add_embed_field(name=f"Contract Price:", value=f"> Open: **${official_open}**\n> High: **${va.highPrice}**\n> Now: **${last_price}**\n> Low: **${va.lowPrice}**\n> VWAP: **${vwap}**\n> Price/Vwap DIFF: **{price_vwap_diff}**")
    embed.add_embed_field(name=f"Volume % of Total:", value=f"> This trade was **{volume_percent_total}%** of the day's volume for this contract.", inline=False)
    embed.add_embed_field(name=f"IV:", value=f"> High: **{va.ivHIGH}%**\n> Now: **{va.IV}%** Low: **{va.ivLOW}%**")
    embed.add_embed_field(name=f"OI vs VOL:", value=f"> OI: **{va.OI}**\n> VOL: **{va.Vol}**")
    embed.add_embed_field(name=f"High ")

    webhook.add_embed(embed)
    await webhook.execute()





async def advanced_snapshot_embed_unusual(contract: dict):
    hook = AsyncDiscordWebhook(os.environ.get('advanced_snapshot_unusual'))
    volume = contract.get('volume')
    underlying_symbol = contract.get('underlying_symbol')
    sym = contract.get('option_symbol')
    exp = contract.get('expiry')
    if exp:
        exp = exp.split(" ")[0]
    strike = contract.get('strike')
    call_put = contract.get('call_put')
    oi = contract.get('oi')
    trade_conditions = contract.get('trade_conditions')
    delta_to_theta = contract.get('delta_to_theta_ratio')
    change_percent = contract.get('change_percent')
    iv = contract.get('iv')
    delta = contract.get('delta')
    vega = contract.get('vega')
    theta = contract.get('theta')
    gamma = contract.get('gamma')
    name = contract.get('name')
    open = contract.get('open')
    high = contract.get('high')
    low = contract.get('low')
    under_price = contract.get('underlying_price')
    close = contract.get('close')
    prev_close = contract.get('previous_close')
    time_to_expiry = contract.get('time_to_expiry')
    trade_size = contract.get('trade_size')
    trade_price = contract.get('trade_price')
    timestamp = contract.get('trade_timestamp')
    cost_of_theta = contract.get('cost_of_theta')
    implied_leverage = contract.get('implied_leverage')
    ask = contract.get('ask')
    bid = contract.get('bid')
    change = contract.get('change')
    ask_size = contract.get('ask_size')
    bid_size = contract.get('bid_size')
    ask_exchange = contract.get('ask_exchange')
    bid_exchange = contract.get('bid_exchange')
    moneyness = contract.get('moneyness')
    liquidity_score = contract.get('liquidity_score')
    trade_exchange = contract.get('trade_exchange')
    extrinsic_value = contract.get('extrinsic_value')

    if gamma is not None and vega is not None and theta is not None and delta is not None:
        gamma = round(float(gamma),2)
        delta = round(float(delta),2)
        vega = round(float(vega),2)
        theta = round(float(theta),2)


    if extrinsic_value is not None:
        extrinsic_value = round(float(extrinsic_value),2)

    if moneyness is not None:
        moneyness = round(float(moneyness),2)


    if cost_of_theta is not None:
        cost_of_theta = round(float(cost_of_theta),2)


    if implied_leverage is not None:
        implied_leverage = round(float(implied_leverage),2)


    if delta_to_theta is not None:
        delta_to_theta = round(float(delta_to_theta),2)


    if iv is not None:
        iv = round(float(iv)*100,2)
    embed = DiscordEmbed(title=f"Unusual Options Feed - {underlying_symbol}", description=f"```py\nContract: {underlying_symbol} | {strike} | {call_put} | {exp}```\n```py\nUnusual options refer to options that are currently trading with more volume than open interest. These feeds in particular are scanning for this scenario but only if the volume >= 1500 in total.```", color=hex_colors['yellow'])

    embed.add_embed_field(name=f"Pricing:", value=f"> Underlying: **${under_price}**\n\n> Contract:\n> Open: **${open}**\n> High: **${high}**\n> Low: **${low}**\n> Close: **${close}**\n> Prev. Close: **${prev_close}**\n> Change Percent: **{change_percent}**\n> Change: **{change}**")
    embed.add_embed_field(name=f"Greeks:", value=f"> Delta: **{delta}**\n> Gamma: **{gamma}**\n> Theta: **{theta}**\n> Vega: **{vega}**\n> IV: **{iv}%**")
    embed.add_embed_field(name=f"Greek Extras:", value=f"> Cost of Theta: **${cost_of_theta}**\n> Delta/Theta Ratio: **{delta_to_theta}**")
    embed.add_embed_field(name=f"Last Trade:", value=f"> Size: **{trade_size}**\n> Price: **${trade_price}**\n> Exchange: **{trade_exchange}**\n> Conditions: **{trade_conditions}**\n> Time: **{timestamp}**")
    embed.add_embed_field(name=f"Last Quote:", value=f"> Ask: **${ask}**\n> Ask Size: **{ask_size}**\n> Ask Exchange: **{ask_exchange}**\n\n> Bid: **${bid}**\n> Bid Size: **{bid_size}**\n> Bid Exchange: **{bid_exchange}**")
    embed.add_embed_field(name=f"Contract Stats:", value=f"> DTE: **{time_to_expiry}**\n> Extrinsic Value: **{extrinsic_value}**\n> Moneyness: **{moneyness}**\n> ImpliedLeverage: **{implied_leverage}**\n> Liquidity Score: **{liquidity_score}**")
    embed.add_embed_field(name=f"Volume & OI:", value=f"> Vol: **{volume}**\n> OI: **{oi}**")
    embed.set_footer(text=f'{sym} Data by Polygon.io | Implemented by FUDSTOP', icon_url=fudstop)
    embed.set_timestamp()
    hook.add_embed(embed)
    await hook.execute()
    



async def advanced_snapshot_embed_oi(contract: dict, hook_url):
    hook = AsyncDiscordWebhook(hook_url)
    volume = contract.get('volume')
    underlying_symbol = contract.get('underlying_symbol')
    sym = contract.get('option_symbol')
    exp = contract.get('expiry')
    if exp:
        exp = exp.split(" ")[0]
    strike = contract.get('strike')
    call_put = contract.get('call_put')
    oi = contract.get('oi')
    trade_conditions = contract.get('trade_conditions')
    delta_to_theta = contract.get('delta_to_theta_ratio')
    change_percent = contract.get('change_percent')
    iv = contract.get('iv')
    delta = contract.get('delta')
    vega = contract.get('vega')
    theta = contract.get('theta')
    gamma = contract.get('gamma')
    name = contract.get('name')
    open = contract.get('open')
    high = contract.get('high')
    low = contract.get('low')
    under_price = contract.get('underlying_price')
    close = contract.get('close')
    prev_close = contract.get('previous_close')
    time_to_expiry = contract.get('time_to_expiry')
    trade_size = contract.get('trade_size')
    trade_price = contract.get('trade_price')
    timestamp = contract.get('trade_timestamp')
    cost_of_theta = contract.get('cost_of_theta')
    implied_leverage = contract.get('implied_leverage')
    ask = contract.get('ask')
    bid = contract.get('bid')
    change = contract.get('change')
    ask_size = contract.get('ask_size')
    bid_size = contract.get('bid_size')
    ask_exchange = contract.get('ask_exchange')
    bid_exchange = contract.get('bid_exchange')
    moneyness = contract.get('moneyness')
    liquidity_score = contract.get('liquidity_score')
    trade_exchange = contract.get('trade_exchange')
    extrinsic_value = contract.get('extrinsic_value')

    if gamma is not None and vega is not None and theta is not None and delta is not None:
        gamma = round(float(gamma),2)
        delta = round(float(delta),2)
        vega = round(float(vega),2)
        theta = round(float(theta),2)


    if extrinsic_value is not None:
        extrinsic_value = round(float(extrinsic_value),2)

    if moneyness is not None:
        moneyness = round(float(moneyness),2)


    if cost_of_theta is not None:
        cost_of_theta = round(float(cost_of_theta),2)


    if implied_leverage is not None:
        implied_leverage = round(float(implied_leverage),2)


    if delta_to_theta is not None:
        delta_to_theta = round(float(delta_to_theta),2)


    if iv is not None:
        iv = round(float(iv)*100,2)
    embed = DiscordEmbed(title=f"Unusual Options Feed - {underlying_symbol}", description=f"```py\nContract: {underlying_symbol} | {strike} | {call_put} | {exp}```\n```py\nUnusual options refer to options that are currently trading with more volume than open interest. These feeds in particular are scanning for this scenario but only if the volume >= 1500 in total.```", color=hex_colors['lime'])

    embed.add_embed_field(name=f"Pricing:", value=f"> Underlying: **${under_price}**\n\n> Contract:\n> Open: **${open}**\n> High: **${high}**\n> Low: **${low}**\n> Close: **${close}**\n> Prev. Close: **${prev_close}**\n> Change Percent: **{change_percent}**\n> Change: **{change}**")
    embed.add_embed_field(name=f"Greeks:", value=f"> Delta: **{delta}**\n> Gamma: **{gamma}**\n> Theta: **{theta}**\n> Vega: **{vega}**\n> IV: **{iv}%**")
    embed.add_embed_field(name=f"Greek Extras:", value=f"> Cost of Theta: **${cost_of_theta}**\n> Delta/Theta Ratio: **{delta_to_theta}**")
    embed.add_embed_field(name=f"Last Trade:", value=f"> Size: **{trade_size}**\n> Price: **${trade_price}**\n> Exchange: **{trade_exchange}**\n> Conditions: **{trade_conditions}**\n> Time: **{timestamp}**")
    embed.add_embed_field(name=f"Last Quote:", value=f"> Ask: **${ask}**\n> Ask Size: **{ask_size}**\n> Ask Exchange: **{ask_exchange}**\n\n> Bid: **${bid}**\n> Bid Size: **{bid_size}**\n> Bid Exchange: **{bid_exchange}**")
    embed.add_embed_field(name=f"Contract Stats:", value=f"> DTE: **{time_to_expiry}**\n> Extrinsic Value: **{extrinsic_value}**\n> Moneyness: **{moneyness}**\n> ImpliedLeverage: **{implied_leverage}**\n> Liquidity Score: **{liquidity_score}**")
    embed.add_embed_field(name=f"Volume & OI:", value=f"> Vol: **{volume}**\n> OI: **{oi}**")
    embed.set_footer(text=f'{sym} Data by Polygon.io | Implemented by FUDSTOP', icon_url=fudstop)
    embed.set_timestamp()
    hook.add_embed(embed)
    await hook.execute()








async def create_newhigh_embed(webhook, symbol, o,h,l,c,cr,f2l,av10d,av3m,vr,f2h,v, type, color):
    if av10d is not None and av3m is not None and v is not None:
        hook = AsyncDiscordWebhook(webhook, content=f"<@375862240601047070>")
        embed=DiscordEmbed(title=f"{type} - {symbol}", description=f"```py\n{symbol} is currently pushing its' {type} of ${f2l} at the time of this feed.```", color=color)
        embed.add_embed_field(name=f"Day Stats:", value=f"> O: **${o}**\n> H: **${h}**\n> L: **${l}**\n> C: **${c}**\n> Change: **{cr}%**")
        embed.add_embed_field(name=f"Volume Snapshot:", value=f"> Day: **{float(v):,}**\n> Avg.10D: **{float(av10d):,}**\n> Avg.3M: **{float(av3m):,}**")
        embed.add_embed_field(name=f"Vibration:", value=f"> **{vr}**")
        embed.add_embed_field(name=f"52 week stats:", value=f"> High: **${f2h}**\n> Now: **${c}**\n> Low: **${f2l}**")
        #embed.set_thumbnail(logo)
        embed.set_timestamp()
        embed.set_footer(text=f"{symbol} | {type} | {c} | {f2h} | {f2l}")

        hook.add_embed(embed)
        await hook.execute()  


async def vol_anal_embed(webhook, symbol, o,h,l,c,cr,f2l,av10d,av3m,vr,f2h,v, type, color, description, buyPct, sellPct, neutPct):
    if v is not None and av10d is not None and av3m is not None:
        hook = AsyncDiscordWebhook(webhook, content=f"<@375862240601047070>")
        embed=DiscordEmbed(title=f"{type} - {symbol}", description=description, color=color)
        embed.add_embed_field(name=f"Day Stats:", value=f"> O: **${o}**\n> H: **${h}**\n> L: **${l}**\n> C: **${c}**\n> Change: **{cr}%**")
        embed.add_embed_field(name=f"Volume Snapshot:", value=f"> Day: **{float(v):,}**\n> Avg.10D: **{float(av10d):,}**\n> Avg.3M: **{float(av3m):,}**")
        embed.add_embed_field(name=f"Volume Analysis:", value=f"> Buy%: **{buyPct}**\n> Sell%: **{sellPct}**\n> Neut%: **{neutPct}**")
        embed.add_embed_field(name=f"Vibration:", value=f"> **{vr}**")
        embed.add_embed_field(name=f"52 week stats:", value=f"> High: **${f2h}**\n> Now: **${c}**\n> Low: **${f2l}**")
        #embed.set_thumbnail(logo)
        embed.set_timestamp()
        embed.set_footer(text=f"{symbol} | {type} | {c} | {f2h} | {f2l}")

        hook.add_embed(embed)
        await hook.execute()  


async def insert_into_table(conn, table_name, data):
    await conn.execute(f"""
        INSERT INTO {table_name} (symbol, open, high, low, close, change_ratio, fifty_two_week_low, avg_10d_vol, avg_vol3m, vibrate_ratio, fifty_two_week_high, volume, type)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    """, data['symbol'], data['open'], data['high'], data['low'], data['close'], data['change_ratio'], data['fifty_two_week_low'], data['avg_10d_vol'], data['avg_vol3m'], data['vibrate_ratio'], data['fifty_two_week_high'], data['volume'], data['type'])


# async def webull_options_embed(opts,data, deriv_id):

#     option_symbol = data.get('option_symbol')
#     underlying_symbol = data.get('underlying_symbol')
#     strike = data.get('strike')
#     expiry = data.get('expiry')
#     call_put = data.get('call_put')
#     total_volume = data.get('total_volume')
#     day_vwap = data.get('day_vwap')
#     volume = data.get('volume')
#     official_open = data.get('official_open')
#     last_price = data.get('last_price')
#     wb_symbol = option_symbol.replace('O:', '')
#     price_vwap_diff=  data.get('price_vwap_diff')
#     price_diff = data.get('price_diff')
#     price_change = data.get('price_percent_change')
#     moneyness = data.get('moneyness')
#     volume_to_price = data.get('volume_to_price')
#     volume_percent_total = data.get('volume_percent_total')
#     async with aiohttp.ClientSession(headers=opts.headers) as session:
#         url=f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={deriv_id}"
#         async with session.get(url) as resp:
#             datas = await resp.json()

        
#             totalNum = datas['totalNum']
#             totalVolume = datas['totalVolume']
#             avgPrice = datas['avgPrice']
#             buyVolume = datas['buyVolume']
#             sellVolume = datas['sellVolume']
#             neutralVolume = datas['neutralVolume']

#             # Calculating percentages and ratios
#             buyPct = (buyVolume / totalVolume) * 100 if totalVolume else 0
#             sellPct = (sellVolume / totalVolume) * 100 if totalVolume else 0
#             nPct = (neutralVolume / totalVolume) * 100 if totalVolume else 0

#             buyRatio = buyVolume / totalVolume if totalVolume else 0
#             sellRatio = sellVolume / totalVolume if totalVolume else 0
#             neutralRatio = neutralVolume / totalVolume if totalVolume else 0


#             if total_volume >= 1000 and buyPct >= 95 and volume_percent_total >= 50:
          
#                 hook = AsyncDiscordWebhook(specials2, content=f"@everyone")
#                 webull_vol_anal_embed = DiscordEmbed(title=f"Option Volume Analysis - {underlying_symbol}", description=f"```py\n{underlying_symbol} ${strike} {call_put} {expiry} - currently trading with:\n\n> Buy: **{round(float(buyPct),2)}%**\n> Neut: **{round(float(nPct),2)}%**\n> Sell: **{round(float(sellPct),2)}%**```", color=hex_colors['magenta'])
#                 webull_vol_anal_embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${official_open}**\n> Now: **${last_price}**\n> VWAP: **${day_vwap}**")
#                 webull_vol_anal_embed.add_embed_field(name=f"Vol. Analysis:", value=f"> Buy: **{buyVolume}**\n> Neut: **{neutralVolume}**\n> Sell: **{sellVolume}**")
#                 webull_vol_anal_embed.add_embed_field(name=f"Trade Vol:", value=f"> Last: **{volume}**\n> AvgPrice: **${avgPrice}**\n> % of total: **{round(float(volume_percent_total),2)}%**")
#                 webull_vol_anal_embed.add_embed_field(name=f"Total Trades:", value=f"> **{totalNum}**")
#                 webull_vol_anal_embed.add_embed_field(name=f"Moneyness:", value=f"> **{round(float(moneyness),4)}**")
#                 webull_vol_anal_embed.add_embed_field(name=f"Stats:", value=f"> Price Diff: **{round(float(price_diff),2)}**\n> Price % Change: **{round(float(price_change),2)}%**\n> Volume to Price: **{round(float(volume_to_price),2)}**", inline=False)

#                 webull_vol_anal_embed.set_footer(f"{underlying_symbol} | {strike} | {call_put} | {expiry} | {buyPct} | {sellPct} | {nPct}")
                

#                 hook.add_embed(webull_vol_anal_embed)

#                 await hook.execute()

#                 await opts.insert_specials(underlying_symbol=underlying_symbol,strike=strike,call_put=call_put,expiry=expiry,buy_pct=buyPct, neutral_pct=nPct, sell_pct=sellPct, official_open=official_open,last_price=last_price,day_vwap=day_vwap,buy_volume=buyVolume,neutral_volume=neutralVolume, sell_volume=sellVolume,last_volume=volume,avg_price=avgPrice,volume_percent_total=volume_percent_total,total_trades=totalNum,moneyness=moneyness,price_diff=price_diff,price_change_pct=price_change,volume_to_price_ratio=volume_to_price,option_id=deriv_id)

#                 await opts.close()




import os
cost_dist_98 = os.environ.get('cost_dist_98')
cost_dist_02 =os.environ.get('cost_dist_02')
async def profit_ratio_98_embed(profit_ratio,cost_dist_98, stock_symbol, price, hex_colors, o, h, l, vol, avg10d, avg3m, buyPct, neutPct, sellPct, fifty_high, fifty_low):
    conn = await db.connect()
    hook = AsyncDiscordWebhook(cost_dist_98, content=f"<@375862240601047070>")

    embed = DiscordEmbed(title=f"Players Profiting - 98%+ {stock_symbol}", description=f"```py\nThis feed is posting tickers with a cost distribution (players profiting) of 98% or more. This typically can indicate a bearish reversal - as most players are currently averaged BELOW the current price of ${price}```", color=hex_colors['red'])

    embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${o}**\n> High: **${h}**\n> Now: **${price}**\n> Low: **${l}**")
    embed.add_embed_field(name=f"Volume Stats:", value=f"> Today: **{round(float(vol),2):,}**\n> Avg10D: **{round(float(avg10d),2):,}**\n> Avg3M: **{round(float(avg3m),2):,}**")
    embed.add_embed_field(name=f"Volume Analysis:", value=f"> Buy: **{round(float(buyPct),2)}%**\n> Neut: **{round(float(neutPct),2)}%**\n> Sell: **{round(float(sellPct),2)}%**")
    embed.add_embed_field(name=f"52 Stats:", value=f"> High: **${fifty_high}**\n> Now: **${price}**\n> Low: **${fifty_low}**")
    embed.add_embed_field(name=f"Players Profiting:", value=f"# > **{profit_ratio}%**")
    embed.set_footer(text=f"{stock_symbol} | {profit_ratio}%")
    embed.set_timestamp()
    hook.add_embed(embed)

    asyncio.create_task(hook.execute())
    data_dict = {
        'type': "98% or more profiting",
        'ticker': stock_symbol,
        'players_profiting': profit_ratio,
        'vol': vol,
        'avg_10d_vol': avg10d,
        'avg_3m_vol': avg3m,
        'buy_pct': buyPct,
        'sell_pct': sellPct,
        'neut_pct': neutPct
    }
    df = pd.DataFrame(data_dict)
    try:
        await db.batch_insert_dataframe(df, table_name='cost_dist')
        

    except UniqueViolationError:
        pass



async def profit_ratio_02_embed(profit_ratio,stock_symbol, price, hex_colors, o, h, l, vol, avg10d, avg3m, buyPct, neutPct, sellPct, fifty_high, fifty_low):
    hook = AsyncDiscordWebhook(cost_dist_02, content=f"<@375862240601047070>")

    embed = DiscordEmbed(title=f"Players Profiting - 98%+ {stock_symbol}", description=f"```py\nThis feed is posting tickers with a cost distribution (players profiting) of 98% or more. This typically can indicate a bearish reversal - as most players are currently averaged BELOW the current price of ${price}```", color=hex_colors['red'])

    embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${o}**\n> High: **${h}**\n> Now: **${price}**\n> Low: **${l}**")
    embed.add_embed_field(name=f"Volume Stats:", value=f"> Today: **{round(float(vol),2):,}**\n> Avg10D: **{round(float(avg10d),2):,}**\n> Avg3M: **{round(float(avg3m),2):,}**")
    embed.add_embed_field(name=f"Volume Analysis:", value=f"> Buy: **{round(float(buyPct),2)}%**\n> Neut: **{round(float(neutPct),2)}%**\n> Sell: **{round(float(sellPct),2)}%**")
    embed.add_embed_field(name=f"52 Stats:", value=f"> High: **${fifty_high}**\n> Now: **${price}**\n> Low: **${fifty_low}**")
    embed.add_embed_field(name=f"Players Profiting:", value=f"# > **{profit_ratio}%**")
    embed.set_footer(text=f"{stock_symbol} | {profit_ratio}")
    embed.set_timestamp()
    hook.add_embed(embed)

    asyncio.create_task(hook.execute())
    data_dict = {
        'type': '2% or less profiting',
        'ticker': stock_symbol,
        'players_profiting': profit_ratio,
        'vol': vol,
        'avg_10d_vol': avg10d,
        'avg_3m_vol': avg3m,
        'buy_pct': buyPct,
        'sell_pct': sellPct,
        'neut_pct': neutPct
    }
    df = pd.DataFrame(data_dict)
    try:
        await db.batch_insert_dataframe(df, table_name='cost_dist')
        

    except UniqueViolationError:
        pass



from typing import List




async def specials_embed_2(underlying_symbol, strike, call_put, expiry, buyPct,nPct,sellPct,official_open,last_price,day_vwap,buyVolume,neutralVolume,sellVolume,volume_percent_total, volume, avgPrice, totalNum, moneyness, price_diff, price_change, volume_to_price, buy_sell_vol_str, deriv_id):
    hook = AsyncDiscordWebhook(specials2, content=f"@everyone")
    webull_vol_anal_embed = DiscordEmbed(title=f"Option Volume Analysis - {underlying_symbol}", description=f"```py\n{underlying_symbol} ${strike} {call_put} {expiry} - currently trading with:\n\n> Buy: **{round(float(buyPct),2)}%**\n> Neut: **{round(float(nPct),2)}%**\n> Sell: **{round(float(sellPct),2)}%**```", color=hex_colors['magenta'])
    webull_vol_anal_embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${official_open}**\n> Now: **${last_price}**\n> VWAP: **${day_vwap}**")
    webull_vol_anal_embed.add_embed_field(name=f"Vol. Analysis:", value=f"> Buy: **{buyVolume}**\n> Neut: **{neutralVolume}**\n> Sell: **{sellVolume}**")
    webull_vol_anal_embed.add_embed_field(name=f"Trade Vol:", value=f"> Last: **{volume}**\n> AvgPrice: **${avgPrice}**\n> % of total: **{round(float(volume_percent_total),2)}%**")
    webull_vol_anal_embed.add_embed_field(name=f"Total Trades:", value=f"> **{totalNum}**")
    webull_vol_anal_embed.add_embed_field(name=f"Moneyness:", value=f"> **{round(float(moneyness),4)}**")
    webull_vol_anal_embed.add_embed_field(name=f"Stats:", value=f"> Price Diff: **{round(float(price_diff),2)}**\n> Price % Change: **{round(float(price_change),2)}%**\n> Volume to Price: **{round(float(volume_to_price),2)}**", inline=False)

    webull_vol_anal_embed.add_embed_field(name="Volume Summary:", value=buy_sell_vol_str, inline=False)
    webull_vol_anal_embed.set_footer(f"{underlying_symbol} | {strike} | {call_put} | {expiry} | {buyPct} | {sellPct} | {nPct}")
    

    hook.add_embed(webull_vol_anal_embed)

    asyncio.create_task(hook.execute())

    # asyncio.create_task(opts.insert_specials(underlying_symbol,strike,call_put,expiry,buyPct,nPct,sellPct,official_open,last_price,day_vwap,buyVolume,neutralVolume,sellVolume,volume,avgPrice,volume_percent_total,totalNum,moneyness,price_diff,price_change,volume_to_price, deriv_id))



async def dip_specials_embed(underlying_symbol,strike,call_put,expiry,buyPct,nPct,sellPct,official_open,last_price,day_vwap,buyVolume,neutralVolume,sellVolume,volume,avgPrice,volume_percent_total,totalNum,moneyness,price_diff,price_change,volume_to_price, buy_sell_vol_str):
    hook = AsyncDiscordWebhook(os.environ.get('dip_specials'), content=f"@everyone")
    webull_vol_anal_embed = DiscordEmbed(title=f"DIP SPECIAL! - {underlying_symbol}", description=f"```py\n{underlying_symbol} ${strike} {call_put} {expiry} - currently trading with:\n\n> Buy: **{round(float(buyPct),2)}%**\n> Neut: **{round(float(nPct),2)}%**\n> Sell: **{round(float(sellPct),2)}%**```", color=hex_colors['magenta'])
    webull_vol_anal_embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${official_open}**\n> Now: **${last_price}**\n> VWAP: **${day_vwap}**")
    webull_vol_anal_embed.add_embed_field(name=f"Vol. Analysis:", value=f"> Buy: **{buyVolume}**\n> Neut: **{neutralVolume}**\n> Sell: **{sellVolume}**")
    webull_vol_anal_embed.add_embed_field(name=f"Trade Vol:", value=f"> Last: **{volume}**\n> AvgPrice: **${avgPrice}**\n> % of total: **{round(float(volume_percent_total),2)}%**")
    webull_vol_anal_embed.add_embed_field(name=f"Total Trades:", value=f"> **{totalNum}**")
    webull_vol_anal_embed.add_embed_field(name=f"Moneyness:", value=f"> **{round(float(moneyness),4)}**")
    webull_vol_anal_embed.add_embed_field(name=f"Stats:", value=f"> Price Diff: **{round(float(price_diff),2)}**\n> Price % Change: **{round(float(price_change),2)}%**\n> Volume to Price: **{round(float(volume_to_price),2)}**", inline=False)

    webull_vol_anal_embed.add_embed_field(name="Volume Summary:", value=buy_sell_vol_str, inline=False)
    webull_vol_anal_embed.set_footer(f"{underlying_symbol} | {strike} | {call_put} | {expiry} | {buyPct} | {sellPct} | {nPct}")


    hook.add_embed(webull_vol_anal_embed)

    asyncio.create_task(hook.execute())

    # asyncio.create_task(self.opts.insert_specials(underlying_symbol,strike,call_put,expiry,buyPct,nPct,sellPct,official_open,last_price,day_vwap,buyVolume,neutralVolume,sellVolume,volume,avgPrice,volume_percent_total,totalNum,moneyness,price_diff,price_change,volume_to_price, deriv_id))



async def index_surge_embed(underlying_symbol, strike, expiry, call_put, buyPct, nPct, sellPct, buyRatio, neutralRatio, sellRatio, totalNum, moneyness, price_diff, price_change, volume_to_price):
    hook = AsyncDiscordWebhook(os.environ.get('index_surge'), content=f"<@375862240601047070>")
    embed = DiscordEmbed(title=f"{underlying_symbol} | {strike} | {call_put} | {expiry}", description='> # INDEX SURGE!\n```py\nThese are INDEX surges for options - large trades on index tickers with buy ratio >= 85% and volume >= 1,500.```', color=hex_colors['green'])

    embed.add_embed_field(name='Volume Analysis',value=f'# > BUY: **{buyPct}%**\n# > NEUTRAL: **{nPct}%**\n# > SELL: **{sellPct}%**')
    embed.add_embed_field(name=f"Volume Ratios:", value=f"> Buy: **{round(float(buyRatio)*100,2)}**\n> Neut: **{round(float(neutralRatio)*100,2)}**\n> Sell: **{round(float(sellRatio)*100,2)}**")

    embed.add_embed_field(name=f"Total Trades:", value=f"> **{totalNum}**")
    embed.add_embed_field(name=f"Moneyness:", value=f"> **{round(float(moneyness),4)}**")
    embed.add_embed_field(name=f"Stats:", value=f"> Price Diff: **{round(float(price_diff),2)}**\n> Price % Change: **{round(float(price_change),2)}%**\n> Volume to Price: **{round(float(volume_to_price),2)}**", inline=False)
    embed.set_timestamp()
    hook.add_embed(embed)
    await hook.execute()



async def dip_specials_embed_2_super(theta,delta,call_put, volume, price, under_sym, strike, expiry, deriv_id, session, oi_change,oi, gamma):
    # Example threshold values
    threshold_volume = 375           # High volume indicating significant interest
    threshold_oi_change = 500          # Significant change in open interest
    threshold_delta_call = 0.5         # For calls, a moderately high delta
    threshold_delta_put = -0.5         # For puts, a moderately high (absolute value) delta
    threshold_change_ratio = 0.05      # 5% change in price
    threshold_percent_move = 0.03      # 3% movement between open and close price
    minimum_expiry_date = 30           # At least 30 days to expiration
    threshold_latest_vol = 5000        # High latest volume for the trade
    threshold_gamma = 0.1              # A notable gamma value
    threshold_theta = -0.02            # A specific theta value (negative for time decay)
    threshold_vega = 0.1               # A notable vega value
    threshold_rho = 0.05               # A noticeable rho value
    if round(float(theta)) <= -0.03 and round(float(delta)) and ((call_put == 'call' and round(float(delta)) > threshold_delta_call) or (call_put == 'put' and round(float(delta)) < threshold_delta_put)) and volume >= threshold_volume and price >= 0.20 and price <= 2.00:
        batch = []
        hook = AsyncDiscordWebhook(os.environ.get('dip_specials'), f"@everyone")
        embed = DiscordEmbed(title=f"Super Special", description=f'{under_sym} | {strike} | {call_put} | {expiry}')
        hook.add_embed(embed)
        await hook.execute()


        url=f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={deriv_id}"
        async with session.get(url) as resp:
            datas = await resp.json()


            totalNum = datas['totalNum']
            totalVolume = datas['totalVolume']
            avgPrice = datas['avgPrice']
            buyVolume = datas['buyVolume']
            sellVolume = datas['sellVolume']
            neutralVolume = datas['neutralVolume']

            # Calculating percentages and ratios
            buyPct = (buyVolume / totalVolume) * 100 if totalVolume else 0
            sellPct = (sellVolume / totalVolume) * 100 if totalVolume else 0
            nPct = (neutralVolume / totalVolume) * 100 if totalVolume else 0

            if buyPct >= 90 and oi_change <= -1000 and volume >= oi * 5 and (call_put == 'call' and delta >= 0.55) or (call_put == 'put' and delta <= -0.55) and gamma >= 0.02:
                hook = AsyncDiscordWebhook(os.environ.get('dip_specials'), content=f"<@375862240601047070>")
                embed = DiscordEmbed(title=f"TEST", description=f'PAPER TRADE THESE <@375862240601047070>\n\n> {under_sym} | ${strike} | {call_put} | {expiry}')
                hook.add_embed(embed)

                await hook.execute()
                batch.clear()



async def theta_resistant_embed(data:dict):

    hook = AsyncDiscordWebhook(os.environ.get('theta_resistant', content=f"<@375862240601047070>"))

    embed = DiscordEmbed(title=f"Theta Resistant | {data.get('under_sym')} | {data.get('strike')} | {data.get('call_put')} | {data.get('expiry')}")


    await hook.add_embed(embed)
    await hook.execute()




async def conditional_embed(webhook, row, color, description, status, sector, db):
    hook = AsyncDiscordWebhook(webhook)

    embed = DiscordEmbed(title=f"{row['ticker']} | ${row['underlying_price']} |{row['strike']} | {row['cp']} | {row['expiry']}", description=f"{description}", color=color)
    embed.add_embed_field(name=f"Contract Stats:", value=f"> OPEN: **${row['open']}**\n> HIGH: **${row['high']}**\n> LOW: **${row['low']}**\n> CLOSE: **${row['close']}**\n> VWAP: **${row['vwap']}**\n> CHANGE%: **{row['change_percent']}**")
    embed.add_embed_field(name=f"IV:", value=f"> **{row['iv']}**\n> Percentile: **{row['iv_percentile']}**")
    embed.add_embed_field(name=f"OI / VOl", value=f"> OI: **{row['oi']}**\n> VOL: **{row['vol']}**\n> RATIO: **{row['vol_oi_ratio']}**")
    embed.add_embed_field(name=f"Value:", value=f"> Intrinsic: **{row['intrinsic_value']}**\n> Extrinsic: **{row['extrinsic_value']}**\n> Time: **{row['time_value']}**")
    embed.add_embed_field(name=f"Spread:", value=f"> Bid: **${row['bid']}**\n> Mid: **${row['mid']}**\n> Ask: **{row['ask']}**\n> Spread: **{round(float(row['spread']),2)}**\n> Spread PCT: **{row['spread_pct']}%**")
    embed.add_embed_field(name="Details:", value=f"> Moneyness: **{row['moneyness']}**\n> Velocity: **{row['velocity']}**\n> Profit Potential: **{row['opp']}**")
    embed.add_embed_field(name=f"GREEKS:", value=f"> Delta: **{row['delta']}**\n> Delta/Theta Ratio: **{row['delta_theta_ratio']}**\n> Gamma: **{row['gamma']}**\n> Gamma Risk: **{row['gamma_risk']}**\n> Vega: **{row['vega']}**\n> Vega Impact: **{row['vega_impact']}**\n> Theta: **{row['theta']}**\n> Decay Rate: **{row['theta_decay_rate']}**", inline=False)
    embed.set_timestamp()
    embed.set_footer(text=f'{type} | Data by Polygon.io | Implemented by FUDSTOP', icon_url=os.environ.get('fudstop_logo'))
    hook.add_embed(embed)
    asyncio.create_task(hook.execute())

    data_dict = { 
        'ticker': row['ticker'],
        'sector': sector,
        'description': description,
        'status': status
    }
   

    df = pd.DataFrame(data_dict, index=[0])

    await db.batch_insert_dataframe(df, table_name='feeds', unique_columns='ticker, status, description')