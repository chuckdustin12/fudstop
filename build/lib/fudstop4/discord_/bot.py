import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
import sys
from pathlib import Path

import re
import disnake
from disnake.ext.commands.errors import CommandInvokeError
from pytrends.request import TrendReq
from disnake.ext import commands
from tabulate import tabulate
from asyncpg.exceptions import UniqueViolationError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fudstop4.apis.webull.webull_paper_trader import WebulScreener, ScreenerSelect
from datetime import datetime

from apis.polygonio.polygon_options import PolygonOptions
import disnake
from apis.y_finance.yf_sdk import yfSDK
import base64
from disnake.ext import commands
from _markets.list_sets.ticker_lists import most_active_tickers
most_active_tickers = set(most_active_tickers)
from discord_.bot_menus.modals.options_modal import OptionsDataModal
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import asyncio
from discord_.bot_menus.pagination import AlertMenus
import aiohttp
from openai import OpenAI
import matplotlib.pyplot as plt
import io
import asyncpg
import pandas as pd

from apis.polygonio.polygon_options import PolygonOptions
from apis.webull.opt_modal import OptionModal, SQLQueryModal
from apis.y_finance.yf_sdk import yfSDK
from apis.gexbot.gexbot import GEXBot
SEC_FILINGS=[1153827348454584443, 1153827546706747455, 1153828102389104771, 1153828288372949062, 1153828427342807110, 1153828752183283752, 1153828753756135424, 1153829229943865344, 1153829615874355240, 1153833634097278976, 1156987190400786452, 1157034088310509568, 1157038070156234843, 1157038883184320624, 1157107615994761236, 1157107859688013866, 1174774750741020752, 1175108609751916606, 1175109001390866473, 1175109208987947108, 1175109603126693958, 1175110342884462604, 1175111342861058099, 1175112249061421207, 1175112431228424262, 1175112609222119467, 1175113168993923082, 1175113782083727504, 1175115817722073168, 1178053558239756288]


MOMENTUM_SCALPS =  [1136061999751631000, 1113439351246946415, 1113563471141941420, 1153564391606849568]


TRADYTICS =  [1112740350059098162, 1112740518753996851, 1112740590845702305, 1112755591358722199, 1112755644357947523, 1112768180469891255, 1112768263911387157, 1151947923970605076, 1112768289303711945, 1112768500923109416, 1112768636961181717, 1151947255167856680, 1151947507069366363]

SECTOR_DARKPOOLS =  [1152761279593840640, 1152761281200279632, 1152761528035070033, 1153325170451288064, 1152761724752105513, 1152761278176170046, 1152761733920858152, 1152761735275630632, 1152761276913680524, 1152761736630390925, 1152761897033154641, 1152761738060644483]

RSI_FEEDS =  [1124185351477526539, 1131240086378393600, 1152294859303043152, 1131238736353562745, 1176615805941723259, 1162153100828754053, 1162153373693399121]

OPTION_FEEDS =  [1134556646278975599, 1154854338167066634, 1077621690131021894, 1165777024091160596, 1156649967214137404, 1165783348443086878, 1156645245287673988, 1156645246847963208, 1156645248932515910, 1156645254460608613, 1165783313907195994, 1165781494040641536, 1165781495739330620, 1165781498096521306, 1165781499212210236, 1167521435514847312]

REDDIT =  [1152766178385731681, 1152766191367098450, 1152766193464262736, 1152766195100024832, 1152766447089627146, 1152968556720431115, 1152973376017682482]

RSS_FEEDS = [1028667813168173167, 1019360302250332301, 1028668345702166698, 1053789985909768254, 1042559047896932362, 1019360339856470146, 1053789646926123086, 1053789818007584938, 1053789896352993341]

STOCK_CONDITIONS = [1149131544422776883, 1149131587670257776, 1161659695157756074, 1161661864443396186, 1161662498278228019, 1161663210236162149, 1161663337726230628, 1174718171459108935]

OPTIONS_CONDITIONS = [1113143417334157332, 1113149881352192030, 1161370852055601163, 1113144168567537795, 1113146480455331840, 1129489929773260872, 1113147180140728383, 1129885678063337474, 1133412240331112448, 1133412527406059600, 1133426263546134568, 1133417470363971794, 1133451695985266738, 1134554830627672135, 1129905458480689242, 1148975826382102568, 1151231796994904094, 1155882061081612318, 1155894099770085476, 1155894448992034857, 1155894842736529490, 1155895232727089162, 1174013415073775726]

OPEN_BB = [1089364481672478780, 1035273203683172434, 1089364148535701544, 1089364318274990191]

STOCK_FEEDS = [1149077047331790848, 1149077345458737172, 1148283428471582870, 1149074180013310093, 1151224045212270732, 1151223877108781169, 1149081836727828550, 1149081838569148497, 1148283426730934303, 1152279428219555840, 1152279430115381248, 1153468681901322460, 1153468687311970384, 1170147962396102757]

CRYPTO = [1162159268598911047, 1178482490864832594]
td9_ids = [
    int("1158471263984029777"),  # Channel: td9⏺5minute
    int("1158488492163211264"),  # Channel: td9⏺day
    int("1158867480853352583"),  # Channel: td9⏺15minute
    int("1158867482824675358"),  # Channel: td9⏺30minute
    int("1158868591911899298"),  # Channel: td9⏺hour
    int("1158868593967108096"),  # Channel: td9⏺2hr
    int("1158868595619672135"),  # Channel: td9⏺4hr
    int("1161334711197630566"),  # Channel: td9⏺20minute
    int("1151905252392575067"),  # Channel: td9[]Minute
    int("1178480622596018226"), # td9 week
    int("1178480910623047822"), # td9 month
]
# List of channel IDs as integers
opt_vol_ids = [
    int("1156645245287673988"),  # Channel: 500➖1k➖vol
    int("1156645246847963208"),  # Channel: 1k➖10k➖vol
    int("1156645248932515910"),  # Channel: 10k➖50k➖vol
    int("1156645254460608613"),
    int("1154854338167066634")   # Channel: 50k➕vol
]

db_config = {
    "host": os.environ.get('DB_HOST', 'localhost'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": os.environ.get('DB_USER'), # Default to 'postgres' if 'DB_USER' not found
    "password": os.environ.get('DB_PASSWORD', 'fud'), # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME', 'polygon') # Database name for the new jawless database
}
opts = PolygonOptions(database='fudstop3')
gexbot = GEXBot()
bot = commands.Bot(command_prefix="!", intents=disnake.Intents.all())

yf = yfSDK()

from _markets.list_sets.ticker_lists import gex_tickers
from typing import List
import disnake
from disnake.ext import commands
import openai

import os
from dotenv import load_dotenv
load_dotenv()


# Function to format timestamp
def format_timestamp(ts):
    if isinstance(ts, str):
        return datetime.fromisoformat(ts).strftime('%y-%m-%d')
    elif isinstance(ts, datetime):
        return ts.strftime('%y-%m-%d')
    else:
        return ts  # In case it's neither a string nor a datetime object

# Apply formatting to td9_data
# Initialize the OpenAI client with your API key
openai.api_key = os.getenv("YOUR_OPENAI_KEY")

import openai



td9_timespan_dict = {
    'm1': '1 minute',
    'm5': '5 minute',
    'm15': '15 minute',
    'm30': '30 minute',
    'm60': '1 hour',
    'm120': '2 hour',
    'm240': '4 hour',
    'd1': 'daily',
    'w': 'weekly',
    'm': 'monthly'
}

@bot.event
async def on_ready():
    guild_id = 888488311927242753  # Replace with your guild's ID
    guild = bot.get_guild(guild_id)

    if guild is None:
        print("Guild not found")
        return

    channels = await guild.fetch_channels()

    for channel in channels:
        try:
            # Attempt to process each channel
            print(f"Channel: {channel.name} - ID: {channel.id}")
        except disnake.errors.InvalidData as e:
            print(f"Encountered an error with channel {channel.id}: {e}")
            # Continue with the next channel
            continue

# now = datetime.now()
# @bot.event
# async def on_message(message: disnake.Message):
#     """Use  GPT4 Vision to listen for image URLs"""

#     if message.author.id == 800519694754644029:
#         message = message.content.split('>')


#     if message.channel.id == 896207280117264434:
#         content = message.content
#         username = message.author.name
#         timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
#         # Assuming you have a connection pool or a single connection to your database
#         conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
        
#         # Insert data into the market_data table
#         await conn.execute('''
#             INSERT INTO messages(message, username, timestamp)
#             VALUES($1, $2, $3)
#             ''', content, username, timestamp)
        
#         # Close the connection if you're not using a connection pool
#         await conn.close()

#     embeds = message.embeds
#     if embeds is not None and len(embeds) > 0:
#         title = embeds[0].title
#         description = embeds[0].description


#         if title is not None and 'Flow' in title:
    
#             ticker = title.split(": ")[1]
    

#             fields = embeds[0].fields

#             names = [i.name for i in fields]
#             values = [i.value for i in fields]

        
#             sentiment = values[1].split(' :')[0].lower()

#             data = values[3]

#             data  = data.split('\n')

#             data = [i.replace(",","").replace('$','').replace('>','').replace('%','').lower().split(': ')[1] for i in data]
  
#             if len(data) == 9:
            
#                 flow_type = title
#                 strike = data[0]
#                 call_put = data[1]
#                 expiry = data[2]
#                 side = data[3].split(' :')[0]
#                 volume = data[4]
#                 oi = data[5]
#                 multiplier=data[6]
#                 iv = data[7]
#                 dte = data[8]
            
#                 timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
#                 try:
#                     # Assuming you have a connection pool or a single connection to your database
#                     conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
                    
#                     # Insert data into the market_data table
#                     await conn.execute('''
#                         INSERT INTO flow(flow_type, ticker, strike, call_put, expiry, side, volume, oi, multiplier, iv, dte, timestamp, sentiment)
#                         VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
#                         ''', flow_type, ticker, strike, call_put, expiry, side, volume, oi, multiplier, iv, dte, timestamp, sentiment)
                    

#                 except UniqueViolationError:
#                     pass


#         if title is not None and 'Bullseye' in title:
#             print(f"BULLSEYE: {title}")

#         if title is not None and 'RSI |' in title:
#             footer = embeds[0].footer.text
            


#             footer = footer.split(' | ')

#             ticker = footer[0]
#             rsi = footer[1].replace(']', '').replace('[', '')
#             timespan = footer[2]
#             status = footer[3]
#             timestamp = embeds[0].timestamp
#             timestamp = str(timestamp.astimezone()).split('.')[0]
#             # Convert string to datetime object
#             timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
#             try:
#                 # Assuming you have a connection pool or a single connection to your database
#                 conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
                
#                 # Insert data into the market_data table
#                 await conn.execute('''
#                     INSERT INTO rsi_status(ticker, rsi, timespan, status, timestamp)
#                     VALUES($1, $2, $3, $4, $5)
#                     ''', ticker, rsi, timespan, status, timestamp_dt)
                
#                 # Close the connection if you're not using a connection pool
#                 await conn.close()
#             except UniqueViolationError:
#                 pass


            # if message.content.endswith('.jpg'):
        #     if message.author == bot.user:
        #         return
        #     url = message.content
        #     analysis = gptsdk.analyze_stock(url)

        #     embed = disnake.Embed(title=f"GPT Vision", description=f"> **{analysis}**", color=disnake.Colour.random())
        #     embed.set_thumbnail(url)

        #     await message.channel.send(embed=embed)


        # elif message.content.endswith('.png'):
        #     if message.author == bot.user:
        #         return
        #     url = message.content
        #     analysis = gptsdk.analyze_stock(url)

        #     embed = disnake.Embed(title=f"GPT Vision", description=f"> **{analysis}**", color=disnake.Colour.random())
        #     embed.set_thumbnail(url)

        #     await message.channel.send(embed=embed)




        # if message.attachments:
        #     print(message.content)
        #     url = message.attachments[0].proxy_url
        #     analysis = gptsdk.analyze_stock(url)

        #     embed = disnake.Embed(title=f"GPT Vision", description=f"> **{analysis}**", color=disnake.Colour.random())
        #     embed.set_thumbnail(url)

        #     await message.channel.send(embed=embed)


    #     if message.channel.id in REDDIT:
    #         embeds = message.embeds[0]
    #         title = embeds.title
    #         description = embeds.description
    #         channel = message.channel.name

    #         # Assuming you have a connection pool or a single connection to your database
    #         conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
            
    #         # Insert data into the market_data table
    #         await conn.execute('''
    #             INSERT INTO reddit_posts(title, context, subreddit, insertion_timestamp)
    #             VALUES($1, $2, $3, NOW())
    #             ''', title, description, channel)
            

    #     if message.channel.id in TRADYTICS:
    #         embeds = message.embeds[0]
    #         title = embeds.title
    #         desc = embeds.description
    #         fields = embeds.fields
    #         field_names = [i.name for i in fields]
    #         field_values = [i.value for i in fields]

            
    #         print(f"FIELD NAMES: {field_names}")
    #         print(f"FIELD VALUES: {field_values}")

    #     if message.channel.id in td9_ids:
    #         footer = message.embeds[0].footer.text

    #         footer = footer.split('|')

    #         symbol = footer[0].replace(' ', '')
    #         timespan = td9_timespan_dict.get(footer[1].replace(' ', ''))
    #         status = footer[2].replace(' ', '')

    #         # Assuming you have a connection pool or a single connection to your database
    #         conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
            
    #         # Insert data into the market_data table
    #         await conn.execute('''
    #             INSERT INTO market_data(ticker, timespan, td9_state, insertion_timestamp)
    #             VALUES($1, $2, $3, NOW())
    #             ''', symbol, timespan, status)

        
    #     elif message.channel.id in SEC_FILINGS:

    #         footer = message.embeds[0].footer.text
    #         footer = footer.split('|')
    #         ticker = footer[0].replace(' ','')
    #         title = footer[1]
    #         link = footer[2].replace(' ', '')

            
    #         try:
    #             # Assuming you have a connection pool or a single connection to your database
    #             conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
                
    #             # Insert data into the market_data table
    #             await conn.execute('''
    #                 INSERT INTO sec_filings(ticker, title, link, insertion_timestamp)
    #                 VALUES($1, $2, $3, NOW())
    #                 ''', ticker, title, link)

    #         except UniqueViolationError:
    #             print(f'Skipping')

    #     # if message.channel.id in RSS_FEEDS:
    #     #     print(message.content)

    #     if message.channel.id in CRYPTO:
    #         footer = message.embeds[0].footer.text
    #         footer = footer.split("|")

    #         ticker = footer[0].replace(' ','')
    #         side = footer[1].replace(' ','')
    #         dollar_cost = float(footer[2].replace(' ',''))
    #         timestamp = embeds[0].timestamp
    #         timestamp = str(timestamp.astimezone()).split('.')[0]
    #         # Convert string to datetime object
    #         timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    #         try:
    #             # Assuming you have a connection pool or a single connection to your database
    #             conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
                
    #             # Insert data into the market_data table
    #             await conn.execute('''
    #                 INSERT INTO crypto(ticker, side, dollar_cost, timestamp)
    #                 VALUES($1, $2, $3, $4)
    #                 ''', ticker, side, dollar_cost, timestamp_dt)
                
    #             # Close the connection if you're not using a connection pool
    #             await conn.close()
    #         except UniqueViolationError:
    #             print(f'Skipping')

    #     # if message.channel.id in RSI_FEEDS:
    #     #     footer = message.embeds[0].footer.text
    #     #     print(footer)

    #     if message.channel.id in MOMENTUM_SCALPS:
    #         footer = message.embeds[0].footer.text
    #         footer = footer.split("|")

    #         ticker = footer[0].replace(' ', '')
    #         timeframe = footer[1].replace(' ', '')
    #         move = footer[2].replace(' ', '')
    #         # Assuming you have a connection pool or a single connection to your database
    #         try:
    #             conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
                
    #             # Insert data into the market_data table
    #             await conn.execute('''
    #                 INSERT INTO momentum_scalps(ticker, timeframe, move, insertion_timestamp)
    #                 VALUES($1, $2, $3, NOW())
    #                 ''', ticker.replace(' ',''), timeframe.replace(' ', ''), move.replace(' ',''))
                
    #             # Close the connection if you're not using a connection pool
    #             await conn.close()
    #         except UniqueViolationError:
    #             pass

    #     if message.channel.id in opt_vol_ids:
    #         footer = message.embeds[0].footer.text
    #         timestamp = embeds[0].timestamp
    #         timestamp = str(timestamp.astimezone()).split('.')[0]
    #         timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    #         print(footer, timestamp_dt)

        


    #     if message.channel.id in SECTOR_DARKPOOLS:
    #         embeds = message.embeds
    #         title = embeds[0].title
    #         desc = embeds[0].description

    #         fields = embeds[0].fields

    #         names = [i.name for i in fields]
    #         values = [i.value for i in fields]
            
    

    #         ticker = title.split(': ')
    #         ticker = ticker[1]
    #         timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    #         size_price = values[1].split(' @ ')
    #         size = size_price[0].replace(',','')
    #         price = size_price[1].replace('$','')
    #         notional_value = values[2].split(' ')
    #         notional_value=notional_value[0].replace('$', '').replace(',','')
    #         channel = message.channel.name
    #         # Regex pattern to match emojis
    #         emoji_pattern = re.compile("["
    #                                 u"\U0001F600-\U0001F64F"  # emoticons
    #                                 u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    #                                 u"\U0001F680-\U0001F6FF"  # transport & map symbols
    #                                 u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
    #                                 u"\U00002702-\U000027B0"
    #                                 u"\U000024C2-\U0001F251"
    #                                 "]+", flags=re.UNICODE)

    #         # Remove emojis from the string
    #         cleaned_string = emoji_pattern.sub(r'', channel)

    #         try:
    #             conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
                
    #             # Insert data into the market_data table
    #             await conn.execute('''
    #                 INSERT INTO dark_pools(sector, ticker, time, size, price, notional_value)
    #                 VALUES($1, $2, $3, $4, $5, $6)
    #                 ''', cleaned_string, ticker, timestamp, size, price, notional_value)
                
    #             # Close the connection if you're not using a connection pool
    #             await conn.close()
    #         except UniqueViolationError:
    #             pass

    #     if message.channel.id in STOCK_FEEDS:
    #         footer = embeds[0].footer.text
    #         footer = footer.split('|')
    #         ticker = footer[0]
    #         type = footer[1].replace('!','').replace(' ','').lower()
    #         price = footer[2]
    #         fifty_high = footer[3]
    #         fifty_low = footer[4]

    #         timestamp = embeds[0].timestamp
    #         timestamp = str(timestamp.astimezone()).split('.')[0]
    #         # Convert string to datetime object
    #         timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

    #         print(type, price, fifty_high, fifty_low)

    #         if 'High' or 'Low' in title:
    #             try:
    #                 conn = await asyncpg.connect('postgresql://postgres:fud@localhost/polygon')
                    
    #                 # Insert data into the market_data table
    #                 await conn.execute('''
    #                     INSERT INTO new_prices(ticker, type, fifty_low, price, fifty_high, timestamp)
    #                     VALUES($1, $2, $3, $4, $5, $6)
    #                     ''', ticker, type, fifty_high, price, fifty_low, timestamp)
                    
    #                 # Close the connection if you're not using a connection pool
    #                 await conn.close()
    #             except UniqueViolationError:
    #                 pass


    #     if message.channel.id in OPTIONS_CONDITIONS:
    #         footer = embeds[0].footer.text
    #         print(footer)
    # await bot.process_commands(message)
# This dictionary will hold the conversation state for each user
conversations = {}
client = OpenAI(api_key=os.environ.get('YOUR_OPENAI_KEY'))
@bot.slash_command()
async def option_data(inter: disnake.ApplicationCommandInteraction):
    modal = OptionModal()
    await inter.response.send_modal(modal)


async def handle_interaction(interaction):
    # Call your async function here
    await server(interaction)
    # Edit the original message after handling the interaction
    await interaction.edit_original_message(content="New content here")
    

@bot.slash_command()
async def server(inter:disnake.AppCmdInter):
    """Gets server feeds"""
    await inter.response.defer()
    db_pool = await asyncpg.create_pool(database='polygon', user='postgres', password='fud')
    counter = 0
    while True:
        counter = counter + 1
        td9_query = "SELECT distinct ticker, timespan, insertion_timestamp FROM market_data order by insertion_timestamp DESC LIMIT 10;"  # Replace 
        sec_query = "SELECT distinct ticker, title, insertion_timestamp FROM sec_filings order by insertion_timestamp DESC limit 1;"
        momentum_query = "SELECT distinct ticker, timeframe, move, insertion_timestamp FROM momentum_scalps order by insertion_timestamp DESC limit 4;"
        reddit_query = "SELECT distinct subreddit, title, insertion_timestamp FROM reddit_posts order by insertion_timestamp DESC limit 4;"
        flow_query = f"SELECT ticker, strike, call_put, expiry, sentiment FROM ( SELECT ticker, strike, call_put, expiry, sentiment, timestamp, ROW_NUMBER() OVER (PARTITION BY ticker, strike, call_put, expiry, sentiment ORDER BY timestamp) as rn FROM flow ) subquery WHERE rn = 1 ORDER BY timestamp DESC limit 3;"
        rsi_query = f"SELECT ticker, timespan, status FROM ( SELECT ticker, timespan, status, timestamp, ROW_NUMBER() OVER (PARTITION BY ticker, timespan, status ORDER BY timestamp DESC) as rn FROM rsi_status ) subquery WHERE rn = 1 ORDER BY timestamp DESC LIMIT 10;"
        dark_pool_query = f"SELECT ticker, price, sector FROM ( SELECT ticker, price, sector, time, ROW_NUMBER() OVER (PARTITION BY ticker, price, sector ORDER BY time DESC) as rn FROM dark_pools ) subquery WHERE rn = 1 ORDER BY time DESC LIMIT 5;"
        new_prices_query = f"SELECT ticker, fifty_high, price, fifty_low FROM ( SELECT ticker, fifty_high, price, fifty_low, timestamp, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn FROM new_prices ) subquery WHERE rn = 1 ORDER BY timestamp DESC LIMIT 5;"
        message_query = f"SELECT username, message FROM ( SELECT username, message, timestamp, ROW_NUMBER () OVER (PARTITION BY message ORDER BY timestamp DESC) as rn FROM messages ) subquery WHERE rn = 1 ORDER BY timestamp DESC LIMIT 5;"
        async with db_pool.acquire() as conn:
            td9_rows = await conn.fetch(td9_query)
            sec_rows = await conn.fetch(sec_query)
            momentum_rows = await conn.fetch(momentum_query)
            reddit_rows = await conn.fetch(reddit_query)
            flow_rows = await conn.fetch(flow_query)
            rsi_rows = await conn.fetch(rsi_query)
            dark_pool_rows = await conn.fetch(dark_pool_query)

            new_prices_rows = await conn.fetch(new_prices_query)

            messages_rows = await conn.fetch(message_query)

            messages_data = [[row[0], row[1][:25]] for row in messages_rows]

            td9_data = [[row[0], row[1], format_timestamp(row[2])] for row in td9_rows]
            sec_data = [[row[0], row[1][:15], format_timestamp(row[2])] for row in sec_rows]
            momentum_data = [[row[0], row[1], row[2], format_timestamp(row[3])] for row in momentum_rows]
            reddit_data = [[row[0][:25], row[1][:25], format_timestamp(row[2])] for row in reddit_rows]
            

            # Create tables using tabulate
            td9_table = tabulate(td9_data, headers=['Ticker', 'Date', 'Date'], tablefmt="fancy", showindex=False)
            sec_table = tabulate(sec_data, headers=['Ticker', 'Title', 'Date'], tablefmt="fancy", showindex=False)
            momentum_table = tabulate(momentum_data, headers=['Ticker', 'Frame', 'Move', 'Date'], tablefmt="fancy", showindex=False)
            reddit_table = tabulate(reddit_data, headers=['Channel', 'Title', 'Timestamp'], tablefmt="fancy", showindex=False)
            new_price_table = tabulate(new_prices_rows, headers='keys', tablefmt='fancy', showindex=False)

            messages_table = tabulate(messages_data, headers='keys', tablefmt='fancy', showindex=False)
            flow_table = tabulate(flow_rows, headers='keys', tablefmt='fancy', showindex=False)
            rsi_table = tabulate(rsi_rows, headers='keys', tablefmt='fancy', showindex=False)
            dark_pool_table = tabulate(dark_pool_rows, headers='keys', tablefmt='fancy', showindex=False)

            embed = disnake.Embed(title=f"GPT4-Turbo", description=f"```Viewing Live FUDSTOP Feeds```", color=disnake.Colour.dark_orange())
            embed.add_field(name=f"TD9s:", value=f"```py\n{td9_table}```", inline=False)
            
            embed.add_field(name=f"Scalps:", value=f"```py\n{momentum_table}```", inline=False)
            embed.add_field(name=f"Opening Flow:", value=f"```py\n{flow_table}```", inline=False)
            embed.add_field(name=f"RSI:", value=f"```py\n{rsi_table}```", inline=False)
            embed.add_field(name=f"Dark Pools:", value=f"```py\n{dark_pool_table}```", inline=False)
            embed.add_field(name=f"New Prices:", value=f"```py\n{new_price_table}```", inline=False)
            embed.add_field(name=f"SEC Filings:", value=f"```py\n{sec_table}```", inline=False)
            embed.add_field(name=f'Reddit:', value=f"```py\n{reddit_table}```", inline=False)
            embed.add_field(name=f"Messages:", value=f"```py\n{messages_table}```", inline=False)
            await inter.edit_original_message(embed=embed)
            button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="<a:_:1104142591110418585>")
            button.callback = handle_interaction
            view=disnake.ui.View()
            if counter == 400:
                view.add_item(button)
                await inter.edit_original_message(view=view)
                break

# @bot.slash_command()
# async def options_database(inter:disnake.ApplicationCommandInteraction):
#     """Use a Modal to query the database for options."""
#     modal = SQLQueryModal()
#     await inter.response.send_modal(modal)


@bot.slash_command()
async def screener(inter: disnake.AppCmdInter,
                   ask_gte: str = None, ask_lte: str = None,
                   bid_gte: str = None, bid_lte: str = None,
                   changeratio_gte: str = None, changeratio_lte: str = None,
                   close_gte: str = None, close_lte: str = None,
                   delta_gte: str = None, delta_lte: str = None,
                   direction: str = None,  # This might need to be handled differently since it's a list
                   expiredate_gte: str = None, expiredate_lte: str = None,
                   gamma_gte: str = None, gamma_lte: str = None,
                   implvol_gte: str = None, implvol_lte: str = None,
                   openinterest_gte: str = None, openinterest_lte: str = None,
                   theta_gte: str = None, theta_lte: str = None,
                   volume_gte: str = None, volume_lte: str = None):
    await inter.response.defer()

    # Creating an instance of WebulScreener
    screener = WebulScreener()

    # Example usage of the query method with parameters from the command
    query_result = screener.query(
        ask_gte=ask_gte, ask_lte=ask_lte,
        bid_gte=bid_gte, bid_lte=bid_lte,
        changeRatio_gte=changeratio_gte, changeRatio_lte=changeratio_lte,
        close_gte=close_gte, close_lte=close_lte,
        delta_gte=delta_gte, delta_lte=delta_lte,
        direction=[direction] if direction else None,  # Handling direction as a list
        expireDate_gte=expiredate_gte, expireDate_lte=expiredate_lte,
        gamma_gte=gamma_gte, gamma_lte=gamma_lte,
        implVol_gte=implvol_gte, implVol_lte=implvol_lte,
        openInterest_gte=openinterest_gte, openInterest_lte=openinterest_lte,
        theta_gte=theta_gte, theta_lte=theta_lte,
        volume_gte=volume_gte, volume_lte=volume_lte
    )
    query_result_df = pd.DataFrame(query_result)
    query_result_df = query_result_df.drop(columns=['id'])
    chunks = [query_result_df[i:i + 3860] for i in range(0, len(query_result_df), 3860)]
    
    embeds =[]
    for chunk in chunks:
    
        embed = disnake.Embed(title=f"Screener Results:", description=f"```py\n{chunk}```")
        # Here, handle the query_result as needed, e.g., sending a message
        view = disnake.ui.View()
        ids = query_result.get('id')
        symbols = query_result.get('symbol')
        strikes = query_result.get('strike')
        expiry = query_result.get('expiry')
        call_put = query_result.get('call_put')
        embed.set_footer(text=f'Implemented by FUDSTOP')
        embeds.append(embed)
    view.add_item(ScreenerSelect(query_result))
    await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ScreenerSelect(query_result)))
        


@bot.command()
async def gpt4(ctx):
    # Start a new conversation with the user
    conversations[ctx.author.id] = []
    
    # Send an initial message to the user
    await ctx.send("> # Slave Bot\n> Online. \n\n> Your work is my...work.. Type to chat... or.. type stop to quit.")

    while True:
        # Wait for a message from the same user
        message = await bot.wait_for(
            "message",
            check=lambda m: m.author == ctx.author and m.channel == ctx.channel
        )

        # Check if the user wants to stop the conversation
        if message.content.lower() == "stop":
            await ctx.send("Goodbye! If you need help again, just call me.")
            del conversations[ctx.author.id]  # Clean up the conversation
            break

        # Append the user's message to the conversation
        conversations[ctx.author.id].append({"role": "user", "content": message.content + "YOU ARE ONLY TO REPLY IN CODE. CODE ONLY. NO MARKDOWN. ONLY CODE!"})

        # Send the conversation to OpenAI
        response = client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=conversations[ctx.author.id],
            temperature=0.33,
            max_tokens=4096        )

        # Get the content from the OpenAI response
        ai_response = response.choices[0].message.content

        # Append the AI's response to the conversation
        conversations[ctx.author.id].append({"role": "assistant", "content": ai_response})
        embeds = []
        chunks = [ai_response[i:i + 3860] for i in range(0, len(ai_response), 3860)]
        for chunk in chunks:
            embed = disnake.Embed(title=f"GPT4-Turbo", description=f"{chunk}", color=disnake.Colour.dark_orange())
            embed.add_field(name=f"Your prompt:", value=f"> **{message.content[:400]}**", inline=False)
            embeds.append(embed)

        await ctx.send(embed=embeds[0], view=AlertMenus(embeds))



# @bot.command()
# async def cfr(ctx, *, query: str):
#     results = fetch_results(query)
    
#     async def create_table(conn):
#         await conn.execute('''
#             CREATE TABLE IF NOT EXISTS cfr_data (
#                 starts_on TEXT,
#                 ends_on TEXT,
#                 type TEXT,
#                 hierarchy_title TEXT,
#                 hierarchy_subtitle TEXT,
#                 hierarchy_chapter TEXT,
#                 hierarchy_subchapter TEXT,
#                 hierarchy_part TEXT,
#                 hierarchy_subpart TEXT,
#                 hierarchy_subject_group TEXT,
#                 hierarchy_section TEXT,
#                 hierarchy_appendix TEXT,
#                 hierarchy_headings_title TEXT,
#                 hierarchy_headings_subtitle TEXT,
#                 hierarchy_headings_chapter TEXT,
#                 hierarchy_headings_subchapter TEXT,
#                 hierarchy_headings_part TEXT,
#                 hierarchy_headings_subpart TEXT,
#                 hierarchy_headings_subject_group TEXT,
#                 hierarchy_headings_section TEXT,
#                 hierarchy_headings_appendix TEXT,
#                 headings_title TEXT,
#                 headings_subtitle TEXT,
#                 headings_chapter TEXT,
#                 headings_subchapter TEXT,
#                 headings_part TEXT,
#                 headings_subpart TEXT,
#                 headings_subject_group TEXT,
#                 headings_section TEXT,
#                 headings_appendix TEXT,
#                 full_text_excerpt TEXT,
#                 score REAL,
#                 structure_index INT,
#                 reserved BOOLEAN,
#                 removed BOOLEAN,
#                 change_types_effective_cross_reference TEXT,
#                 change_types_cross_reference TEXT,
#                 change_types_effective TEXT,
#                 change_types_initial TEXT
#             );
#         ''')

#     async def insert_data(conn, results):
#         insert_query = '''
#             INSERT INTO cfr_data (
#                 starts_on, ends_on, type, hierarchy_title, hierarchy_subtitle,
#                 hierarchy_chapter, hierarchy_subchapter, hierarchy_part,
#                 hierarchy_subpart, hierarchy_subject_group, hierarchy_section,
#                 hierarchy_appendix, hierarchy_headings_title,
#                 hierarchy_headings_subtitle, hierarchy_headings_chapter,
#                 hierarchy_headings_subchapter, hierarchy_headings_part,
#                 hierarchy_headings_subpart, hierarchy_headings_subject_group,
#                 hierarchy_headings_section
#             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38);
#         '''
#         await conn.executemany(insert_query, results)
    
#     if 'results' in results:

#         df = pd.DataFrame(results['results'])
#         csv_buffer = io.StringIO()
#         df.to_csv(csv_buffer, index=False)
#         csv_buffer.seek(0)
#         await ctx.send(file=disnake.File(fp=csv_buffer, filename='cfr_results.csv'))
    

from apis.oic.oic_sdk import OICSDK
oic_sdk = OICSDK()

@bot.command()
async def monitor(ctx, ticker):
    """Monitors an option in great detail"""
    try:
        df = await oic_sdk.options_monitor(ticker=ticker)

        df.as_dataframe.to_csv('data/oic/options_monitor.csv', index=False)
        
        await ctx.send(file=disnake.File('data/oic/options_monitor.csv'))
    except CommandInvokeError:
        await bot.restar


@bot.command()
async def active(ctx):
    """Returns the most active options from the OIC"""

  


    df = oic_sdk.most_active_options()

    df.to_csv('data/oic/most_active_options.csv', index=False)


    await ctx.send(file=disnake.File('data/oic/most_active_options.csv'))


    


        # # Validate tickers
        # valid_tickers = [ticker for ticker in tickers if ticker in self.valid_tickers]
        # invalid_tickers = list(set(tickers) - set(valid_tickers))

        # if not valid_tickers:
        #     # No valid tickers were entered
        #     await interaction.response.send_message(
        #         "None of the entered tickers were recognized. Please try again with valid ticker symbols.",
        #         ephemeral=True
        #     )
        #     return

        # # If there are invalid tickers, inform the user but proceed with valid ones
        # if invalid_tickers:
        #     await interaction.followup.send(
        #         f"The following tickers were not recognized and will be ignored: {', '.join(invalid_tickers)}",
        #         ephemeral=True
        #     )

        # # Proceed with scanning the valid tickers
        # results = await scan_bars(valid_tickers, timeframe)
        # await interaction.followup.send(results)

from fudstop4._markets.scripts.stock_market import StockMarketLive




# stock_market = StockMarketLive()
# @bot.slash_command()
# async def stream(inter: disnake.AppCmdInter, ticker:str):
#     """Stream live trades for a ticker."""
#     await inter.response.defer()
    
#     await stock_market.connect()
#     counter = 0
#     while True:
#         counter = counter + 1
#         data = await stock_market.fetch_latest_trade(ticker)
#         if data:
#             # Format the message with the trade data
#             message = f"> # Latest trade for {ticker} | Price: ${data['price']} | Size: {data['size']} | Time: {data['timestamp']}"
#         else:
#             # No trade data found
#             message = f"> No recent trades found for {ticker}."
#         await inter.edit_original_message(f"> # {message}")

#         if counter == 250:
#             await inter.send(f'> # Stream ended.')
#             break
    
@bot.command()
async def cal(ctx):
    # Set up headless browser options for Selenium
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    # Initialize the Chrome driver
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Get the webpage snapshot
        driver.get("https://www.newyorkfed.org/research/calendars/nationalecon_cal")

        # Get today's date in the format that matches the calendar on the webpage
        today = datetime.now().strftime("%d")  # Format to match the date format on the calendar, e.g. "13" for 13th
        if today.startswith('0'):
            # Remove leading zero for single digit dates if necessary
            today = today[1:]

        # Wait for the calendar element that matches today's date to be present in the DOM
        wait = WebDriverWait(driver, 10)
        # Replace 'dateElementLocator' with the actual locator that matches the date on the calendar
        date_element_locator = f"//someElement[contains(text(), '{today}')]"
        date_element = wait.until(EC.presence_of_element_located((By.XPATH, date_element_locator)))

        # Scroll the date element into view
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", date_element)

        # Optionally, wait for any animations or dynamic content to settle
        driver.implicitly_wait(2)

        # Take a screenshot of the visible part of the page, hopefully centered on today's date
        screenshot = driver.get_screenshot_as_png()

        # Convert the screenshot to a Discord file-like object
        screenshot_file = io.BytesIO(screenshot)
        screenshot_file.seek(0)
        screenshot_file.name = 'calendar.png'

        # Create a disnake File object
        discord_file = disnake.File(screenshot_file, filename='calendar.png')

        # Create embed to attach the image
        embed = disnake.Embed(title="Economic Calendar", color=0x1a1a1a)
        embed.set_image(url="attachment://calendar.png")

        # Send the embed with the screenshot in the current channel
        await ctx.send(embed=embed, file=discord_file)

    finally:
        # Make sure to quit the driver to free up resources
        driver.quit()

timeframe_choices = [
    disnake.OptionChoice(name="1 Minute", value="m1"),
    disnake.OptionChoice(name="5 Minutes", value="m5"),
    disnake.OptionChoice(name="10 Minutes", value="m10"),
    disnake.OptionChoice(name="15 Minutes", value="m15"),
    disnake.OptionChoice(name="30 Minutes", value="m30"),
    disnake.OptionChoice(name="1 Hour", value="m60"),
    disnake.OptionChoice(name="2 Hours", value="m120"),
    disnake.OptionChoice(name="4 Hours", value="m240"),
    disnake.OptionChoice(name="Daily", value="d1"),
    disnake.OptionChoice(name="Weekly", value="w")
]





async def fetch_options():
    conn = await asyncpg.connect(user=os.environ.get('DB_USER'), password=os.environ.get('DB_PASSWORD'), database=os.environ.get('DB_NAME'), host=os.environ.get('DB_HOST'))
    rows = await conn.fetch("""WITH LatestPrices AS (
    SELECT 
        a.ticker, 
        a.strike, 
        a.expiry, 
        a.call_put, 
        a.theta,
        b.low AS current_price,
        a.bid, 
        a.ask,
        ROW_NUMBER() OVER (PARTITION BY a.ticker, a.strike, a.expiry, a.call_put ORDER BY b.timestamp DESC) AS rn
    FROM 
        options_data a
    INNER JOIN 
        option_aggs b ON a.ticker = b.ticker
                      AND a.strike = b.strike
                      AND a.expiry = b.expiry
                      AND a.call_put = b.call_put
    WHERE 
        a.bid >= 0.14 AND a.ask <= 1.00
        AND a.theta >= -0.02
),
AllTimeLows AS (
    SELECT 
        a.ticker, 
        a.strike, 
        a.expiry, 
        a.call_put, 
        MIN(b.low) AS all_time_low
    FROM 
        options_data a
    INNER JOIN 
        option_aggs b ON a.ticker = b.ticker
                      AND a.strike = b.strike
                      AND a.expiry = b.expiry
                      AND a.call_put = b.call_put
    GROUP BY 
        a.ticker, a.strike, a.expiry, a.call_put
)
SELECT 
    l.ticker, 
    l.strike, 
    l.expiry, 
    l.call_put, 
    l.current_price
FROM 
    LatestPrices l
INNER JOIN 
    AllTimeLows a ON l.ticker = a.ticker
                 AND l.strike = a.strike
                 AND l.expiry = a.expiry
                 AND l.call_put = a.call_put
                 AND l.current_price = a.all_time_low
WHERE 
    l.rn = 1;""")
    await conn.close()
    return rows

@commands.is_owner()
@bot.command()
async def clear(ctx: commands.Context, limit: int = 10):
    await ctx.channel.purge(limit=limit)




@bot.command()
async def iv(ctx, ticker, strike, call_put, expiry):
    """Gets IV for a ticker"""
    
 
    await opts.connect()
    async for results in opts.fetch_iter(query=f"""select option_symbol from options_data where ticker = '{ticker}' and strike = {strike} and expiry = '{expiry}' and call_put = '{call_put}';"""):
        view = disnake.ui.View()
        option_symbol = results[0]
        data = []
        class Select(disnake.ui.Select):
            def __init__(self):
                self.option_symbol = option_symbol
                super().__init__( 
                    placeholder='Select -->',
                    min_values=1,
                    max_values=1,
                    options= [disnake.SelectOption(label=f'{option_symbol}')]
                )

            async def callback(self, inter:disnake.AppCmdInter):
                while True:
                    await inter.response.defer()

                    data = await opts.get_universal_snapshot(ticker=option_symbol)
                    df = pd.DataFrame(data)
                    # Selecting the 'iv' column
                    print(df.columns)
                    iv_column = df['IV']

                    await inter.edit_original_message(iv_column)
        view.add_item(Select())
        await ctx.send(view=view)

# @bot.command()
# async def puts(ctx, ticker: str):
#     """Gets at the money puts for a ticker"""
#     data = sdk.atm_puts(ticker)
#     data = pd.DataFrame(data)
#     filename = f'data/yf_atm_puts.csv'
#     data.to_csv(filename)
#     await ctx.send(file=disnake.File(filename))









# Command to print webhook URLs for a specific channel
@bot.command(name='webhooks')
async def fetch_webhooks(ctx):
    # Ensure the author has the necessary permissions
    if ctx.author.guild_permissions.manage_webhooks:
        webhooks = await ctx.channel.webhooks()
        if webhooks:
            response = "\n".join([webhook.url for webhook in webhooks])
            await ctx.send(f"Webhook URLs for {ctx.channel.name}:\n{response}")
        else:
            await ctx.send("No webhooks found in this channel.")
    else:
        await ctx.send("You do not have permission to manage webhooks.")


@bot.slash_command()
async def study_greeks(inter:disnake.AppCmdInter, ticker:str):
    """Study greeks and their effects on price action."""

    await inter.response.defer()
    while True:
        price = yf.fast_info('AAPL')

        current_price = price[price[0] == 'lastPrice'][1].values[0]

        moenyness = await opts.get_option_strikes_by_moneyness(ticker=ticker, current_price=current_price, expiration_date='2023-12-01')

        embed = disnake.Embed(title=f"Greek Study", description=f"Studying Greeks for {ticker}")

        for term, categories in moenyness.items():
            embed.add_field(name=f"Term: {term}", value="-" * 10, inline=False)  # Separator for terms
            for moneyness_category, options in categories.items():
                # Assume each 'option' object has attributes like 'delta', 'gamma', etc.
                for option, category in options:
                    delta = getattr(option, 'delta', 'N/A')
                    gamma = getattr(option, 'gamma', 'N/A')
                    vega = getattr(option, 'vega', 'N/A')
                    theta = getattr(option, 'theta', 'N/A')
                    ask = getattr(option, 'ask', 'N/A')
                    bid = getattr(option, 'ask', 'N/A')
                    strike = getattr(option, 'strike', 'N/A')
                    expiry = getattr(option, 'expiry', 'N/A') 

                    option_info = f"> Delta: {round(float(delta),4)}\n> Gamma: {round(float(gamma),4)}\n> Vega: {round(float(vega),4)}\n> Theta: {round(float(theta),2)}\n> Bid: **${bid}**\n> Ask: **${ask}**"
                    embed.add_field(name=f"${ticker} | {strike} | {expiry}", value=option_info, inline=True)

        await inter.edit_original_message(embed=embed)


@bot.command()
async def pricecheck(ctx, ticker:str):


    result = await opts.get_theoretical_price(ticker)
    
    df = pd.DataFrame(result)
    df = df.sort_values('theoretical_price', ascending=False)
    df.to_csv('theo_check.csv')

    await ctx.send(file=disnake.File('theo_check.csv'))

opts = PolygonOptions(database='fudstop3')

async def find_extreme_tickers(pool):
    # SQL query to find tickers that are overbought or oversold on both day and week timespans
    query_sql = """
    SELECT day_rsi.ticker, day_rsi.status
    FROM rsi as day_rsi
    JOIN rsi as week_rsi ON day_rsi.ticker = week_rsi.ticker
    WHERE day_rsi.timespan = 'day' 
    AND week_rsi.timespan = 'week'
    AND day_rsi.status IN ('overbought', 'oversold')
    AND week_rsi.status IN ('overbought', 'oversold')
    AND day_rsi.status = week_rsi.status;
    """

        # Execute the query using the provided connection pool
    async with pool.acquire() as conn:
        records = await conn.fetch(query_sql)
        return [(record['ticker'], record['status']) for record in records]

async def find_plays():
    db_config = {
        'user': 'postgres',
        'password': 'fud',
        'database': 'opts',
        'host': '127.0.0.1',
        'port': 5432
    }

    async with asyncpg.create_pool(**db_config) as pool:
        extreme_tickers_with_status = await find_extreme_tickers(pool)

        # To separate the tickers and statuses, you can use list comprehension
        extreme_tickers = [ticker for ticker, status in extreme_tickers_with_status]
        statuses = [status for ticker, status in extreme_tickers_with_status]
        all_options_df_calls =[]
        all_options_df_puts = []
        for ticker, status in extreme_tickers_with_status:
            if status == 'overbought':
                print(f"Ticker {ticker} is overbought.")
                all_options = await opts.get_option_chain_all(ticker, expiration_date_gte='2024-03-01', expiration_date_lite='2024-06-30', contract_type='put')
                
                for i in range(len(all_options.theta)):  # Assuming all lists are of the same length
                    theta_value = all_options.theta[i]
                    volume = all_options.volume[i]
                    open_interest = all_options.open_interest[i]
                    ask = all_options.ask[i]
                    bid = all_options.bid[i]

                    # Conditions
                    theta_condition = theta_value is not None and theta_value >= -0.03
                    volume_condition = volume is not None and open_interest is not None and volume > open_interest
                    price_condition = ask is not None and bid is not None and 0.25 <= bid <= 1.75 and 0.25 <= ask <= 1.75

                    if theta_condition and volume_condition and price_condition:
                        df = pd.DataFrame([all_options.ticker, all_options.underlying_ticker, all_options.strike, all_options.contract_type, all_options.expiry])
                        all_options_df_puts.append(df)  #

            if status == 'oversold':
                print(f"Ticker {ticker} is oversold.")
                all_options = await opts.get_option_chain_all(ticker, expiration_date_gte='2024-03-01', expiration_date_lite='2024-11-30', contract_type='call')
                
                for i in range(len(all_options.theta)):  # Assuming all lists are of the same length
                    theta_value = all_options.theta[i]
                    volume = all_options.volume[i]
                    open_interest = all_options.open_interest[i]
                    ask = all_options.ask[i]
                    bid = all_options.bid[i]

                    # Conditions
                    theta_condition = theta_value is not None and theta_value >= -0.03
                    volume_condition = volume is not None and open_interest is not None and volume > open_interest
                    price_condition = ask is not None and bid is not None and 0.25 <= bid <= 1.75 and 0.25 <= ask <= 1.75

                    if theta_condition and volume_condition and price_condition:
                        # Assuming all_options.df is a DataFrame containing the current option data
                        df = pd.DataFrame([all_options.ticker, all_options.strike, all_options.contract_type, all_options.expiry])
                        all_options_df_calls.append(df)  #
        # Concatenate all the dataframes
        final_df_calls = pd.concat(all_options_df_calls, ignore_index=True)
        final_df_puts = pd.concat(all_options_df_puts, ignore_index=True)
        print(final_df_calls, final_df_puts)
        return final_df_calls, final_df_puts, extreme_tickers, statuses


class CallResults(disnake.ui.Select):
    def __init__(self, options: List[disnake.SelectOption], all_options:pd.DataFrame=None):
        self.all_options=all_options
        self.options = options
        super().__init__( 
            custom_id='callresults',
            min_values=1,
            max_values=1,
            placeholder='Results -->',
            options=options
        )


    async def callback(self, inter:disnake.AppCmdInter):

        if self.values[0] == self.values[0]:

            data = self.all_options.head(10)


            await inter.response.edit_message(data)

    
@bot.slash_command()
async def play(inter:disnake.AppCmdInter, type:str=commands.Param(choices=['calls','puts'])):
    await inter.response.defer()
    await inter.edit_original_message(f'Finding plays for {type}..')


    calls, puts, extreme_tickers, statuses = await find_plays()


    view = disnake.ui.View()
    if type == 'calls':
        options = []
        
        for ticker, status in zip(extreme_tickers, statuses):
            if status == 'oversold':
                
                options.append(disnake.SelectOption(label=ticker, description=status))
        await inter.edit_original_message(file=disnake.File('calls.csv'), view=view.add_item(CallResults(options,calls)))
    elif type == 'puts':
        options = []
        
        for ticker, status in zip(extreme_tickers, statuses):
            if status == 'overbought':
                options.append(disnake.SelectOption(label=ticker, description=status))
        await inter.edit_original_message(file=disnake.File('puts.csv'), view=view.add_item(CallResults(options,puts)))

    

@bot.slash_command()
async def master_database(inter: disnake.AppCmdInter):
    """View multiple tables in real-time that show discord feeds."""
    await opts.connect()
    await inter.response.defer()
    page_number = 0
    limit = 5
    while True:
        offset = page_number * limit
        fire_sale_query = f"SELECT ticker FROM fire_sale ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        neutral_zone_query = f"SELECT ticker FROM neutral_zone ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        accumulation_query = f"SELECT ticker FROM accumulation ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        above_vol_query = f"SELECT ticker FROM above_avg_vol ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        below_vol_query = f"SELECT ticker FROM below_avg_vol ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        near_high_query = f"SELECT ticker FROM near_high ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        near_low_query = f"SELECT ticker FROM near_low ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        dark_pool_query = f"SELECT ticker, value FROM dark_pools ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        trades_query = f"SELECT ticker, exchange FROM trades ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset}"
        os_query = f"SELECT ticker, status, timespan, rsi FROM rsi_status WHERE status ='oversold' ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset}"
        ob_query =f"SELECT ticker, status, timespan, rsi FROM rsi_status WHERE status ='overbought' ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset}"
        ob_query = f"SELECT ticker, strike, cp, expiry oi"
        # fire_sale_query = f"SELECT ticker, avg_price, buy_pct, sell_pct, neut_pct FROM fire_sale ORDER BY insertion_timestamp DESC LIMIT {limit} OFFSET {offset};"
        # fire_sale_query = f"SELECT ticker, avg_price, buy_pct, sell_pct, neut_pct FROM fire_sale order by insertion_timestamp DESC limit 5;"
        # neutral_zone_query = f"SELECT ticker, avg_price, buy_pct, sell_pct, neut_pct FROM neutral_zone order by insertion_timestamp DESC limit 5;"
        # accumulation_query = f"SELECT ticker, avg_price, buy_pct, sell_pct, neut_pct FROM accumulation order by insertion_timestamp DESC limit 5;"
        # above_vol_query = f"SELECT ticker, fifty_high, close, fifty_low, change_percent FROM above_avg_vol order by insertion_timestamp DESC limit 5;"
        # below_vol_query = f"SELECT ticker, fifty_high, close, fifty_low, change_percent FROM below_avg_vol order by insertion_timestamp DESC limit 5;"
        # near_high_query = f"SELECT ticker, fifty_high, close, fifty_low, change_percent FROM near_high order by insertion_timestamp DESC limit 5;"
        # near_low_query = f"SELECT ticker, fifty_high, close, fifty_low, change_percent FROM near_low order by insertion_timestamp DESC limit 5;"
        # dark_pool_query = f"SELECT ticker, sector, size, price FROM dark_pools order by time DESC limit 5;"


        os_records = await opts.fetch(os_query)
        ob_records = await opts.fetch(ob_query)
        trades_records = await opts.fetch(trades_query)

        fs_records = await opts.fetch(fire_sale_query)

        nz_records = await opts.fetch(neutral_zone_query)

        acc_records = await opts.fetch(accumulation_query)

        above_vol_records = await opts.fetch(above_vol_query)

        below_vol_records = await opts.fetch(below_vol_query)
        dp_records = await opts.fetch(dark_pool_query)

        near_high_records = await opts.fetch(near_high_query)

        near_low_records = await opts.fetch(near_low_query)


        os_df = pd.DataFrame(os_records, columns=['sym', 'status', 'timespan', 'rsi'])
        ob_df = pd.DataFrame(ob_records, columns=['sym', 'status', 'timespan', 'rsi'])

        trade_df = pd.DataFrame(trades_records, columns=['sym', 'exchange'])


        fs_df = pd.DataFrame(fs_records, columns=['sym'])

        nz_df = pd.DataFrame(nz_records, columns=['sym'])

        acc_df = pd.DataFrame(acc_records, columns=['sym'])

        above_df = pd.DataFrame(above_vol_records, columns=['sym'])

        below_df = pd.DataFrame(below_vol_records, columns=['sym'])


        dp_df = pd.DataFrame(dp_records, columns=['sym', 'value'])
        
        near_df = pd.DataFrame(near_high_records, columns=['sym'])

        nearlow = pd.DataFrame(near_low_records, columns=['sym'])


        # fs_df = pd.DataFrame(fs_records, columns=['sym', 'avgPrice', 'buyPct', 'sellPct', 'neutPct'])

        # nz_df = pd.DataFrame(nz_records, columns=['sym', 'avgPrice', 'buyPct', 'sellPct', 'neutPct'])

        # acc_df = pd.DataFrame(acc_records, columns=['sym', 'avgPrice', 'buyPct', 'sellPct', 'neutPct'])

        # above_df = pd.DataFrame(above_vol_records, columns=['sym', 'f2high', 'close', 'f2low', 'cp'])

        # below_df = pd.DataFrame(below_vol_records, columns=['sym', 'f2high', 'close', 'f2low', 'cp'])


        # dp_df = pd.DataFrame(dp_records, columns=['sym', 'sector', 'size', 'price'])
        
        # near_df = pd.DataFrame(near_high_records, columns=['sym', 'f2high', 'close', 'f2low', 'cp'])

        # nearlow = pd.DataFrame(near_low_records, columns=['sym', 'f2high', 'close', 'f2low', 'cp'])
        os_table = tabulate(os_df, headers='keys', tablefmt='fancy', showindex=False)
        ob_table = tabulate(ob_df, headers='keys', tablefmt='facny', showindex=False)
        trade_table = tabulate(trade_df, headers='keys', tablefmt='fancy', showindex=False)
        fs_table = tabulate(fs_df, headers='keys', tablefmt='fancy', showindex=False)
        nz_table = tabulate(nz_df, headers='keys', tablefmt='fancy', showindex=False)
        acc_table = tabulate(acc_df, headers='keys', tablefmt='fancy', showindex=False)
        above_table = tabulate(above_df, headers='keys', tablefmt='fancy', showindex=False)
        below_table = tabulate(below_df, headers='keys', tablefmt='fancy', showindex=False)
        dp_table = tabulate(dp_df, headers='keys', tablefmt='fancy', showindex=False)
        near_table = tabulate(near_df, headers='keys', tablefmt='fancy', showindex=False)
        nearlow_table = tabulate(nearlow, headers='keys', tablefmt='fancy', showindex=False)


        embed = disnake.Embed(title=f"FUDSTOP DISCORD", description=f"# > DARK POOLS:\n```py\n{dp_table}```")
        embed.add_field(name=f"ABOE AVG. VOL:", value=f"```py\n{above_table}```")
        embed.add_field(name=f"BELOW AVG VOL:", value=f"```py\n{below_table}```")
        embed.add_field(name=f"FIRE SALE:", value=f"```py\n{fs_table}```")
        embed.add_field(name=f"NEUTRAL ZONE:", value=f"```py\n{nz_table}```")
        embed.add_field(name=f"ACCUMULATION:", value=f"```py\n{acc_table}```")
        embed.add_field(name=f"NEAR HIGH:", value=f"```py\n{near_table}```")
        embed.add_field(name=f"NEAR LOW:", value=f"```py\n{nearlow_table}```")
        embed.add_field(name=f"STOCK TRADES:", value=f"```py\n{trade_table}```")
        embed.add_field(name=f"OVERSOLD RSI:", value=f"```py\b{os_table}```", inline=False)
        embed.add_field(name=f"OVERBOUGHT RSI:", value=f"```py\b{ob_table}```", inline=False)


        await inter.edit_original_message(embed=embed)




# Specify the directory where your cogs are located
cogs_dir = "C:/users/chuck/markets/fudstop/fudstop/discord_/cogs"

print("Path being used:", cogs_dir)

# Get a list of all cog files in the specified directory
cog_files = [filename for filename in os.listdir(cogs_dir) if filename.endswith(".py")]

# Load each cog extension
for cog_file in cog_files:
    # Construct the full module name for the cog
    cog_name = f"cogs.{cog_file[:-3]}"  # Remove '.py' extension and prepend 'cogs.'

    try:
        # Load the cog extension
        bot.load_extension(cog_name)
        print(f"Loaded cog: {cog_name}")
    except Exception as e:
        print(f"Failed to load cog: {cog_name}\nError: {str(e)}")
bot.run(os.environ.get('RESEARCH_BOT'))