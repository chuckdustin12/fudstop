
import os
import sys
from pathlib import Path
import disnake
from disnake.ext.commands.errors import CommandInvokeError
from pytrends.request import TrendReq
from disnake.ext import commands
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from apis.webull.webull_screener import WebulScreener, ScreenerSelect
from datetime import datetime
from apis.openai_ import OpenAISDK
from fudstop.apis.polygonio.polygon_options import PolygonOptions
import disnake
from apis.y_finance.yf_sdk import yfSDK
import base64
from disnake.ext import commands
from list_sets.ticker_lists import most_active_tickers
most_active_tickers = set(most_active_tickers)
from bot_menus.modals.options_modal import OptionsDataModal
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import asyncio
from bot_menus.pagination import AlertMenus
import aiohttp
from openai import OpenAI
import matplotlib.pyplot as plt
import io
import asyncpg
import pandas as pd
from cogs.database import MyModal
from apis.polygonio.polygon_options import PolygonOptions
from apis.webull.opt_modal import OptionModal, SQLQueryModal
from apis.gexbot.gexbot import GEXBot
td9_ids = [
    int("1158471263984029777"),  # Channel: td9⏺5minute
    int("1158488492163211264"),  # Channel: td9⏺day
    int("1158867480853352583"),  # Channel: td9⏺15minute
    int("1158867482824675358"),  # Channel: td9⏺30minute
    int("1158868591911899298"),  # Channel: td9⏺hour
    int("1158868593967108096"),  # Channel: td9⏺2hr
    int("1158868595619672135"),  # Channel: td9⏺4hr
    int("1161334711197630566"),  # Channel: td9⏺20minute
    int("1151905252392575067")   # Additional channel ID
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
    "user": os.environ.get('DB_USER', 'postgres'), # Default to 'postgres' if 'DB_USER' not found
    "password": os.environ.get('DB_PASSWORD', 'fud'), # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME', 'polygon') # Database name for the new jawless database
}
opts = PolygonOptions(**db_config)
gexbot = GEXBot()
bot = commands.Bot(command_prefix="!", intents=disnake.Intents.all())
gptsdk=OpenAISDK()
opts = PolygonOptions(**db_config)
from cogs.database import QueryView
from list_sets.ticker_lists import gex_tickers
from typing import List
import disnake
from disnake.ext import commands
import openai

import os
from dotenv import load_dotenv
load_dotenv()

sdk = yfSDK()
# Initialize the OpenAI client with your API key
openai.api_key = os.getenv("YOUR_OPENAI_KEY")

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
@bot.event
async def on_message(message: disnake.Message):
    """Use  GPT4 Vision to listen for image URLs"""
    embeds = message.embeds


    titles = [i.title for i in embeds]
    descriptions = [i.description for i in embeds]
    fields = [i.fields for i in embeds]

    if message.channel.id == 1175195864814321834:
        if message.content.endswith('.jpg'):
            url = message.content
            analysis = gptsdk.analyze_stock(url)

            embed = disnake.Embed(title=f"GPT Vision", description=f"> **{analysis}**", color=disnake.Colour.random())
            embed.set_thumbnail(url)

            await message.author.send(embed=embed)


    if message.channel.id == 1175195864814321834:
        if message.attachments:
            print(message.content)
            url = message.attachments[0].proxy_url
            analysis = gptsdk.analyze_stock(url)

            embed = disnake.Embed(title=f"GPT Vision", description=f"> **{analysis}**", color=disnake.Colour.random())
            embed.set_thumbnail(url)

            await message.channel.send(embed=embed)


    if message.channel.id in td9_ids:
        description = embeds[0].description
        description = description.split('py\n')[3]
        title = embeds[0].title
        timestamp = embeds[0].timestamp
        timestamp = str(timestamp.astimezone()).split('.')[0]
        print(description,title, timestamp)
        

    if message.channel.id in opt_vol_ids:
        description = embeds[0].description
        description = description.split('py\n')[0]
        title = embeds[0].title
        timestamp = embeds[0].timestamp
        timestamp = str(timestamp.astimezone()).split('.')[0]


        footer = embeds[0].footer.text
        print(footer, timestamp)


    await bot.process_commands(message)
# This dictionary will hold the conversation state for each user
conversations = {}
client = OpenAI(api_key=os.environ.get('YOUR_OPENAI_KEY'))
@bot.slash_command()
async def option_data(inter: disnake.ApplicationCommandInteraction):
    modal = OptionModal()
    await inter.response.send_modal(modal)



@bot.slash_command()
async def options_database(inter:disnake.AppCmdInter):
    """Query options data"""
    await inter.response.send_modal(OptionsDataModal())


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

from live_markets.stock_market import StockMarketLive




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



@bot.command()
async def query(inter:disnake.AppCmdInter, ticker:str):
    await inter.send(view=QueryView(ticker))


@bot.command()
async def test(ctx, ticker):
    await ctx.send(view=QueryView(ticker))


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

@bot.command(name='cheapies')
async def plays(ctx):
    options = await fetch_options()
    if options:
        message = "Options:\n"
        for option in options:
            message += (f"{option['ticker']}, "
                        f"{option['strike']}, "
                        f"{option['call_put']}, "
                        f"{option['expiry']}, "
                        f"{option['current_price']}\n")
        embed = disnake.Embed(title=f"Cheapies - All Time Lows", description=f"```py\n{message}```")
        await ctx.send(embed=embed)
    else:
        message = "No options found."


pytrends = TrendReq(hl='en-US', tz=360)
@bot.command()
async def trends(ctx):


    
    trending_searches_df = pytrends.trending_searches(pn='united_states')  # you can change the region
    embed = disnake.Embed(title=f"Current Trends on Google:", description=f"```py\n{trending_searches_df}```", color=disnake.Colour.dark_green())
    await ctx.send(embed=embed)


@bot.command()
async def mf(ctx, ticker: str):
    """Gets mutual fund holdings for a ticker - top 10"""
    try:
        data = sdk.mutual_fund_holders(ticker)
        
        filename = f'data/yf_{ticker}_mf_holders.csv'
        data.to_csv(filename)
        
        embed = disnake.Embed(
            title=f"Mutual Fund Holders - {ticker}", 
            description=f"Your Download is Ready!", 
            color=disnake.Colour.dark_teal()
        )
        embed.set_footer(text=f'Implemented by FUDSTOP')
        
        await ctx.send(embed=embed)
        await ctx.send(file=disnake.File(filename))
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")


@bot.command()
async def balance(ctx, ticker: str):
    """Gets balance sheet information for a ticker"""
    try:
        data = sdk.balance_sheet(ticker)
        
        filename = f'data/yf_balance_sheet.csv'
        data.to_csv(filename)
        
        await ctx.send(f"Balance Sheet for {ticker}:")
        await ctx.send(file=disnake.File(filename))
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")

@bot.command()
async def cashflow(ctx, ticker: str):
    """Gets cash flow information for a ticker"""
    try:
        data = sdk.get_cash_flow(ticker)
        
        filename = f'data/yf_cash_flow.csv'
        data.to_csv(filename)
        
        await ctx.send(f"Cash Flow for {ticker}:")
        await ctx.send(file=disnake.File(filename))
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")

@bot.command()
async def financials(ctx, ticker: str):
    """Gets all financials for a ticker"""
    try:
        data = sdk.financials(ticker)
        
        filename = f'data/yf_financials.csv'
        data.to_csv(filename)
        
        await ctx.send(f"Financials for {ticker}:")
        await ctx.send(file=disnake.File(filename))
    except Exception as e:
        await ctx.send(f"An error occurred: {e}")


# Command for the income_statement method within yfSDK
@bot.command()
async def statement(ctx, ticker: str, frequency: str = 'quarterly', pretty: bool = False, as_dict: bool = False):
    """Gets the income statement for a ticker"""
    data = sdk.income_statement(ticker, frequency=frequency, pretty=pretty, as_dict=as_dict)
    filename = f'data/yf_income_statement.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))

# Command for the get_info method within yfSDK
@bot.command()
async def info(ctx, ticker: str):
    """Returns a large dictionary of information for a ticker"""
    data = sdk.get_info(ticker)
    filename = f'data/yf_info.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))

# Command for the institutional_holdings method within yfSDK
@bot.command()
async def whales(ctx, ticker: str):
    """Gets institutional holdings for a ticker"""
    data = sdk.institutional_holdings(ticker)
    filename = f'data/yf_institutional_holdings.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))


@bot.command()
async def div(ctx, ticker: str):
    """Gets dividends for a ticker - if any."""
    data = sdk.dividends(ticker)
    filename = f'data/dividends.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))


@bot.command()
async def allinfo(ctx, ticker: str):
    """Gets all relevant company data for a ticker."""
    data = sdk.fast_info(ticker)
    filename = f'data/fast_info.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))


@bot.command()
async def candles(ctx, *, ticker: str):
    """Gets all candlestick data for a ticker"""
    data = sdk.get_all_candles(ticker)
    filename = f'data/all_candles.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))


# Command for the news method within yfSDK
@bot.command()
async def news(ctx, ticker: str):
    """Gets ticker news"""
    data = sdk.news(ticker)
    filename = f'data/yf_{ticker}_news.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))

# Command for the atm_calls method within yfSDK
@bot.command()
async def calls(ctx, ticker: str):
    """Gets at the money calls for a ticker"""
    data = sdk.atm_calls(ticker)
    data = pd.DataFrame(data)
    filename = f'data/yf_atm_calls.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))

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

@bot.command()
async def puts(ctx, ticker: str):
    """Gets at the money puts for a ticker"""
    data = sdk.atm_puts(ticker)
    data = pd.DataFrame(data)
    filename = f'data/yf_atm_puts.csv'
    data.to_csv(filename)
    await ctx.send(file=disnake.File(filename))


@bot.command()
async def related(ctx, keyword):
    related_queries = pytrends.related_queries()
    # Define keywords
    keywords = [f"{keyword}"]

    # Build payload
    pytrends.build_payload(keywords, timeframe='today 12-m')

    # Fetch related queries
    related_queries = pytrends.related_queries()

    
    embed = disnake.Embed(title=f"Current Trends on Google:", description=f"```py\n{related_queries}```", color=disnake.Colour.dark_green())
    await ctx.send(embed=embed)

cogs_dir = "C:/users/chuck/fudstop/fudstop/cogs"
print("Path being used:", cogs_dir)
for extension in disnake.utils.search_directory(cogs_dir):
    bot.load_extension(extension)



bot.run(os.environ.get('BOT'))