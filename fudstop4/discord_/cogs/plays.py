import os
from dotenv import load_dotenv
load_dotenv()
import disnake
from disnake.ext import commands
from tabulate import tabulate
import pandas as pd
from apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.polygonio.polygon_options import PolygonOptions

from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
import datetime
from discord_.bot_menus.pagination import AlertMenus

wb_opts = WebullOptions(database='fudstop3', user='chuck')
db = PolygonOptions(user='chuck', database='charlie', host='localhost', port=5432, password='fud')
opts = PolygonOptions(user='chuck', database='charlie', host='localhost', port=5432, password='fud')


class PlaysCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot


    def nearest_friday(self):
        today = datetime.date.today()
        # Find the number of days to add or subtract to get to the nearest Friday
        # Friday is represented by 4 in Python's date.weekday() (Monday is 0, Sunday is 6)
        days_ahead = 4 - today.weekday()
        if days_ahead <= 0:  # If today is after Friday, move to next week
            days_ahead += 7
        nearest_friday = today + datetime.timedelta(days=days_ahead)
        return nearest_friday.strftime('%Y-%m-%d')

    @commands.slash_command()
    async def plays(self, inter):
        pass



    @plays.sub_command()
    async def easy_mode(self, inter:disnake.AppCmdInter, type:str=commands.Param(choices=['calls', 'puts'])):
        """Low Theta - With Time on Clock | | Works In Overbought/Oversold Markets"""
        await inter.response.defer()
        await inter.edit_original_message(f"Finding plays...")

        if type == 'calls':
            calls, puts = await opts.find_plays()
            

            
            await inter.edit_original_message(file=disnake.File(calls.to_csv('calls.csv')))

        elif type =='puts':
            calls, puts = await opts.find_plays()
            await inter.edit_original_message(file=disnake.File(puts.to_csv('puts.csv')))

  
    @plays.sub_command()
    async def check_rsi(self, inter:disnake.AppCmdInter):
        """Check RSI for overbought/oversold tickers across multiple timeframes."""
        await inter.response.defer()

        await db.connect()
        await inter.edit_original_message('Please wait while I refresh the database records..')
        await db.update_all_rsi()
        query = f"""SELECT ticker, timespan, rsi_value, status FROM rsi ORDER BY timespan ASC"""
        records = await db.fetch(query)

        df = pd.DataFrame(records, columns=['ticker', 'timespan', 'rsi', 'status'])
        # Define thresholds
        OVERSOLD_THRESHOLD = 30
        OVERBOUGHT_THRESHOLD = 70

        # Create 'oversold' DataFrame
        oversold_df = df[df['rsi'] < OVERSOLD_THRESHOLD]

        # Create 'overbought' DataFrame
        overbought_df = df[df['rsi'] > OVERBOUGHT_THRESHOLD]
        oversold_table = tabulate(oversold_df, headers='keys', tablefmt='fancy', showindex='false')
        overbought_table = tabulate(overbought_df, headers='keys', tablefmt='fancy', showindex='false')

        oversold_chunks = db.chunk_string(oversold_table, 2000)
        overbought_chunks = db.chunk_string(overbought_table, 2000)
        embeds=[]
        for oversold_chunk, overbought_chunk in zip(overbought_chunks, oversold_chunks):
            embed = disnake.Embed(title=f"Overbought / Oversold RSI", description=f"# > OVERBOUGHT:\n```py\n{overbought_chunk}```\n\n_ _ _\n\n_ _ _\n\n# > OVERSOLD:\n```py\n{oversold_chunk}```")
            embeds.append(embed)

        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds))
def setup(bot: commands.Bot):
    bot.add_cog(PlaysCOG(bot))
    print('PLAYS READY')