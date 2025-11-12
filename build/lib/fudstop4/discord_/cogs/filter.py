import disnake
from disnake.ext import commands
from discord_.bot_menus.pagination import AlertMenus
import pandas as pd
import asyncio
import os
import asyncpg
from dotenv import load_dotenv
from datetime import datetime
from tabulate import tabulate
from schema import run_conversation, tools
from options_filter import FilterView, FilterMenu
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
load_dotenv()



class FilterCog(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
        self.access_token = os.environ.get('ACCESS_TOKEN')
        self.osv = os.environ.get('OSV')
        self.did = os.environ.get('DID')
        self.opts = WebullOptions(database='fudstop', user='postgres')
        self.pool = None
        self.db_params = {
            'host': 'localhost',
            'user': 'postgres',
            'password': 'fud',
            'database': 'fudstop',
            'port': 5432
        }
        self.pool = None

    async def create_pool(self, min_size=1, max_size=10):
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    min_size=min_size, 
                    max_size=max_size, 
                    **self.db_params
                )
            except Exception as e:
                print(f"Error creating connection pool: {e}")
                raise

    async def close_pool(self):
        if self.pool:
            await self.pool.close()

    def chunk_string(self, string, size):
        """Yield successive size-sized chunks from string."""
        for i in range(0, len(string), size):
            yield string[i:i + size]

    def sanitize_value(self, value, col_type):
        """Sanitize and format the value for SQL query."""
        if col_type == 'str':
            # For strings, add single quotes
            return f"'{value}'"
        elif col_type == 'date':
            # For dates, format as 'YYYY-MM-DD'
            if isinstance(value, str):
                try:
                    datetime.strptime(value, '%Y-%m-%d')
                    return f"'{value}'"
                except ValueError:
                    raise ValueError(f"Invalid date format: {value}")
            elif isinstance(value, datetime):
                return f"'{value.strftime('%Y-%m-%d')}'"
        else:
            # For other types, use as is
            return str(value)
    async def get_connection(self):
        if self.pool is None:
            await self.create_pool()

        return await self.pool.acquire()

    async def release_connection(self, connection):
        await self.pool.release(connection)
   
    @commands.slash_command()
    async def filter(self, inter):
        pass

    @filter.sub_command()
    async def menu(self, inter:disnake.AppCmdInter):
        await inter.response.defer()
        await inter.edit_original_message(view=FilterView(), embed=FilterMenu())



        
    @filter.sub_command()
    async def ai(self, inter: disnake.AppCmdInter):
        await inter.response.defer()
        embed = disnake.Embed(title="GPT AI - Options Filter", description="Enter your filter request or type 'stop' to end.")
        await inter.edit_original_message(embed=embed)
        while True:
            # Prompt the user for input
            
            

            # Wait for the user's response
            try:
                message = await self.bot.wait_for('message', check=lambda m: m.author == inter.author and m.channel == inter.channel, timeout=60.0)
            except asyncio.TimeoutError:
                await inter.edit_original_message(content="No response received, session ended.")
                break

            # Check if the user wants to stop
            if message.content.lower() == 'stop':
                await inter.edit_original_message(content="Session ended.")
                break

            # Process the user's input
            results = await run_conversation(message.content)
            results = results.choices[0].message.content
            embed = disnake.Embed(title="GPT AI - Options Filter", description=f"```py\n{results}```")
            
            # Send the results
            await inter.edit_original_message(embed=embed)


def setup(bot: commands.Bot):
    bot.add_cog(FilterCog(bot))

    print('Filter - READY!')
