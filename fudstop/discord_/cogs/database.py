import os
import re
from dotenv import load_dotenv
load_dotenv()
from disnake.ext import commands
import disnake
import pandas as pd
from datetime import datetime
from apis.webull.webull_options import WebullOptions
import disnake
from disnake import TextInputStyle
from tabulate import tabulate
from discord_.bot_menus.pagination import AlertMenus
from apis.helpers import human_readable
from apis.webull.modal import WebullModal, VolumeAnalysisModal
from apis.polygonio.polygon_database import PolygonDatabase
from discord_.bot_menus.pagination import AlertMenus
from typing import List
options = WebullOptions(user='chuck', host='localhost', port=5432, password='fud', database='market_data')




class DatabaseCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
        self.db = PolygonDatabase(user='chuck', database='market_data', host='localhost', password='fud', port=5432)





    @commands.slash_command()
    async def database(self, inter):
        pass



            

    @database.sub_command()
    async def markets(self, inter:disnake.AppCmdInter):
        """Run live market streams"""
        await self.db.connect()
        await inter.response.defer()
        while True:

            indices_query = f"""SELECT ticker, ROUND(CAST((close - official_open) / official_open * 100 AS numeric), 2) AS change_percent
                                FROM indices_aggs_second
                                ORDER BY insertion_timestamp DESC
                                LIMIT 5;"""
            indicies_records = await self.db.fetch(indices_query)
            df = pd.DataFrame(indicies_records, columns=['name', 'change%'])

            indices_table = tabulate(df, headers='keys', tablefmt='fancy', showindex=False)

            stock_trades_query = f"SELECT ticker, trade_size, trade_price FROM stock_trades where trade_size >= 100 order by insertion_timestamp DESC LIMIT 15;"
            stock_trades_records = await self.db.fetch(stock_trades_query)
            stock_trade_df = pd.DataFrame(stock_trades_records, columns=['sym', 'size', 'price'])
            stock_trades_table = tabulate(stock_trade_df, headers='keys' , tablefmt='fancy', showindex=False)


            option_trades_query = f"SELECT ticker, strike, call_put, expiry, size FROM option_trades WHERE size >= 100 order by insertion_timestamp DESC LIMIT 15;"
            option_trades_records = await self.db.fetch(option_trades_query)

            option_trades_df = pd.DataFrame(option_trades_records, columns=['sym', 'strike', 'cp', 'exp', 'size'])

            option_trades_table = tabulate(option_trades_df, headers='keys', tablefmt='fancy', showindex=False)


            forex_aggs_query = f"SELECT ticker, close, volume FROM forex_aggs order by insertion_timestamp DESC LIMIT 15;"
            forex_aggs_records = await self.db.fetch(forex_aggs_query)

            forex_aggs_df = pd.DataFrame(forex_aggs_records, columns=['ticker', 'close', 'volume'])

            forex_aggs_table = tabulate(forex_aggs_df, headers='keys', tablefmt='fancy', showindex=False)

            crypto_trades_query = f"SELECT ticker, price, size, conditions FROM crypto_trades order by insertion_timestamp DESC LIMIT 15;"
            crypto_trades_records = await self.db.fetch(crypto_trades_query)

            crypto_trades_df = pd.DataFrame(crypto_trades_records, columns=['ticker', 'price', 'size', 'side'])

            crypto_trades_table = tabulate(crypto_trades_df, headers='keys', tablefmt='fancy', showindex=False)




            embed=  disnake.Embed(title=f"Live Markets", description=f"# > INDICES:\n```py\n{indices_table}```\n_ _ _\n# > STOCK TRADES:\n```py\n{stock_trades_table}```\n_ _ _\n# > OPTION TRADES:\n```py\n{option_trades_table}```\n# > FOREX AGGS:\n```py\n{forex_aggs_table}```\n# > CRYPTO TRADES:\n```py\n{crypto_trades_table}```")

            await inter.edit_original_message(embed=embed)

    @database.sub_command()
    async def feeds(self, inter:disnake.AppCmdInter):
        """Get discord feeds from the database."""
        await self.db.connect()
        await inter.response.defer()
        counter = 0
        while True:
            counter = counter + 1
            query = f"""SELECT ticker, status FROM feeds order by insertion_timestamp DESC LIMIT 25;"""


            records = await self.db.fetch(query)


            df = pd.DataFrame(records, columns=['ticker', 'status'])


            table = tabulate(df, headers='keys', tablefmt='fancy', showindex=False)





            embed = disnake.Embed(title="FUDSTOP Feeds", 
                                description=f'# > FUDSTOP FEEDS:\n\n_ _ _\n# > LIVE:\n```py\n{table}```', 
                                color=disnake.Colour.random())

            # Set thumbnail
            embed.set_thumbnail(url=os.environ.get('fudstop_logo'))




            await inter.edit_original_message(embed=embed)
            if counter == 500:
                await inter.send('Stream ended')

        
cog = DatabaseCOG(bot=commands.Bot)


class OptionsView(disnake.ui.View):
    def __init__(self, grouped_chunks, current_page):
        super().__init__()
        self.grouped_chunks = grouped_chunks
        self.current_page = current_page
        # Add buttons and select menus

        for chunk in grouped_chunks[current_page]:
            options = [disnake.SelectOption(label=human_readable(ticker), description='Click me for data!') for ticker in chunk]
            self.select_menu = TickerSelect(options)
            self.add_item(self.select_menu)

            

    async def generate_view(self, option_chunks, current_page):
        return self(option_chunks, current_page)
    @disnake.ui.button(label="Previous", custom_id="prev_page")
    async def prev_button(self, button: disnake.ui.Button, interaction: disnake.MessageInteraction):
        self.current_page -= 1
        if self.current_page >= 0:
            # Update view for the previous page
            new_view = await self.generate_view(self.grouped_chunks, self.current_page)
            await interaction.response.edit_message(view=new_view)
        else:
            await interaction.response.send_message("This is the first page.", ephemeral=True)


    @disnake.ui.button(label="Next", custom_id="next_page")
    async def next_button(self, button: disnake.ui.Button, interaction: disnake.MessageInteraction):
        self.current_page += 1
        if self.current_page < len(self.grouped_chunks):
            # Update view for the next page
            new_view = await self.generate_view(self.grouped_chunks, self.current_page)
            await interaction.response.edit_message(view=new_view)
        else:
            await interaction.response.send_message("This is the last page.", ephemeral=True)




class TickerSelect(disnake.ui.Select):
    def __init__(self, options, **kwargs):
        super().__init__(placeholder='Choose an option', options=options,min_values=1,max_values=25, **kwargs)

    async def callback(self, interaction: disnake.MessageInteraction):
        await interaction.response.defer(ephemeral=True)
        await cog.db.connect()


        embeds = []
        for value in self._selected_values:
            parts = value.split(' ')
            ticker = parts[0]
            strike = parts[1].replace('$','').replace('.00','')
            call_put = parts[2].lower()
            expiry = parts[4]      

            query = f"""SELECT distinct * FROM opts WHERE ticker = '{ticker}' AND strike = {strike} AND cp = '{call_put}' AND expiry = '{expiry}';"""
            print(query)
            records = await cog.db.fetch(query)
            for record in records:
                dte = record['dte']
                time_value = record['time_value']
                moneyness = record['moneyness']
                liquidity_score = record['liquidity_score']
                theta = record['theta']
                theta_decay_rate = record['theta_decay_rate']
                delta = record['delta']
                delta_theta_ratio = record['delta_theta_ratio']
                gamma = record['gamma']
                gamma_risk = record['gamma_risk']
                vega = record['vega']
                vega_impact = record['vega_impact']
                timestamp = record['timestamp']
                oi = record['oi']
                open_price = record['open']
                high = record['high']
                low = record['low']
                close = record['close']
                intrinstic_value = record['intrinstic_value']
                extrinsic_value = record['extrinsic_value']
                leverage_ratio = record['leverage_ratio']
                vwap = record['vwap']
                conditions = record['conditions']
                price = record['price']
                trade_size = record['trade_size']
                exchange = record['exchange']
                ask = record['ask']
                bid = record['bid']
                spread = record['spread']
                spread_pct = record['spread_pct']
                iv = record['iv']
                bid_size = record['bid_size']
                ask_size = record['ask_size']
                vol = record['vol']
                mid = record['mid']
                change_to_breakeven = record['change_to_breakeven']
                underlying_price = record['underlying_price']
                ticker = record['ticker']
                return_on_risk = record['return_on_risk']
                velocity = record['velocity']
                sensitivity = record['sensitivity']
                greeks_balance = record['greeks_balance']
                opp = record['opp']
                insertion_timestamp = record['insertion_timestamp']
                embed = disnake.Embed(title=f"Selected Option: {record['ticker']} ${record['strike']} {record['cp']} {record['expiry']}", description=f"```py\nDTE: {dte}\n> Time Value: ${time_value}\n> Intrinsic value: ${intrinstic_value}\n> Extrinsic Value: ${extrinsic_value}\n\n> Open: ${open_price}\n> High: ${high}\n> Low: ${low}\n> Close: ${close}```", color=disnake.Colour.dark_teal())
                embed.add_field(name=f"Volume & OI", value=f"> **{vol}** // **{oi}**")
                embed.add_field(name=f"Delta:", value=f"> Value: **{delta}**\n> Delta/Theta Ratio: {delta_theta_ratio}**")
                embed.add_field(name=f"Vega:", value=f"> Value: **{vega}**\n> Impact: **{vega}**")
                embed.add_field(name=f"Gamma:", value=f"> Value: **{gamma}**\n> Risk: **{gamma_risk}**")
                embed.add_field(name="Theta:", value=f"> Value: **{theta}**\n> Decay Rate: **{theta_decay_rate}**")
                embed.add_field(name=f"IV:", value=f"> Value: **{round(float(iv)*100,2)}%**\n> Sensitivity: **{sensitivity}**\n> Velocity: **{velocity}**")
                embed.add_field(name=f"Bid/Ask/Spread:", value=f"> Bid: **${bid}**\n> Ask: **${ask}**\n> Spread: **{round(float(spread),2)}**\n> Spread Pct: **{spread_pct}%**")
                embed.add_field(name=f"Return/Risk:", value=f"> RoR: **{return_on_risk}**\n> ProfitPotential: **{opp}**\n> Greek Balance: **{greeks_balance}**")
                embed.add_field(name=f"Entry Cost:", value=f"> Mid: **${mid}**\n> VWAP: **${vwap}**\n> {ticker} Price: **${underlying_price}**")
                embeds.append(embed)
            await interaction.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(self))
def setup(bot: commands.Bot):
    bot.add_cog(DatabaseCOG(bot))

    print(f"Database commands - READY!")