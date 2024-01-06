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
from apis.helpers import chunk_string, human_readable
from apis.webull.modal import WebullModal, VolumeAnalysisModal
from apis.polygonio.polygon_database import PolygonDatabase
from discord_.bot_menus.pagination import AlertMenus
from typing import List
options = WebullOptions(connection_string=os.environ.get('WEBULL_OPTIONS'))




class DatabaseCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
        self.db = PolygonDatabase(user='postgres', database='fudstop')





    @commands.slash_command()
    async def database(self, inter):
        pass



    @database.sub_command()
    async def rsi(self, inter:disnake.AppCmdInter, status:str=commands.Param(choices=['oversold', 'overbought', "overbought', 'oversold"])):
        """Search the database for overbought/oversold tickers."""
        await inter.response.defer()
        query = f"""
        SELECT ticker, rsi_value, timespan, status from rsi where status in ('{status}') AND timespan in ('day','week') ORDER BY timespan ASC;
        """
        print(query)
        await self.db.connect()
        records = await self.db.fetch(query)
        df = pd.DataFrame(records)
        table = tabulate(df, headers='keys', tablefmt='fancy', showindex=False)
        
        chunks = chunk_string(table, 4000)
        embeds = []
        for chunk in chunks:
            embed = disnake.Embed(title=f"RSI Results | Database", description=f"```py\n{chunk}```", color=disnake.Colour.dark_gold())

            embed.add_field(name=f"Query:", value=f"> **{query}**")
            embeds.append(embed)

        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds))
            

    @database.sub_command()
    async def atm_options(self, inter:disnake.AppCmdInter, ticker:str):
        """Returns options within 10% of the current strike in both directions."""
        await inter.response.defer()
        ticker = ticker.upper()

        data, tickers = await self.db.atm_options(ticker=ticker)

        # Assuming tickers is a list of option symbols
        options = tickers  # Use all available tickers
        MAX_OPTIONS_PER_SELECT = 25
        MAX_SELECTS_PER_VIEW = 4
        # Your logic for pagination
        chunks = [options[i:i + MAX_OPTIONS_PER_SELECT] for i in range(0, len(options), MAX_OPTIONS_PER_SELECT)]
        grouped_chunks = [chunks[i:i + MAX_SELECTS_PER_VIEW] for i in range(0, len(chunks), MAX_SELECTS_PER_VIEW)]

        current_page = 0

        table = tabulate(data.sort_values('exp', ascending=True), headers='keys', tablefmt='fancy', showindex=False)

        chunks = chunk_string(table, 4000)
        
        embeds = [ ]
        for chunk in chunks:
            embed = disnake.Embed(title=f'Results for {ticker}', description=f"```py\n{chunk}```", color=disnake.Colour.dark_gold())
            embeds.append(embed)
        # In your atm_options function
        view = OptionsView(grouped_chunks, current_page)
        await inter.edit_original_message(view=view, embed=embeds[0])


    # @database.sub_command()
    # async def custom_filter(self, inter:disnake.AppCmdInter, ticker=None, call_put=None, strike_min=None, strike_max=None, 
    #             expiry_min=None, expiry_max=None,dte_min=None,dte_max=None,
    #             time_value_min=None, time_value_max=None, 
    #             intrinsic_value_min=None, intrinsic_value_max=None, 
    #             extrinsic_value_min=None, extrinsic_value_max=None, 
    #             vol_min=None, vol_max=None, oi_min=None,oi_max=None, 
    #             theta_min=None, theta_max=None, gamma_min=None,
    #             gamma_max=None, gamma_risk_min=None, gamma_risk_max=None, 
    #             delta_min=None, delta_max=None, delta_theta_ratio_min=None, 
    #             delta_theta_ratio_max=None, vega_min=None,
    #             vega_max=None, vega_impact_min=None, vega_impact_max=None):
    #     await inter.response.defer()
    #     await self.db.connect()

    #     # Base query string
    #     base_query = "SELECT * FROM opts WHERE "
    #     conditions = []

    #     # Check each parameter and append condition if not None
    #     if ticker is not None:
    #         conditions.append(f"ticker = '{ticker}'")
    #     if call_put is not None:
    #         conditions.append(f"call_put = '{call_put}'")
        
    #     # Add conditions for range parameters
    #     def add_range_condition(param_min, param_max, column_name):
    #         if param_min is not None:
    #             conditions.append(f"{column_name} >= {param_min}")
    #         if param_max is not None:
    #             conditions.append(f"{column_name} <= {param_max}")

    #     # Apply for all range parameters
    #     add_range_condition(strike_min, strike_max, "strike")
    #     add_range_condition(expiry_min, expiry_max, "expiry")
    #     add_range_condition(dte_min, dte_max, "dte")
    #     add_range_condition(time_value_min, time_value_max, "time_value")
    #     add_range_condition(intrinsic_value_min, intrinsic_value_max, "intrinsic_value")
    #     add_range_condition(extrinsic_value_min, extrinsic_value_max, "extrinsic_value")
    #     add_range_condition(vol_min, vol_max, "vol")
    #     add_range_condition(oi_min, oi_max, "oi")
    #     add_range_condition(theta_min, theta_max, "theta")
    #     add_range_condition(gamma_min, gamma_max, "gamma")
    #     add_range_condition(gamma_risk_min, gamma_risk_max, "gamma_risk")
    #     add_range_condition(delta_min, delta_max, "delta")
    #     add_range_condition(delta_theta_ratio_min, delta_theta_ratio_max, "delta_theta_ratio")
    #     add_range_condition(vega_min, vega_max, "vega")
    #     add_range_condition(vega_impact_min, vega_impact_max, "vega_impact")


    #     # Final query
    #     query = base_query + ' AND '.join(conditions) if conditions else "SELECT * FROM options"
    #     print(query)
    #     # Execute the query
    #     data = await self.db.fetch(query)
    #     df = pd.DataFrame(data).iloc[[0]]
    #     embed = disnake.Embed(title=f"Test", description=f"```py\n{df}```")
    #     await inter.edit_original_message(embed=embed)


    @database.sub_command()
    async def strategy(self, inter:disnake.AppCmdInter, strategy:str=commands.Param(choices=['low theta'])):
        """Choose a strategy to run for options - returns a spreadsheet."""
        await inter.response.defer()
        if strategy == 'low theta':
            data = await self.db.strategy_filter_theta()
            print(data)


            data.to_csv('data.csv')

            # table = tabulate(df_selected, headers='keys')

            # chunks = chunk_string(table, 4000)
            # embeds = []
            # for chunk in chunks:
            #     embed = disnake.Embed(title='Theta Stratey Results', description=f"```py\n{chunk}```", color=disnake.Colour.dark_gold())

            #     embed.add_field(name=f"Low Theta", value='```py\nThis strategy takes advantage of time on the clock as well as low-theta. This allows for an easily managed position and works in over-extended markets. [the current situation].```')

            #     embeds.append(embed)

            # await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds))

            await inter.send(file=disnake.File('data.csv'))

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