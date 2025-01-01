import os
from dotenv import load_dotenv
load_dotenv()
import matplotlib.pyplot as plt
import disnake
import plotly.graph_objects as go
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import aiohttp

from plots import plot_calls_and_puts,plot_greek_exposure,plot_iv_surface
from disnake.ext import commands
from discord_.bot_menus.pagination import AlertMenus, PageSelect
from discord_.autocomp import ticker_autocomp,strike_autocomp,expiry_autocomp
from apis.polygonio.polygon_options import PolygonOptions
from apis.webull.webull_options import WebullOptions
from apis.webull.webull_trading import WebullTrading
from apis._openai.openai_sdk import OpenAISDK
from apis.polygonio.polygon_database import PolygonDatabase
from tabulate import tabulate
from all_helpers import determine_emoji
import asyncio
from apis.helpers import format_large_numbers_in_dataframe
import pandas as pd


class OptionsCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
        self.poly = PolygonOptions(user='chuck', database='market_data', host='localhost', port=5432, password='fud')
        self.wt = WebullTrading()
        self.wb = WebullOptions(user='chuck', database='market_data', host='localhost', password='fud', port=5432)
        self.db = PolygonDatabase(user='chuck', database='market_data', host='localhost', password='fud', port=5432)
        self.ai = OpenAISDK()


    @commands.slash_command()
    async def options(self, inter):
        pass


    @options.sub_command()
    async def full_skew(self, inter:disnake.AppCmdInter, ticker:str):
        """Gets the lowest IV call/put option for all expirations for a ticker"""
        ticker = ticker.upper()
        await inter.response.defer()
        full_skew, price = await self.poly.full_skew(ticker)
        full_skew['iv'] = (full_skew['iv'] * 100).round(2)
        full_skew = full_skew.rename(columns={'strike': 'skew_strike'})
        full_skew = full_skew.drop(columns=['call_put'])
        table = tabulate(full_skew, headers='keys', tablefmt='fancy', showindex=False)


        chunks = [table[i:i + 3860] for i in range(0, len(table), 3860)]
        embeds=[]
        for chunk in chunks:
            embed = disnake.Embed(title=f"Full Skew - {ticker}", description=f"> Current Price: **${price}**\n```py\n{chunk}```", color=disnake.Colour.dark_orange())
            embed.add_field(name=f"Full Skew for: {ticker}",value=f"> Price: **{price}**")
            embed.set_footer(text=f'Full Skew: {ticker} | Data by Polygon.io | Implemented by FUDSTOP')
            embeds.append(embed)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, label='Download')
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('full_skew.csv'))

        
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(PageSelect(embeds)).add_item(button).add_item(OptsSelect(ticker)))


    @options.sub_command()
    async def top_vol_strikes(self, inter:disnake.AppCmdInter, ticker:str):
        """
        Gets the top volume strikes across all expirations for calls/puts.
        
        """
        await inter.response.defer()
        ticker = ticker.upper()

        data = await self.poly.vol_oi_top_strike(ticker)

        # Ensure 'expiry' is a datetime column
        data['expiry'] = pd.to_datetime(data['expiry'])

        # Change date format to 'YY-MM-DD'
        data['expiry'] = data['expiry'].dt.strftime('%y-%m-%d')

        data = format_large_numbers_in_dataframe(data)
        data = data.drop(columns=['ticker'])
        data.to_csv('top_vol_strikes.csv', index=False)
        table = tabulate(data, headers='keys', tablefmt='fancy', showindex=False)


        chunks = [table[i:i + 3860] for i in range(0, len(table), 3860)]
        embeds=[]
        for chunk in chunks:
            embed = disnake.Embed(title=f"TopVol Strikes - {ticker}", description=f"```py\n{chunk}```", color=disnake.Colour.dark_orange())
            embed.set_footer(text=f'TopVol Strikes: {ticker} | Data by Polygon.io | Implemented by FUDSTOP')
            embeds.append(embed)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, label='Download')
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('top_strike_vol.csv'))

        
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(PageSelect(embeds)).add_item(button).add_item(OptsSelect(ticker)))



    @options.sub_command()
    async def volume_analysis(self, inter:disnake.AppCmdInter, ticker:str, date:str):
        """Enter a date in YYYY-MM-DD and view the volume analysis per strike!"""
        ticker =ticker.upper()
        await inter.response.defer()

        
        headers = self.wb.headers
        option_ids = await self.wb.get_option_ids(ticker)  # Adjust based on actual return structure

        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [self.wb.fetch_volume_analysis(session, option_symbol, id, underlying_ticker) for option_symbol, id, underlying_ticker in option_ids]
            dfs = await asyncio.gather(*tasks)  # Run all tasks concurrently
            
        final_df = pd.concat(dfs, ignore_index=True)
        print(final_df)
        price = await self.wt.stock_quote(ticker)
        price_ = price.web_stock_close
        final_df['underlying_price'] = price_
        final_df = final_df.sort_values('buy', ascending=False)
        final_df.to_csv(f'volume_analysis.csv', index=False)

        def plot_volume_by_strike_for_date(df, target_date, option_type='all', dark_background=True):
            # Extract data for the specific date
            target_df = df[(df['expiry'] == pd.to_datetime(target_date)) & (df['call_put'] == option_type)]
            
            # If no data available for the target_date, inform and return without plotting
            if target_df.empty:
                print(f"No data available for the date: {target_date} and type: {option_type}")
                return

            plt.style.use('dark_background' if dark_background else 'default')
            
            # Create a plot with dark background
            fig, ax = plt.subplots(figsize=(12, 6), facecolor='black' if dark_background else 'white')
            
            # Plot buy and sell volumes for calls and puts separately
            calls = target_df[target_df['option'] == 'call']
            puts = target_df[target_df['option'] == 'put']
            
            width = (target_df['strike'].max() - target_df['strike'].min()) / len(target_df['strike'].unique()) * 0.4
            
            # Call Volumes
            ax.bar(calls['strike'] - width/2, calls['buy'], width=width, label='Call Buy Volume', color='blue', alpha=0.7)
            ax.bar(calls['strike'] - width/2, -calls['sell'], width=width, label='Call Sell Volume', color='lightblue', alpha=0.7)
            
            # Put Volumes
            ax.bar(puts['strike'] + width/2, puts['buy'], width=width, label='Put Buy Volume', color='purple', alpha=0.7)
            ax.bar(puts['strike'] + width/2, -puts['sell'], width=width, label='Put Sell Volume', color='pink', alpha=0.7)
            
            # Set title and axes labels with gold color and high readability
            ax.set_title(f'Call/Put Buy/Sell Volume by Strike for Expiry: {pd.to_datetime(target_date).strftime("%Y-%m-%d")}', color='gold')
            ax.set_xlabel('Strike Price', color='gold')
            ax.set_ylabel('Volume', color='gold')
            
            # Tick parameters for readability
            ax.tick_params(axis='x', colors='gold')
            ax.tick_params(axis='y', colors='gold')

            # Adding grid for better readability, with a lighter color
            ax.grid(True, color='dimgray')
            
            # Show legend with readable font color
            ax.legend(facecolor='darkgray', edgecolor='gold', fontsize='large')
            
            # Show the plot
            plt.tight_layout()
            plt.savefig('plot.png')
            plt.show()

        # Example usage:
        # Load the data (make sure to parse the 'date' and 'expiry' columns correctly)
        df = pd.read_csv('volume_analysis.csv', parse_dates=['date', 'expiry'])

        # Filter out rows with missing values in the volume, strike, or expiry columns if necessary
        # df = df.dropna(subset=['buy_volume', 'sell_volume', 'strike', 'expiry'])

        # Call the function with the chosen date
        plot_volume_by_strike_for_date(df, date, dark_background=True)
        file = disnake.File('plot.png', filename='plot.png')
        embed = disnake.Embed(title=f"Volume Analysis - {ticker} | {date}")
        
        embed.set_image(url="attachment://plot.png")

        await inter.edit_original_message(embed=embed, file=file)


    @options.sub_command()
    async def all(self, inter:disnake.AppCmdInter, ticker:str):
        
        """Returns a CSV file of all options and accompanying data for a ticker."""
        await self.poly.connect()
        if ticker == 'SPX':
            ticker = 'I:SPX'
        elif ticker == 'NDX':
            ticker = 'I:NDX'
        elif ticker == 'VIX':
            ticker = 'I:VIX'
        await inter.response.defer(ephemeral=False)
        ticker = ticker.upper()

        all_options = await self.poly.get_option_chain_all(ticker)
        
        all_options.df.to_csv('all_options.csv', index=False)
        df = all_options.df
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'ticker': 'symbol', 'oi': 'open_interest', 'cp': "call_put", 'exp': 'expiry', 'vol': 'volume', 'entrycost': "'entry_cost'"})
        df = df.drop(columns=['conditions', 'exchange'])

        await self.poly.batch_insert_dataframe(df, table_name='optionsall', unique_columns='symbol, strike, expiry, call_put')
        embed = disnake.Embed(title=f"All Options - {ticker}", description=f'```py\nYour downloadable CSV is ready!```', color=disnake.Colour.brand_green())
        embed.add_field(name=f"Info:", value=f"This file contains all options for {ticker} across ALL expiration dates and contains ALL accompanying data.")
        embed.set_footer(text="Data Provided by Polygon.IO | Implemented by FUDSTOP", icon_url=os.environ.get('fudstop'))
        await inter.edit_original_message(embed=embed,file=disnake.File('all_options.csv'), view=OptionsView(ticker).add_item(OptsSelect(ticker)))







        return df
    







    # @options.sub_command()
    # async def snapshot(self, inter:disnake.AppCmdInter, ticker:str=commands.Param(autocomplete=ticker_autocomp), strike:str=commands.Param(autocomplete=strike_autocomp), expiry:str=commands.Param(autocomplete=expiry_autocomp), call_put: str=commands.Param(choices=["Call", "Put"])):
    #     """View the snapshot of an option contract!"""
    #     await inter.response.defer()
    #     ticker = build_option_symbol(underlying_symbol=ticker,strike_price=strike,call_or_put=call_put,expiry=expiry,prefix_o=True)
    #     print(ticker)
    #     snapshot_data = await master.get_universal_snapshot(ticker)
    #     if snapshot_data.change_percent and snapshot_data.change_percent[0] is not None:  # Check if the list is not empty
    #         if snapshot_data.change_percent[0] > 0:
    #             color = disnake.Colour.dark_green()
    #         elif snapshot_data.change_percent[0] < 0:
    #             color = disnake.Colour.dark_red()
    #         else:
    #             color = disnake.Colour.dark_grey()
    #             print("change_percent is empty")
    #     if snapshot_data.volume[0] is not None and snapshot_data.open_interest[0] is not None:
    #         if snapshot_data.volume[0] > snapshot_data.open_interest[0]:
    #             # Your logic here
    #             unusual = 'YES'
    #         else:
    #             unusual = 'NO'
    #         # Handle the case where one or both of the values are None
    #         print("Either volume or open_interest or both are None.")
    #     else:
    #         unusual = 'N/A'
            

    #     embed = disnake.Embed(title=f"Option Snapshot - {snapshot_data.underlying_ticker[0]}", description=f"```py\nViewing the most up-to-date option data for {snapshot_data.underlying_ticker[0]} | {snapshot_data.strike[0]} | {snapshot_data.expiry[0]} | {snapshot_data.contract_type}.```\n> *Color of the embed is represented by the change % of the contract on the day.*", color=color)
    #     embed.add_field(name=f"Day:", value=f"> Open: **${snapshot_data.open[0]}\n> High: **${snapshot_data.high[0]}**\n> Low: **${snapshot_data.low[0]}**\n> Close: **${snapshot_data.close[0]}**\n> PrevClose: **${snapshot_data.prev_close[0]}**\n> Change%: **{round(float(snapshot_data.change_percent[0]),2)}%**", inline=False)
    #     try:
    #         embed.add_field(name=f"Greeks:", value=f"> Delta: **{round(float(snapshot_data.delta[0]),2)}**\n> Gamma: **{round(float(snapshot_data.gamma[0]),2)}**\n> Theta: **{round(float(snapshot_data.theta[0]),2)}**\n> Vega: **{round(float(snapshot_data.vega[0]),2)}**")
    #     except TypeError:
    #         embed.add_field(name=f"Greeks:", value=f"> Delta: **{snapshot_data.delta}**\n> Gamma: **{snapshot_data.gamma}**\n> Theta: **{snapshot_data.theta}**\n> Vega: **{snapshot_data.vega}**")
    #     embed.add_field(name=f"Vol. Vs OI:", value=f"> VOL: **{float(snapshot_data.volume[0]):,}**\n> OI: **{float(snapshot_data.open_interest[0]):,}**\n> Unusual? **{unusual}**")
    #     embed.add_field(name=f"Last Trade:", value=f"> Size: **{snapshot_data.trade_size[0]}**\n> Price: **${snapshot_data.trade_price[0]}**\n> Exchange: **${snapshot_data.exchange[0]}**\n> Conditions: **{snapshot_data.conditions[0]}**")
    #     embed.add_field(name=f"Last Quote:", value=f"> Bid: **{snapshot_data.bid[0]}**\n> Bid Size: **{snapshot_data.bid_size[0]}**\n> Mid: **${snapshot_data.midpoint[0]}**\n> Ask: **{snapshot_data.ask[0]}**\n> Ask Size: **{snapshot_data.ask_size[0]}**")
    #     embed.set_footer(text=f"{ticker} | Data Provided by Polygon.io | Implemented by FUDSTOP")
  


    #     await inter.edit_original_message(embed=embed, view=OptionsView(ticker, self.bot,snapshot_data.data_dict))



    @options.sub_command()
    async def allskew(self, inter:disnake.AppCmdInter):
        """Scans and returns all skews with a depth of 5 or more, or -5 or less."""
        await inter.response.defer()


        tasks = [self.db.process_ticker(ticker) for ticker in self.wb.most_active_tickers]
        results = await asyncio.gather(*tasks)

        # Filter out None values and print the first non-None result
        valid_results = [result for result in results if result is not None]
        

        df = pd.DataFrame(valid_results)

        df['expiration'] = pd.to_datetime(df['expiration']).dt.strftime('%y/%m/%d')
        # Apply the function to add a new column to the DataFrame
        df['dir'] = df.apply(determine_emoji, axis=1)

        # Tabulate the DataFrame
        tabulated_data = tabulate(df, headers='keys', tablefmt='fancy', showindex=False)
        


        chunks = [tabulated_data[i:i + 4000] for i in range(0, len(tabulated_data), 4000)]
        embeds=[]
        for chunk in chunks:
            embed = disnake.Embed(title=f"All Skew", description=f"```py\n{chunk}```", color=disnake.Colour.dark_purple())
            embed.add_field(name=f"_ _", value=f"> **Viewing all skews.**")
            embeds.append(embed)

        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds))




    @options.sub_command()
    async def iv_skew(self, inter:disnake.AppCmdInter, ticker: str):
        # Step 1: Fetch Data
        ticker = ticker.upper()
        await inter.response.defer()
        all_options = await opts.get_option_chain_all(ticker)
        all_options = all_options.df.sort_values('strike')
        all_options.to_csv('all_options.csv', index=False)
        # Aggregate IV by strike (taking average for this example)
        # Sort by IV and take the first (smallest) value
        min_iv_row = all_options.sort_values('iv', ascending=True).iloc[0]
        full_iv_sorted = all_options.sort_values('iv', ascending=True)

        min_iv_value = min_iv_row['iv']
        min_iv_strike = min_iv_row['strike']


        # Step 2: Visualize Data with Plotly
        # Now plot
        # Create the figure
        fig = go.Figure()

        # Add the IV vs Strike curve
        fig.add_trace(go.Scatter(
            x=full_iv_sorted['strike'],
            y=full_iv_sorted['iv'],
            mode='lines+markers',
            line=dict(color='blue', width=2.0),  # Vibrant blue line with increased width
            marker=dict(size=8, color='purple', opacity=0.7)  # Red markers
        ))

        # Extract the UNDERLYING_PRICE (assuming it's a constant for all rows, hence taking the first row's value)
        underlying_price = all_options.iloc[0]['price']

        # Update the title to include the underlying price
        fig.update_layout(
            title=f"{ticker} Consolidated IV Skew | Underlying Price: ${underlying_price:.2f}",  # Format it to 2 decimal places
            titlefont=dict(size=20),
            xaxis_title="Strike",
            yaxis_title="Implied Volatility",
            template="plotly_dark"
        )

        # If you want to add it as an annotation next to the lowest IV strike:
        if min_iv_strike > underlying_price:
            result = "🟢"
            color="green"
        else:
            result = "🔥"
            color="red"
        fig.add_annotation(
            x=min_iv_strike,
            y=min_iv_value,
            xref="x",
            yref="y",
            text=f"Price: ${underlying_price:.2f} vs. Low IV: ${min_iv_strike} == {result}",
            showarrow=True,
            arrowhead=4,
            ax=0,
            ay=-160,  # Adjust the y-offset to position it correctly
            font=dict(size=18, color=color),
            bordercolor="#c7c7c7",
            borderwidth=2,
            borderpad=1,
            bgcolor="black",
            opacity=0.8
        )
        fig.write_html('temp_plot_options.html')

        # Step 3: Convert HTML to Image with Selenium
        file_path = os.path.abspath('temp_plot_options.html')
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        browser = webdriver.Chrome(options=options)
        browser.get(f'file://{file_path}')
        screenshot_bytes = browser.get_screenshot_as_png()
        with open('screenshot2.png', 'wb') as f:
            f.write(screenshot_bytes)
        browser.quit()

        with open('screenshot2.png', "rb") as f:
            file = disnake.File(f, filename="screenshot2.png")  # Use file pointer directly

        embed = disnake.Embed().set_image(url="attachment://screenshot2.png")
        embed.add_field(name=f"Chart:", value=f"> **Viewing consolidated IV skew across all options for {ticker}. This is a visual representation of the consolidated skew across all expirations.")
        embed.set_footer(text=f'IV Skew - {ticker} | Data by Polygon.Io | Implemented by FUDSTOP', icon_url=os.environ.get('fudstop_logo'))
        await inter.edit_original_message(file=file, embed=embed, view = OptionsView(ticker).add_item(OptsSelect(ticker)))


    @options.sub_command()
    async def iv_surface(self, inter:disnake.AppCmdInter, ticker:str):
        """Plots the IV surface of a ticker across all expirations and strikes."""
        ticker = ticker.upper()
        await inter.response.defer()

        await plot_iv_surface(ticker)

        
        with open('iv_surface.png', "rb") as f:
            file = disnake.File(f, filename="iv_surface.png")  # Use file pointer directly

        embed = disnake.Embed().set_image(url="attachment://iv_surface.png")
        embed.add_field(name=f"Chart:", value=f"> **Viewing IV Surface 3D Plot for {ticker}. This is a visual representation of the volatility surface across all strikes and expirations - showing days until expiry vs strike + IV levels.")
        embed.set_footer(text=f'IV Surface - {ticker} | Data by Polygon.Io | Implemented by FUDSTOP', icon_url=os.environ.get('fudstop'))
        await inter.edit_original_message(file=file, embed=embed, view = OptionsView(ticker))




    @options.sub_command()
    async def greek_exposure(self, inter:disnake.AppCmdInter, ticker:str, greek:str=commands.Param(choices=['delta', 'gamma', 'theta', 'vega'])):
        """Charts the delta/gamma/vega/theta exposure across all strikes and expirations."""

        await inter.response.defer()
        ticker = ticker.upper()
        await plot_greek_exposure(ticker, greek)
        
        with open(f'files/{greek}.jpg', "rb") as f:
            file = disnake.File(f, filename=f"files/{greek}.jpg")  # Use file pointer directly

        embed = disnake.Embed().set_image(url=f"attachment://files/{greek}.jpg")
        embed.add_field(name=f"Chart:", value=f"> **Viewing consolidated vega across all strikes for {ticker}. This is a visual representation of the current levels of VEAG per strike on a consolidated basis across all expirations.")
        embed.set_footer(text=f'Consolidated {greek.upper()} - {ticker} | Data by Polygon.Io | Implemented by FUDSTOP', icon_url=os.environ.get('fudstop'))
        await inter.edit_original_message(file=file, embed=embed, view = OptionsView(ticker).add_item(OptsSelect(ticker)))

    
     
    @options.sub_command()
    async def filter(self, inter:disnake.AppCmdInter, filter:str):
        """Filter options!"""
        await inter.response.defer()
        ai = await self.ai.run_conversation(f"Hey, can you look at some options for me? I want: {filter}.")
        
        embed = disnake.Embed(title=f"Filter Options", description=f"```py\n{ai}```")


        await inter.edit_original_message(embed=embed)





opts = OptionsCOG(commands.Bot)

class GreekModal(disnake.ui.Modal):
    def __init__(self, greek):
        self.greek=  greek
        

        components = [

        disnake.ui.TextInput(
            label=f"Choose a ticker..",
            placeholder='e.g. AAPL',
            custom_id=f"greekmodal",
            style=TextInputStyle.short,
            max_length=6,
            required=True
        ),
        
        ]
            
        # Make sure to pass the components to the super().__init__
        super().__init__(title="Query Options Database", components=components)
        
    async def callback(self, inter: disnake.ModalInteraction):
        await inter.response.defer()
        user_input = inter.text_values
        ticker = user_input.get('greekmodal')

        await plot_greek_exposure(ticker, greek=self.greek)

        with open(f'files/{self.greek}.jpg', "rb") as f:
            file = disnake.File(f, filename=f"files/{self.greek}.jpg")  # Use file pointer directly

        embed = disnake.Embed().set_image(url=f"attachment://files/{self.greek}.jpg")
        embed.add_field(name=f"Chart:", value=f"> **Viewing consolidated vega across all strikes for {ticker}. This is a visual representation of the current levels of VEAG per strike on a consolidated basis across all expirations.")
        embed.set_footer(text=f'Consolidated {self.greek.upper()} - {ticker} | Data by Polygon.Io | Implemented by FUDSTOP', icon_url=os.environ.get('fudstop'))
        await inter.edit_original_message(file=file, embed=embed, view = OptionsView(ticker).add_item(OptsSelect(ticker)))

    









class OptsSelect(disnake.ui.Select):
    def __init__(self, ticker=None):
        self.ticker=ticker

        super().__init__( 
            placeholder='Choose a command -->',
            min_values=1,
            max_values=1,
            custom_id='optsCOGselect',
            options = [ 
                disnake.SelectOption(label='Analyze',value='9', description=f'Analyze the options market..'),
                disnake.SelectOption(label='Full Skew',value='0', description=f'View the skew across all expirations.'),
                disnake.SelectOption(label='Top Vol. Strikes',value='1', description=f'View top volume strikes across all expirations.'),
                disnake.SelectOption(label='Greek Exposure', value='3', description='View the consolidated greek exposure of choice.')
            ]
        )


    async def callback(self, inter:disnake.AppCmdInter):
        if self.values[0] == '0':
            await opts.full_skew(inter, self.ticker)

        elif self.values[0] == '1':
            await opts.top_vol_strikes(inter, self.ticker)
 
        elif self.values[0] == '2':
            await opts.lowest_theta(inter, self.ticker)

        elif self.values[0] == '3':
            await opts.gamma_exposure(inter, self.ticker)

        elif self.values[0] == '9':
            await inter.response.edit_message(view=OptionsView().add_item(OptsSelect(self.ticker)))

from disnake import TextInputStyle

# Subclassing the modal.
class MyModal(disnake.ui.Modal):
    def __init__(self):
        # The details of the modal, and its components
        components = [
            disnake.ui.TextInput(
                label="Ticker",
                placeholder="e.g. AAPL",
                custom_id="ticker",
                style=TextInputStyle.short,
                max_length=10,
            ),
            disnake.ui.TextInput(
                label="Strike",
                placeholder="e.g. 125",
                custom_id="strike",
                style=TextInputStyle.short,
                max_length=10
            ),
            disnake.ui.TextInput(
                label="Call or Put",
                placeholder="e.g. call",
                custom_id="call_put",
                style=TextInputStyle.short,
                max_length=4
            ),
            disnake.ui.TextInput(
                label="Expiration Date",
                placeholder="e.g. 231208",
                custom_id="expiry",
                style=TextInputStyle.short,
                max_length=6
            ),
        ]
        super().__init__(title="Select Your Option", components=components)

    # The callback received when the user input is completed.
    async def callback(self, inter: disnake.ModalInteraction):
   
        embed = disnake.Embed(title="Option Selector", description=f"```py\nThis is a test.```")
        expiry = inter.text_values.get('expiry')
        strike = inter.text_values.get('strike')
        call_put = inter.text_values.get('call_put')
        ticker = inter.text_values.get('ticker')
        await inter.send(embed=embed, ephemeral=True, view=OptionsView(ticker=ticker, strike=strike, call_put=call_put, expiry=expiry))


class OptionsView(disnake.ui.View):
    def __init__(self, greek:str='vega', ticker=None, strike=None, call_put=None, expiry=None):
        self.ticker=ticker
        self.strike=strike
        self.call_put=call_put
        self.greek=greek
        self.expiry=expiry
        super().__init__(timeout=None)
        # Add the button only if all the option arguments are passed in
        if all([self.ticker, self.strike, self.call_put, self.expiry]):
            self.add_item(disnake.ui.Button(style=disnake.ButtonStyle.blurple, label='OPTION LOADED', row=4, custom_id='optview2'))
            self.remove_item(self.optview1)

    @disnake.ui.button(style=disnake.ButtonStyle.blurple, label='Pick an Option', row=0, custom_id='optview1')
    async def optview1(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
  
        await inter.response.send_modal(MyModal())



    @disnake.ui.button(style=disnake.ButtonStyle.red, row=0, custom_id='theta', emoji='⏳', label='Theta')
    async def theta(self, button: disnake.ui.Button, inter: disnake.AppCmdInter):

        await inter.response.send_modal(GreekModal(greek=self.greek))


    @disnake.ui.button(style=disnake.ButtonStyle.blurple, row=0, custom_id='gamma', emoji='🎯', label='Gamma')
    async def gamma(self, button: disnake.ui.Button, inter: disnake.AppCmdInter):

        await inter.response.send_modal(GreekModal(greek=self.greek))



    @disnake.ui.button(style=disnake.ButtonStyle.grey, row=1, custom_id='delta', emoji='⚔️', label='Delta')
    async def delta(self, button: disnake.ui.Button, inter: disnake.AppCmdInter):

        await inter.response.send_modal(GreekModal(greek=self.greek))



    @disnake.ui.button(style=disnake.ButtonStyle.green, row=1, custom_id='vega', emoji='✨', label='Vega')
    async def vega(self, button: disnake.ui.Button, inter: disnake.AppCmdInter):

        await inter.response.send_modal(GreekModal(greek=self.greek))





        




def setup(bot: commands.Bot):
    bot.add_cog(OptionsCOG(bot))
    print(f'Options command - Ready')