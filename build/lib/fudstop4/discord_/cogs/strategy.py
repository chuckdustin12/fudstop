import disnake
from disnake.ext import commands
import pandas as pd
from tabulate import tabulate
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from discord_.bot_menus.pagination import AlertMenus
import os

from dotenv import load_dotenv
load_dotenv()
theta_em = "<a:_:1047010360554704977>"
gamma_em = "<a:_:1190024505700134973>"
delta_em = "<a:_:1044647851466182736>"
vega_em = "<a:_:1044647903009976372>"
rho_em = "<a:_:1044647745702596648>"
ticker_em = "<:_:1190025407815221270>"
volume_em = "🔊"
oi_em = "👥"
oi_change_em = "🔀"
iv_em="💉"
expiry_em="🗓️"
strike_em="🎳"
call_put_em="↕️"
change_ratio_em="➗"
start_over_em="<:_:1190026332248232126>"
open_em="<a:_:1044647404693106728>"
high_em="<a:_:1047010108930019340>"
low_em="<a:_:1044658254137008148>"
close_em="<a:_:1044647558133334126>"
underlying_price_em="💲"
sdk = WebullOptions(database='fudstop3', user='chuck')
from disnake import TextInputStyle
class MyModal(disnake.ui.Modal):
    def __init__(self, label, placeholder, name, conn, query, strat=None):
        self.strat=strat
        self.label=label
        self.placeholder=placeholder
        self.name=name
        self.query=query
        self.conn=conn
        # The details of the modal, and its components
        components = [

            disnake.ui.TextInput(
                label='Operator',
                placeholder='Choose between >=, <=, >, <, or =',
                custom_id='operator',
                style=TextInputStyle.short,
                max_length=2

            ),
            disnake.ui.TextInput(
                label=self.label,
                placeholder=self.placeholder,
                custom_id=self.name,
                style=TextInputStyle.short,
                
            ),
        
        ]
        super().__init__(title="Create Tag", components=components)

    # The callback received when the user input is completed.
    async def callback(self, inter: disnake.ModalInteraction):
        user_input = inter.text_values

        value = user_input.get(self.name)
        operator = user_input.get('operator')

        select = StrategySelect()
        embeds = await select.get_results(self.conn, self.query, self.strat)

        await inter.edit_original_message(embeds=embeds[0], view=AlertMenus(embeds).add_item(StrategyView()))

class ExpiryButton(disnake.ui.Button):
    def __init__(self, query):
        self.query=query
     

        super().__init__( 
            style=disnake.ButtonStyle.blurple,
            label='Expiry',
            emoji=expiry_em,
            custom_id='expiry_button'
        )


    async def callback(self, inter:disnake.AppCmdInter):
        name = 'expire_date'
        await inter.response.send_modal(MyModal(label='Expiration Date', placeholder='e.g. 2024-03-15', name='expire_date', conn=await sdk.db_manager.get_connection(), query=self.query))


        

        

class StrategyView(disnake.ui.View):
    def __init__(self):
        super().__init__(timeout=None)


        self.add_item(StrategySelect())



class StrategySelect(disnake.ui.Select):
    def __init__(self):
        super().__init__( 
            custom_id='strategyselect',
            min_values=1,
            max_values=1,
            options=[ 
                disnake.SelectOption(label='Easily Managed', description=f'Low theta with time on the clock. Easily managed.'),
                disnake.SelectOption(label='Change Percent', description=f'Options that are down significantly with time left.'),
                disnake.SelectOption(label='Long Puts', description=f'Put options gain as stocks fall, with low time decay.'),
                disnake.SelectOption(label='OTM Puts', description=f'Put options slightly out of the money with moderate delta.')
            ]
        )


    def chunk_string(self, string, size):
        """Yield successive size-sized chunks from string."""
        for i in range(0, len(string), size):
            yield string[i:i + size]


    async def get_results(self, conn, query, strat):
        results = await conn.fetch(query)
        df = pd.DataFrame(results, columns = ['underlying_symbol', 'strike_price', 'call_put', 'expire_date', 'price'])
        print(df.columns)
        # Select only the specified columns
  
        df = df.rename(columns={'underlying_symbol': 'sym', 'strike_price':'strike', 'call_put': 'cp', 'expire_date': 'exp'})
        table = tabulate(df, headers=['sym', 'strike', 'cp', 'expiry', 'price'], tablefmt='fancy', showindex=False)
        # Break apart data into chunks of 4000 characters
        chunks = self.chunk_string(table, 4000)
        embeds=[]
        # Create and send embeds for each chunk
        # Strategy descriptions
        strategy_dict = {
            # Existing strategies
            'Easily Managed': 'Options with low theta for minimal time decay, ideal for stable strategies.  This query sifts through the options database to uncover potential trading opportunities with a short to medium-short time horizon. Its tailored for traders who prefer to engage in strategies that have time on the clock - with low theta decay - allowing for easy managability.',
            'Long Calls': 'Options with high delta, signaling strong potential price increase with the underlying stock.',
            'Long Puts': 'Put options that gain value as the underlying stock price falls, with low time decay.',
            # Add the new strategy
            'Moderate Out-of-the-Money Puts': 'Put options slightly out of the money with moderate delta, balancing risk and potential inverse movement to the stock.'
            # Add more strategies and their descriptions here
        }

        # [Earlier code]
        for chunk in chunks:
            embed = disnake.Embed(title=f"Strategy Menu | {strat}", description=f"```py\n{chunk}```")
            # Dynamically adding strategy information from the dictionary
            strategy_info = strategy_dict.get(strat, "No information available")
            embed.add_field(name="Strategy Info:", value=f"```py\n{strategy_info}```")
            embed.set_footer(text='Implemented by FUDSTOP')
            embeds.append(embed)
        return embeds


    async def callback(self, inter:disnake.AppCmdInter):
        conn = await sdk.db_manager.get_connection()
        await inter.response.defer()
        if self.values[0] == 'Easily Managed':
            query = f"""SELECT underlying_symbol, strike_price, expire_date, call_put, close 
                        FROM options 
                        WHERE expire_date >= '2024-01-01' AND expire_date <= '2025-12-31'
                        AND close >= 0 and theta <= -0.03;
                        """
            embeds = await self.get_results(conn,query, self.values[0])
            await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(StrategySelect()))


            return query

        if self.values[0] == 'Long Puts':
            query = """SELECT underlying_symbol, strike_price, expire_date, call_put, delta, theta 
                    FROM options 
                    WHERE call_put = 'put' AND delta <= -0.7 AND theta <= 0.02 
                    AND expire_date >= '2023-03-15';"""
            embeds = await self.get_results(conn,query, self.values[0])
            await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(StrategySelect()))


            return query
        

        if self.values[0] == 'High Delta / Low Theta':
            query = """SELECT underlying_symbol, strike_price, expire_date, call_put, delta, theta
           FROM options
           WHERE delta >= 0.7 AND theta <= 0.01 AND expire_date >= CURRENT_DATE + INTERVAL '5 months'
           ORDER BY delta DESC, theta ASC LIMIT 25;"""
            
            embeds = await self.get_results(conn,query, self.values[0])
            await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(StrategySelect()))


            return query
        
        if self.values[0] == 'OTM Puts':
            query = f"""SELECT underlying_symbol, strike_price, expire_date, call_put, delta
                        FROM options
                        WHERE call_put = 'put' AND delta <= -0.05
                        AND strike_price >= close + 5
                        LIMIT 25;
                        """
            
            embeds = await self.get_results(conn,query, self.values[0])
            await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(StrategySelect()))


            return query


class StrategyCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
        

    @commands.slash_command()
    async def strategy(self, inter):
        pass


    @strategy.sub_command()
    async def menu(self, inter:disnake.AppCmdInter):
        await inter.response.defer()

        await inter.edit_original_message(view=StrategyView())



def setup(bot:commands.Bot):
    bot.add_cog(StrategyCOG(bot))
    print('Strategy! Ready!')