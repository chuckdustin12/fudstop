import disnake
from disnake.ext import commands
import pandas as pd
from apis.polygonio.polygon_options import PolygonOptions
from tabulate import tabulate
opts = PolygonOptions(user='postgres', database='fudstop')


class SPXCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot


    @commands.slash_command()
    async def spx(self, inter):
        pass




    @spx.sub_command()
    async def monitor_change_percent(self, inter:disnake.AppCmdInter):
        await inter.response.defer()
        counter = 0
        while True:
            counter = counter + 1

            fetched_price = await opts.get_price(ticker='SPX')
            lower_price = float(fetched_price) - 15
            upper_price = float(fetched_price) + 15

            # Fetch options
            options = await opts.get_option_chain_all(
                underlying_asset='I:SPX', 
                strike_price_gte=lower_price, 
                strike_price_lte=upper_price, 
                expiration_date='2024-01-03'
            )


            iv = [round(vol * 100, 2) for vol in options.implied_volatility]
            # Your options data
            cr = options.change_percent
            cp = options.contract_type
            ticker = options.underlying_ticker
            price = options.underlying_price
            theta = [round(option, 2) for option in options.theta]
            delta = [round(option, 2) for option in options.delta]
            gamma = [round(option, 2) for option in options.gamma]
            vol = options.volume
            strike = options.strike

            # Initialize dictionaries for calls and puts
            calls_data = {'strike': [], 'iv': [], 'change%': [], 'theta': [], 'vol': [],}
            puts_data = {'strike': [], 'iv': [], 'change%': [], 'theta': [], 'vol': []}

            # Separate out calls and puts
            for i, contract_type in enumerate(cp):
                if contract_type.lower() == 'call':
                    calls_data['strike'].append(strike[i])
                    calls_data['iv'].append(iv[i])
                    calls_data['change%'].append(cr[i])
                    calls_data['theta'].append(theta[i])
                    calls_data['vol'].append(vol[i])
                    
                elif contract_type.lower() == 'put':
                    puts_data['strike'].append(strike[i])
                    puts_data['iv'].append(iv[i])
                    puts_data['change%'].append(cr[i])
                    puts_data['theta'].append(theta[i])
                    puts_data['vol'].append(vol[i])
                    

            # Convert to DataFrames if needed
            calls_df = pd.DataFrame(calls_data)
            puts_df = pd.DataFrame(puts_data)


            # Get the new price from a separate function

            # Determine the length of the other columns


            calls_table = tabulate(calls_df, headers='keys', tablefmt='fancy', showindex=False)
            puts_table = tabulate(puts_df, headers='keys', tablefmt='fancy', showindex=False)

        
            color = disnake.Colour.dark_red()

            embed = disnake.Embed(title=f"SPX Change % Monitor - | 0DTE", description=f"```py\n${fetched_price}```", color=color)
            embed.add_field(name=f"# > CALLS:", value=f"```py\n{calls_table}```", inline=False)
            embed.add_field(name=f"_ _ _ _", value=f"# > SPX: **${fetched_price}**", inline=False)
            embed.add_field(name="# > PUTS:", value=f"```py\n{puts_table}```", inline=False)
            await inter.edit_original_message(embed=embed)

            if counter == 150:
                break

def setup(bot: commands.Bot):
    bot.add_cog(SPXCOG(bot))