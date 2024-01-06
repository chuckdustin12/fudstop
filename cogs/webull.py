import disnake

from disnake.ext import commands
from apis.webull.webull_trading import WebullTrading




class WebullCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
        self.wt = WebullTrading()



    @commands.slash_command()
    async def webull(self, inter):
        pass




    @webull.sub_command()
    async def multi_quote(self, inter:disnake.AppCmdInter, tickers:str):
        """Stream quotes for a comma separated list of tickers .. e.g. MSFT,AAPL,TSLA.."""
        await inter.response.defer()
        all_quotes = []
        async for quotes in self.wt.multi_quote(tickers):

            # Split the quotes string by '|', assuming each piece of information is separated by '|'
            quote_details = quotes.split(' | ')

            # Join the split quote details with a newline character
            formatted_quote = '\n'.join(quote_details)

            # Append the formatted quote to the all_quotes list
            all_quotes.append(formatted_quote)

        # Join all quotes with two newlines for clear separation between each ticker's quotes
        final_output = "\n\n".join(all_quotes)
        await inter.edit_original_message(f"# > {final_output}")



def setup(bot:commands.Bot):
    bot.add_cog(WebullCOG(bot))
    print('Webull ready!')
