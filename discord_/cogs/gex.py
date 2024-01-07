import disnake

from disnake.ext import commands
from apis.gexbot.gexbot import GexMajorLevels,GEXBot,Gex,MaxGex
from _markets.list_sets.ticker_lists import gex_tickers
import asyncio



# At the top of the file.
import disnake
from disnake.ext import commands





class GEXCog(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
        self.tickers = gex_tickers
        self.gexbot = GEXBot()




    @commands.slash_command()
    async def gex(self, inter):
        pass




    @gex.sub_command()
    async def get_gex(self, inter:disnake.AppCmdInter, ticker:str=commands.Param(choices=gex_tickers)):
        """Gets GEX data for a ticker - Work in progress"""
        ticker = ticker.upper()
        await inter.response.defer()
        while True:

            gex = await self.gexbot.get_gex(ticker)


            await inter.edit_original_message(f"> **{gex}**")

            await asyncio.sleep(5)



    @gex.sub_command()
    async def gex_major_levels(self, inter:disnake.AppCmdInter, ticker:str=commands.Param(choices=gex_tickers)):
        """Returns major GEX Levels"""
        ticker = ticker.upper()
        await inter.response.defer()
        counter = 0
        while True:
            counter = counter + 1
            data = await self.gexbot.major_levels(ticker)
            embed = disnake.Embed(title=f"Gex Major Levels - {ticker}", description=f"```py\n{data.as_dataframe}```", color=disnake.Colour.dark_gold())
        
            embed.set_footer(text='Provided by KRAKENSLAYER')


            await inter.edit_original_message(embed=embed)

            if counter == 250:
                await inter.edit_original_message(f"> Stream ended. Use /gex_major_levels to run again.")

    @gex.sub_command()
    async def gex_spy_spx(self, inter:disnake.AppCmdInter):

        """Comparitive GEX between SPY/SPX"""
    
        await inter.response.defer()
        counter = 0
        while True:
            counter = counter + 1
            data = await self.gexbot.major_levels('SPX')
            data2 = await self.gexbot.major_levels('SPY')
            embed = disnake.Embed(title=f"Gex Major Levels - SPY/SPX", color=disnake.Colour.dark_gold())
            
            embed.add_field(name=f"Zero Gamma:", value=f"> SPY: **{round(float(data2.zero_gamma),4)}**\n> SPX: **{round(float(data.zero_gamma),2)}**")
            embed.add_field(name=f"Major OI:", value=f"> SPY POS: **{data2.mpos_oi}**\n> SPY NEG: **{data2.mneg_oi}**\n\n> SPX POS: **{data.mpos_oi}**\n> SPX NEG: **{data.mneg_oi}**")
            embed.add_field(name=f"Major Vol:", value=f"> SPY POS: **{data2.mpos_vol}**\n> SPY NEG: **{data2.mneg_vol}**\n\n> SPX POS: **{data.mpos_vol}**\n> SPX NEG: **{data.mneg_vol}**")
            embed.add_field(name=f"Prices:", value=f"> SPY: **${data2.spot}**\n> SPX: **${data.spot}**")
            embed.set_footer(text='Provided by KRAKENSLAYER')


            await inter.edit_original_message(embed=embed)

            if counter == 250:
                await inter.edit_original_message(f"> Stream ended. Use /gex_spy_spx to run again.")
                break

    async def run_gex(self):
    
        tasks = [self.gex(i) for i in gex_tickers]
        await asyncio.gather(*tasks)


gex = GEXCog(commands.Bot)




class GexSelect(disnake.ui.Select):
    def __init__(self, ticker=None):
        self.ticker=ticker
        super().__init__( 
            placeholder='Select A Function -->',
            min_values=1,
            max_values=1,
            custom_id='gexSelect',
            options= [ 
                disnake.SelectOption(label='Get Gex', value='0', description=f'Get GEX data for a ticker.'),
                disnake.SelectOption(label='GEX Major Levels', value='1', description=f'Get major GEX levels for a ticker.'),
                disnake.SelectOption(label='SPY // SPX', value='2', description='Get comparitive GEX between SPY & SPX')
            ]
        )


    async def callback(self, inter:disnake.AppCmdInter):
        if self.values[0] == '0':
            await gex.get_gex(inter, self.ticker)

        elif self.values[0] == '1':
            await gex.gex_major_levels(inter, self.ticker)


        elif self.values[0] == '2':
            await gex.gex_spy_spx(inter)


def setup(bot: commands.Bot):
    bot.add_cog(GEXCog(bot))
    print(f"Gex commands - READY!")