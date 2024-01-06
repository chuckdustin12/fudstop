import os
from dotenv import load_dotenv
load_dotenv()
from disnake.ext import commands
import disnake
from apis.polygonio.async_polygon_sdk import Polygon
from scripts.rsi_2_4_8 import get_2_4_8_rsi, scan_2_4_8
from disnake import TextInputStyle
import pandas as pd


# Subclassing the modal.
class TaModal(disnake.ui.Modal):
    def __init__(self):
        # The details of the modal, and its components
        components = [
            disnake.ui.TextInput(
                label="Select Your Timespan",
                placeholder="minute,hour,day,week,month,quarter,year",
                custom_id="ta-timespan",
                style=TextInputStyle.short,
                max_length=10,
            ),
            disnake.ui.TextInput(
                label="Select Your Window",
                placeholder="e.g. 14",
                custom_id="ta-window",
                style=TextInputStyle.short,
            ),
            disnake.ui.TextInput(
                label="Select Your Tickers",
                placeholder="e.g. SPY,TSLA,QQQ,MSFT",
                custom_id="ta-tickers",
                style=TextInputStyle.short,
            ),
        ]
        super().__init__(title="Create Tag", components=components)

    # The callback received when the user input is completed.
    async def callback(self, inter: disnake.ModalInteraction):
        user_input = inter.text_values
        tamodal_ = user_input.get('tamodal')


class TACog(commands.Cog):
    def __init__(self, bot):
        self.bot=bot

        self.polygon = Polygon(os.environ.get('TECHNICALS_STRING'))


    @commands.slash_command()
    async def ta(self, inter):
        pass


    @ta.sub_command()
    async def get_all_rsi(self, inter:disnake.AppCmdInter):
        await inter.response.send_modal(TaModal())






    @ta.sub_command()
    async def rsi_2_4_8(self, inter:disnake.AppCmdInter):
        """Scans for daily 2/4/8 that meets criteria"""
        await inter.response.defer()

        results = await scan_2_4_8()



        df = pd.DataFrame(results)
        # Rename the column
        df.columns = ["RESULTS:"]
  
        embed = disnake.Embed(title=f"RSI 2/4/8", description=f"> *This scanner is checking the RSI indicator on the DAILY timeframe for the window-lengths of:    \
        2/4/8*\n\n> **{df}**", color=disnake.Colour.dark_blue())
        embed.add_field(name=f"RSI Conditions:", value=f"> Window 2: **<= 1 & >= 99** ✅\n> Window 4: **<= 10 & >= 90** ✅\n> Window 8: **<= 20 & >= 80 ✅**", inline=False)
   
        await inter.edit_original_message(embed=embed)





def setup(bot:commands.Bot):
    bot.add_cog(TACog(bot))
    print(f"Technicals.. READY!!")