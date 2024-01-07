import disnake
from disnake.ext import commands

# cogs/main_view.py
from .earnings import EarningsSelect
from .gex import GexSelect
from .options import OptsSelect
import asyncio

class MainView(disnake.ui.View):
    def __init__(self, bot = None, ticker=None):
        self.ticker=ticker
        self.bot=bot
        super().__init__(timeout=None)
        
        # Initialize other views as needed


    @disnake.ui.button(style=disnake.ButtonStyle.blurple, label='GEX', row=0, custom_id='gexcmds')
    async def gex(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        self.clear_items()
        self.add_item(GexSelect())

        

    @disnake.ui.button(style=disnake.ButtonStyle.blurple, label='EARNINGS', row=0, custom_id='earnings')
    async def earnings(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        self.clear_items()
        self.add_item(EarningsSelect())




        # Clear the existing items and add a new OptsSelect with the ticker
        self.clear_items()
        self.add_item(OptsSelect(self.ticker))




    @disnake.ui.button(style=disnake.ButtonStyle.blurple, label='OPTIONS',row=0, custom_id='options')
    async def opts(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        self.clear_items()
        await inter.response.defer()
        # Check if ticker is None and prompt the user
        if self.ticker is None:
            await inter.send("Please enter the ticker:")

            # Wait for a message from the user; you can add checks to validate the response
            try:
                message = await self.bot.wait_for(
                    'message',
                    check=lambda m: m.author == inter.author and m.channel == inter.channel,
                    timeout=30.0  # Timeout after 30 seconds
                )
            except asyncio.TimeoutError:
                await inter.followup.send("You didn't reply in time!")
                return

            # Set the ticker with the content of the message
            self.ticker = message.content


            self.add_item(OptsSelect(ticker=self.ticker))

            await inter.send(view=self)


def setup(bot: commands.Bot):
    bot.add_cog(MainView(bot))
    print('MainView')