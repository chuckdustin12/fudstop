from .accounting import AccountingSelect
from .gex import Gex, GexSelect
from .yfin import yfCOG, YFSelect
from .earnings import EarningsSelect



import disnake
from disnake import TextInputStyle
from disnake.ext import commands


class MainView(disnake.ui.View):
    def __init__(self, ticker=None):
        self.ticker=ticker
        super().__init__(timeout=None)


    @disnake.ui.button(style=disnake.ButtonStyle.blurple, label='-', custom_id='-1', row=0, disabled=True)
    async def one(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        pass

    @disnake.ui.button(style=disnake.ButtonStyle.grey, label='-', custom_id='-2', row=0, disabled=True)
    async def two(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        pass

    @disnake.ui.button(style=disnake.ButtonStyle.blurple, label='-', custom_id='-3', row=0, disabled=True)
    async def three(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        pass

    @disnake.ui.button(style=disnake.ButtonStyle.grey, label='-', custom_id='-4', row=0, disabled=True)
    async def four(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        pass

    @disnake.ui.button(style=disnake.ButtonStyle.blurple, label='-', custom_id='-5', row=0, disabled=True)
    async def five(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        pass

    @disnake.ui.button(style=disnake.ButtonStyle.green, label='GEX', custom_id='gex', row=1)
    async def gex(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        await inter.response.edit_message(view=GexView(self.ticker))


    @disnake.ui.button(style=disnake.ButtonStyle.green, label='INFO', custom_id='info', row=1)
    async def info(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        await inter.response.edit_message(view=YFView(self.ticker))

    @disnake.ui.button(style=disnake.ButtonStyle.green, label='REG', custom_id='reg', row=1)
    async def reg(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        pass


    @disnake.ui.button(style=disnake.ButtonStyle.green, label='FS', custom_id='fiscal', row=1)
    async def fs(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        pass

    @disnake.ui.button(style=disnake.ButtonStyle.green, label='ACC', custom_id='accounting', row=1)
    async def acc(self, button:disnake.ui.Button, inter:disnake.AppCmdInter):
        await inter.response.edit_message(view=AccountingView())
        


class GexView(disnake.ui.View):
    def __init__(self, ticker=None):
        self.ticker=ticker
        super().__init__(timeout=None)


        self.add_item(GexSelect(self.ticker))


    @disnake.ui.button(style=disnake.ButtonStyle.blurple, emoji='♻️', custom_id='toMainView', row=3)
    async def recycler(self, button: disnake.ui.Button, inter:disnake.AppCmdInter):
        await inter.response.edit_message(view=MainView(self.ticker))




class AccountingView(disnake.ui.View):
    def __init__(self):
     
        super().__init__(timeout=None)


        self.add_item(AccountingSelect())


    @disnake.ui.button(style=disnake.ButtonStyle.blurple, emoji='♻️', custom_id='toMainView', row=3)
    async def recycler(self, button: disnake.ui.Button, inter:disnake.AppCmdInter):
        await inter.response.edit_message(view=MainView())


class EarningsView(disnake.ui.View):
    def __init__(self):
     
        super().__init__(timeout=None)


        self.add_item(EarningsSelect())


    @disnake.ui.button(style=disnake.ButtonStyle.blurple, emoji='♻️', custom_id='toMainView', row=3)
    async def recycler(self, button: disnake.ui.Button, inter:disnake.AppCmdInter):
        await inter.response.edit_message(view=MainView())




class YFView(disnake.ui.View):
    def __init__(self, ticker=None):
        self.ticker=ticker
        super().__init__(timeout=None)

        self.add_item(YFSelect(ticker=self.ticker))

    @disnake.ui.button(style=disnake.ButtonStyle.blurple, emoji='♻️', custom_id='toMainView', row=3)
    async def recycler(self, button: disnake.ui.Button, inter:disnake.AppCmdInter):
        await inter.response.edit_message(view=MainView(self.ticker))






