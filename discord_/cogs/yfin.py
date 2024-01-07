import os
from dotenv import load_dotenv
load_dotenv()
import disnake
from disnake.ext import commands
from apis.y_finance.yf_sdk import yfSDK



class yfCOG(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
        self.sdk = yfSDK()



    @commands.slash_command()
    async def info(self, inter):
        pass


    @info.sub_command()
    async def mf(self, inter: disnake.AppCmdInter, ticker: str):
        """Gets mutual fund holdings for a ticker - top 10"""
        await inter.response.defer()
        try:
            data = self.sdk.mutual_fund_holders(ticker)
            
            filename = f'data/yf_{ticker}_mf_holders.csv'
            data.to_csv(filename)
            
            embed = disnake.Embed(
                title=f"Mutual Fund Holders - {ticker}", 
                description=f"Your Download is Ready!", 
                color=disnake.Colour.dark_teal()
            )
            embed.set_footer(text=f'Implemented by FUDSTOP')
            
            await inter.edit_original_message(embed=embed)
            await inter.send(file=disnake.File(filename), view=YFView(ticker))
        except Exception as e:
            await inter.edit_original_message(f"An error occurred: {e}")


    @info.sub_command()
    async def balance(self, inter:disnake.AppCmdInter, ticker: str):
        """Gets balance sheet information for a ticker"""
        await inter.response.defer()
        try:
            data = self.sdk.balance_sheet(ticker)
            
            filename = f'data/yf_balance_sheet.csv'
            data.to_csv(filename)
            
            await inter.edit_original_message(f"Balance Sheet for {ticker}:")
            await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))
        except Exception as e:
            await inter.edit_original_message(f"An error occurred: {e}")

    @info.sub_command()
    async def cashflow(self, inter:disnake.AppCmdInter, ticker: str):
        """Gets cash flow information for a ticker"""
        await inter.response.defer()
        try:
            data = self.sdk.get_cash_flow(ticker)
            
            filename = f'data/yf_cash_flow.csv'
            data.to_csv(filename)
            
            await inter.edit_original_message(f"Cash Flow for {ticker}:")
            await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))
        except Exception as e:
            await inter.edit_original_message(f"An error occurred: {e}")

    @info.sub_command()
    async def financials(self, inter:disnake.AppCmdInter, ticker: str):
        """Gets all financials for a ticker"""
        await inter.response.defer()
        try:
            data = self.sdk.financials(ticker)
            
            filename = f'data/yf_financials.csv'
            data.to_csv(filename)
            
            await inter.edit_original_message(f"Financials for {ticker}:")
            await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))
        except Exception as e:
            await inter.edit_original_message(f"An error occurred: {e}")


    # Command for the income_statement method within yfSDK
    @info.sub_command()
    async def statement(self, inter:disnake.AppCmdInter, ticker: str, frequency: str = 'quarterly', pretty: bool = False, as_dict: bool = False):
        """Gets the income statement for a ticker"""
        await inter.response.defer()
        data = self.sdk.income_statement(ticker, frequency=frequency, pretty=pretty, as_dict=as_dict)
        filename = f'data/yf_income_statement.csv'
        data.to_csv(filename)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))

    # Command for the get_info method within yfSDK
    @info.sub_command()
    async def infos(self, inter:disnake.AppCmdInter, ticker: str):
        """Returns a large dictionary of information for a ticker"""
        await inter.response.defer()
        data = self.sdk.get_info(ticker)
        filename = f'data/yf_info.csv'
        data.to_csv(filename)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))

    # Command for the institutional_holdings method within yfSDK
    @info.sub_command()
    async def whales(self, inter:disnake.AppCmdInter, ticker: str):
    
        """Gets institutional holdings for a ticker"""
        await inter.response.defer()
        data = self.sdk.institutional_holdings(ticker)
        filename = f'data/yf_institutional_holdings.csv'
        data.to_csv(filename)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))


    @info.sub_command()
    async def div(self, inter:disnake.AppCmdInter, ticker: str):
        """Gets dividends for a ticker - if any."""
        await inter.response.defer()
        data = self.sdk.dividends(ticker)
        filename = f'data/dividends.csv'
        data.to_csv(filename)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))


    @info.sub_command()
    async def allinfo(self, inter:disnake.AppCmdInter, ticker: str):
        """Gets all relevant company data for a ticker."""
        await inter.response.defer()
        data = self.sdk.fast_info(ticker)
        filename = f'data/fast_info.csv'
        data.to_csv(filename)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))


    @info.sub_command()
    async def candles(self, inter:disnake.AppCmdInter, *, ticker: str):
        """Gets all candlestick data for a ticker"""
        await inter.response.defer()
        data = self.sdk.get_all_candles(ticker)
        filename = f'data/all_candles.csv'
        data.to_csv(filename)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))


    # Command for the news method within yfSDK
    @info.sub_command()
    async def news(self, inter:disnake.AppCmdInter, ticker: str):
        """Gets ticker news"""
        await inter.response.defer()
        data = self.sdk.news(ticker)
        filename = f'data/yf_news.csv'
        data.to_csv(filename)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))

    # Command for the atm_calls method within yfSDK
    @info.sub_command()
    async def calls(self, inter:disnake.AppCmdInter, ticker: str):
        """Gets at the money calls for a ticker"""
        await inter.response.defer()
        data = self.sdk.atm_calls(ticker)
        data.to_csv('data/atm_calls.csv')
        filename = f'data/atm_calls.csv'
        data.to_csv(filename, index=False)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))

    # Command for the atm_calls method within yfSDK
    @info.sub_command()
    async def puts(self, inter:disnake.AppCmdInter, ticker: str):
        """Gets at the money puts for a ticker"""
        await inter.response.defer()
        data = self.sdk.atm_puts(ticker)
        data.to_csv('data/atm_puts.csv')
        filename = f'data/atm_puts.csv'
        data.to_csv(filename, index=False)
        await inter.edit_original_message(file=disnake.File(filename), view=YFView(ticker))

yf = yfCOG(commands.Bot)

class YFView(disnake.ui.View):
    def __init__(self, ticker=None):
        self.ticker=ticker
        super().__init__(timeout=None)

        self.add_item(YFSelect(ticker=self.ticker))



class YFSelect(disnake.ui.Select):
    def __init__(self, ticker=None):
        self.ticker=ticker

        super().__init__(placeholder='Select A Command -->',
                         min_values=1,
                         max_values=1,
                         custom_id='YFSELECT',
                         options = [ 
                             disnake.SelectOption(label='Mutual Fund Holders',value='0',description='View top mutual fund holders for a ticker'),
                             disnake.SelectOption(label='Balance Sheet',value='1',description='Get balance sheet info for a ticker.'),
                             disnake.SelectOption(label='Cash Flow',value='2',description='Get cash flow info for a ticker.'),
                            disnake.SelectOption(label='Income Statement',value='3',description='Get the income statement for a ticker.'),
                             disnake.SelectOption(label='All Financials',value='4',description='Get all company financials for a ticker.'),
                            
                             disnake.SelectOption(label='Ticker Info',value='5',description='Get a vast amount of company info for a ticker.'),
                             disnake.SelectOption(label='Institutions',value='6',description='Get top institutional holdings for a ticker.'),
                             disnake.SelectOption(label='Dividends',value='7',description='Get dividend information for a ticker.'),
                             disnake.SelectOption(label='All Info',value='8',description='Get all information for a ticker.'),
                             disnake.SelectOption(label='News', value='9', description=f'Get recent news for a ticker.'),
                             disnake.SelectOption(label='ATM Calls', value='10', description=f'Get at the money calls for a ticker.'),
                             disnake.SelectOption(label='ATM Puts', value='11', description='Get at the money puts for a ticker.'),
                             disnake.SelectOption(label='All Candles', value='12', description=f"Get all candlestick data for a ticker.")
                         ])
        
    async def callback(self, inter: disnake.AppCmdInter):
        if self.values[0] == '0':
            await yf.mf(inter,self.ticker)
        elif self.values[0] == '1':
            await yf.balance(inter, self.ticker)

        elif self.values[0] == '2':
            await yf.cashflow(inter, self.ticker)
        elif self.values[0] == '3':
            await yf.statement(inter, self.ticker)
        elif self.values[0] == '4':
            await yf.financials(inter, self.ticker)
        elif self.values[0] == '5':
            await yf.infos(inter, self.ticker)
        elif self.values[0] == '6':
            await yf.whales(inter, self.ticker)
        elif self.values[0] == '7':
            await yf.div(inter, self.ticker)
        elif self.values[0] == '8':
            await yf.allinfo(inter, self.ticker)
        elif self.values[0] == '9':
            await yf.news(inter,self.ticker)
        elif self.values[0] == '10':
            await yf.calls(inter,self.ticker)
        elif self.values[0] == '11':
            await yf.puts(inter,self.ticker)
        elif self.values[0] == '12':
            await yf.candles(inter,self.ticker)



def setup(bot: commands.Bot):
    bot.add_cog(yfCOG(bot))


    print(f'YF COG - READY')
