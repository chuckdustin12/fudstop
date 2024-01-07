
from disnake.ext import commands
import disnake
from apis.fed_print.fedprint_sdk import FedPrint
import pandas as pd

fedprint = FedPrint()


class FedPrintCog(commands.Cog):
    """
    Discord commands related to fed-print API
    """

    def __init__(self, bot):
        self.bot=bot



    @commands.slash_command()
    async def fedprint(self, inter):
        pass


    @fedprint.sub_command()
    async def item_search(self, inter:disnake.AppCmdInter, year:str=commands.Param(choices=['year:2023', 'year:2022', 'year:2021', 'year:2020', 'year:2019', 'year:2018', 'year:2017', 'year:2016']), limit:str='25'):
        """Search for Fed Print items based on year"""
        await inter.response.defer()

        items = await fedprint.search(filter=year, limit=limit)
        df = items.as_dataframe
        filename='fudstop/data/fed_print/search_results.csv'
        df.to_csv(filename, index=False)
        await inter.edit_original_message(file=disnake.File(filename))


    @fedprint.sub_command()
    async def get_all(self, inter:disnake.AppCmdInter):
        """Get an abstract for a Fed Document"""
        await inter.response.defer()
        all_dataframes = []
        ids = await fedprint.get_series_id()
        for id in ids:
            series_info = await fedprint.get_series(id)
            all_dataframes.append(series_info.as_dataframe)

        final_dataframe = pd.concat(all_dataframes, ignore_index=True)

        filename = 'fudstop/data/fed_print/all_data.csv'

        final_dataframe.to_csv(filename, index=False)

        await inter.edit_original_message(file=disnake.File(filename))


    @fedprint.sub_command()
    async def get_urls(self, inter:disnake.AppCmdInter, limit:str):
        """Returns URLs only from fedprint"""
        await inter.response.defer()


        urls = await fedprint.print_file_urls(limit=limit)
        await inter.edit_original_message(', '.join(urls))




def setup(bot: commands.Bot):
    bot.add_cog(FedPrintCog(bot))
    print(f'Fed Print API Commands - Loaded!')