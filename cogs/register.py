import disnake

from disnake.ext import commands

from discord_.bot_menus.pagination import AlertMenus
from apis.federal_register.fed_register_sdk import FedRegisterSDK
from apis.helpers import remove_html_tags
import pandas as pd

fedreg = FedRegisterSDK()


class FedRegisterCog(commands.Cog):
    """
    Fetch documents from the federal register!
    
    """


    def __init__(self, bot):
        self.bot=bot




    @commands.slash_command()
    async def register(self, inter):
        pass



    @register.sub_command()
    async def query_document(self, inter:disnake.AppCmdInter, query:str, effective_date_greater_than:str='2019-09-17', effective_date_less_than:str=None, order=commands.Param(choices=['newest','oldest','relevance']), is_significant=commands.Param(choices=['0', '1'])):
        """Provide input text to search for documents! Also can use optional data parameters!"""
        try:

            if effective_date_less_than is None:
                effective_date_less_than = fedreg.today


            await inter.response.defer(ephemeral=False)


            await fedreg.query_document(query,effective_date_greater_than=effective_date_greater_than, effective_date_less_than=effective_date_less_than,order=order, is_significant=is_significant)
            filename='data/fed_register/query_results.csv'
            df = pd.read_csv(filename)

            embeds = []
            for i,row in df.iterrows():
                result_count = row['result_count']
                search_description	= row['search_description']
                total_pages	= row['total_pages']
                title = row['title']
                type = row['type']
                abstract = row['abstract']
                document_number	= row['document_number']
                html_url = row['html_url']
                pdf_url	= row['pdf_url']
                public_inspection_url = row['public_inspection_url']
                publication_date = row['publication_date']
                excerpts = remove_html_tags(row['excerpts'])
                agency = row['agency']

                embed = disnake.Embed(title=f'Federal Register - {query}', description=f"```py\nDocument: {title}\nFROM: **{agency}**\n\nExcerpt:\n\n{excerpts}```", color=disnake.Colour.dark_gold(), url=html_url)
                embed.add_field(name=f"Doc Info:", value=f"> Result Count: **{result_count}**\n> Total Pages: **{total_pages}**\n> Type: **{type}**\n> Pub. Date: **{publication_date}**", inline=False)
                embed.add_field(name=f"Links:", value=f"> HTML: {html_url}\n\n> PDF: **{pdf_url}**\n\n> Pub. Inspection: **{public_inspection_url}**", inline=False)
                embed.add_field(name=f"Abstract:", value=f"> **{abstract[:1010]}**")
                embed.add_field(name="Search Description", value=f"> **{search_description}**")
                embed.set_footer(text=f'Doc #: {document_number} | Implemented by FUDSTOP')

                embeds.append(embed)

            
    




        
                await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds), file=disnake.File('data/fed_register/query_results.csv'))
        except Exception as e:
            await inter.send(f"# > {e}")
    


def setup(bot: commands.Bot):
    bot.add_cog(FedRegisterCog(bot))

    print(f'Federal Register Ready!')