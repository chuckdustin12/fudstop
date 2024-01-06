import disnake

from disnake.ext import commands

import requests
import pandas as pd
from apis.helpers import format_large_number
from discord_.bot_menus.pagination import AlertMenus, PageSelect
class IBRS(commands.Cog):

    def __init__(self, bot):
        self.bot=bot





    @commands.slash_command()
    async def ibrd(self, inter):
        pass





    @ibrd.sub_command()
    async def flows_commitments(self, inter:disnake.AppCmdInter, year:str='2023'):
        """loans and IDA Credits are made to, or guaranteed by, countries that are members of IBRD and IDA"""
        await inter.response.defer()
        r = requests.get(f"https://finances.worldbank.org/resource/7ipw-i7ht.json?fiscal_year={year}").json()

        df = pd.DataFrame(r)

        embeds=[]
        for i,row in df.iterrows():
            financier = row['financier']
            year = row['fiscal_year']
            region = row['region']
            country = row['country']
            gross_disbursement = format_large_number(row['gross_disbursement'])
            repayments = format_large_number(row['repayments'])
            net_disbursement = format_large_number(row['net_disbursement'])
            interest = format_large_number(row['interest'])
            fees = format_large_number(row['fees'])
            ibrd_commitments_us = format_large_number(row['ibrd_commitments_us'])
            ida_non_concessional = format_large_number(row['ida_non_concessional'])
            ida_concessional_commitments = format_large_number(row['ida_concessional_commitments'])
            ida_other_commitments_us = format_large_number(row['ida_other_commitments_us'])

            embed = disnake.Embed(title=f"IBRD and IDA Net Flows & Commitments", description=f"""```py\nThe International Bank for Reconstruction and Development (IBRD) loans and International Development Association (IDA) credits are public and publicly guaranteed debt extended by the World Bank Group. IBRD loans and IDA Credits are made to, or guaranteed by, countries that are members of IBRD and IDA.
IBRD lends at market rates.\n\nIDA provides development credits, grants, and guarantees to its recipient member countries at concessional rates. IDA also has Non-Concessional Lending which is priced at market rates (similar to IBRD). IBRD and IDA net flows and commitments dataset contains IBRD and IDA commitments, gross disbursements, repayments, net disbursements (disbursements net of repayments), Interest charges, and fees (commitment fee, front end fee, service charges, and guarantee fees) at a country level. Data are in U.S. dollars calculated using historical rates.```""", color=disnake.Colour.dark_green())
            embed.add_field(name=f"Financier:", value=f"> **{financier}**")
            embed.add_field(name=f"Year:", value=f"> **{year}**")
            embed.add_field(name=f"Region:", value=f"> **{region}**")
            embed.add_field(name=f"Country:", value=f"> **{country}**")
            embed.add_field(name=f"Disbursement:", value=f"> Gross: **${gross_disbursement}**\n> Net: **${net_disbursement}**")
            embed.add_field(name=f"Repayments:", value=f"> **${repayments}**")
            embed.add_field(name=f"Interest & Fees:", value=f"> **{interest}** // **{fees}**")
            embed.add_field(name=f"U.S. Commitments:", value=f"> **${ibrd_commitments_us}**")
            embed.add_field(name=f"Concessional vs Non IDA:", value=f"> Concessional: **${ida_concessional_commitments}**\n> Non: **${ida_non_concessional}**")
            embed.add_field(name=f"Other Commitments - U.S.:", value=f"> **{ida_other_commitments_us}**")
            embeds.append(embed)


        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds))




def setup(bot: commands.Bot):
    bot.add_cog(IBRS(bot))
    print('IBRS READY!')