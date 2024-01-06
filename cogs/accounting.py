import disnake
import requests
from disnake.ext import commands

from discord_.bot_menus.pagination import AlertMenus
from apis.helpers import format_large_number
import pandas as pd

class Accounting(commands.Cog):
    def __init__(self, bot):
        self.bot=bot
     





    @commands.slash_command()
    async def accounting(self, inter):
        pass






    @accounting.sub_command()
    async def weekly_treasury_offerings(self, inter: disnake.AppCmdInter):
        """🖋️ Details of Offerings for Regular Weekly Treasury Bills"""

        await inter.response.defer()
        

        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/tb/pdo1_offerings_regular_weekly_treasury_bills?sort=-record_date&limit=1000").json()
        data = r['data']

        df = pd.DataFrame(data)
        df.to_csv('data/weekly_treasury_offerings.csv')
        embeds = []

        for i, row in df.iterrows():
            record_date = row["record_date"]
            issue_date = row["issue_date"]
            maturity_date = row["maturity_date"]
            days_to_maturity = row["days_to_maturity"]
            bids_tendered = float(row["bids_tendered_mil_amt"]) * 1000000
            bids_acc_total = float(row["bids_acc_total_mil_amt"]) * 1000000
            bids_acc_comp_basis = float(row["bids_acc_comp_basis_mil_amt"]) * 1000000
            bids_acc_noncomp_basis = float(row["bids_acc_noncomp_basis_mil_amt"]) * 1000000
            high_price_per_hundred = row["high_price_per_hundred"]
            high_discount_rate = row["high_discount_rate"]
            high_investment_rate = row["high_investment_rate"]
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]

            embed = disnake.Embed(title=f"🖋️ Accounting Service - Weekly Treasury Offerings", 
                                description='```py\nDetails of offerings for regular weekly Treasury Bills. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.dark_teal())

            embed.add_field(name=f"Record:", value=f"> Name: **Regular Weekly Treasury Bills**\n> Date: **{record_date}**\n> Issue Date: **{issue_date}**\n> Maturity Date: **{maturity_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Bids Info:", value=f"> Tendered: **{format_large_number(bids_tendered)}**\n> Accepted Total: **{format_large_number(bids_acc_total)}**\n> Accepted on Competitive Basis: **{format_large_number(bids_acc_comp_basis)}**\n> Accepted on Non-Competitive Basis: **{format_large_number(bids_acc_noncomp_basis)}**")
            embed.add_field(name=f"Price and Rate Info:", value=f"> High Price Per Hundred: **{high_price_per_hundred}**\n> High Discount Rate: **{high_discount_rate}**\n> High Investment Rate: **{high_investment_rate}**")
            embed.add_field(name=f"Days to Maturity:", value=f"> **{days_to_maturity}**")
            
            embeds.append(embed)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/weekly_treasury_offerings.csv'), ephemeral=True)


        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(AccountingSelect()))

    @accounting.sub_command()
    async def marketable_securities_offerings(self, inter: disnake.AppCmdInter):
        """🖋️ Offerings of Marketable Securities Other Than Regular Weekly Treasury Bills"""

        await inter.response.defer()
        

        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/tb/pdo2_offerings_marketable_securities_other_regular_weekly_treasury_bills?sort=-record_date&limit=1000").json()
        data = r['data']

        df = pd.DataFrame(data)
        df.to_csv('data/marketable_securities_offerings.csv')
        embeds = []

        for i, row in df.iterrows():
            record_date = row["record_date"]
            auction_date = row["auction_date"]
            issue_date = row["issue_date"]
            securities_desc = row["securities_desc"]
            period_to_final_maturity = row["period_to_final_maturity"]
            tendered_mil_amt = float(row["tendered_mil_amt"]) * 1000000
            acc_mil_amt = float(row["acc_mil_amt"]) * 1000000
            acc_yield_discount_margin = row["acc_yield_discount_margin"]
            eq_price_for_notes_bonds = row["eq_price_for_notes_bonds"]
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]

            embed = disnake.Embed(title=f"🖋️ Accounting Service - Marketable Securities Offerings", 
                                description='```py\nDetails of offerings for marketable securities other than regular weekly Treasury Bills. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.dark_teal())

            embed.add_field(name=f"Record:", value=f"> Date: **{record_date}**\n> Description: **{securities_desc}**\n> Auction Date: **{auction_date}**\n> Issue Date: **{issue_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Offerings Info:", value=f"> Tendered: **{format_large_number(tendered_mil_amt)}**\n> Accepted: **{format_large_number(acc_mil_amt)}**\n> Yield/Discount Margin: **{acc_yield_discount_margin}**\n> Eq. Price for Notes/Bonds: **{eq_price_for_notes_bonds}**")
            embed.add_field(name=f"Final Maturity:", value=f"> **{period_to_final_maturity}**")
            
            embeds.append(embed)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/marketable_securities_offerings.csv'), ephemeral=True)


        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(AccountingSelect()))


    @accounting.sub_command()
    async def federal_securities_distribution(self, inter: disnake.AppCmdInter):
        """🖋️ Distribution of Federal Securities by Class of Investors and Type of Issues"""

        await inter.response.defer()
        

        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/tb/ofs1_distribution_federal_securities_class_investors_type_issues?sort=-record_date&limit=1000").json()
        data = r['data']

        df = pd.DataFrame(data)
        df.to_csv('data/federal_securities_distribution.csv')
        embeds = []

        for i, row in df.iterrows():
            record_date = row["record_date"]
            end_fiscal_year_or_month = row["end_fiscal_year_or_month"]
            securities_classification = row["securities_classification"]
            investors_classification = row["investors_classification"]
            issues_type = row["issues_type"]
            securities_mil_amt = float(row["securities_mil_amt"]) * 1000000
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]

            embed = disnake.Embed(title=f"🖋️ Accounting Service - Federal Securities Distribution",
                                description='```py\nDistribution of Federal securities by class of investors and type of issues. All figures are rounded to the nearest million.```',
                                color=disnake.Colour.darker_grey())

            embed.add_field(name=f"Record:", value=f"> Date: **{record_date}**\n> Fiscal Year End: **{end_fiscal_year_or_month}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Securities Info:", value=f"> Classification: **{securities_classification}**\n> Investors Classification: **{investors_classification}**\n> Issues Type: **{issues_type}**")
            embed.add_field(name=f"Amount:", value=f"> **{securities_mil_amt}**")

            embeds.append(embed)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/federal_securities_distribution.csv'), ephemeral=True)


        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(AccountingSelect()))



    @accounting.sub_command()
    async def estimated_ownership(self, inter: disnake.AppCmdInter):
        """🖋️ Estimated Ownership of Treasury Securities"""

        await inter.response.defer()
        

        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/tb/ofs2_estimated_ownership_treasury_securities?sort=-record_date&limit=1000").json()
        data = r['data']

        df = pd.DataFrame(data)
        df.to_csv('data/estimated_ownership.csv')
        embeds = []

        for i, row in df.iterrows():
            record_date = row["record_date"]
            end_of_month = row["end_of_month"]
            securities_owner = row["securities_owner"]
            securities_bil_amt = float(row["securities_bil_amt"]) * 1000000
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]

            embed = disnake.Embed(title=f"🖋️ Accounting Service - Estimated Ownership of Treasury Securities",
                                description='```py\nEstimated ownership of Treasury securities by various entities. All figures are rounded to the nearest billion.```',
                                color=disnake.Colour.darker_grey())

            embed.add_field(name=f"Record:", value=f"> Date: **{record_date}**\n> End of Month: **{end_of_month}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Ownership Info:", value=f"> Owner: **{securities_owner}**")
            embed.add_field(name=f"Amount:", value=f"> **{securities_bil_amt}**")

            embeds.append(embed)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/estimated_ownership.csv'), ephemeral=True)

      
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(AccountingSelect()))



    @accounting.sub_command()
    async def outstanding_circulation(self, inter: disnake.AppCmdInter):
        """🖋️ Amounts Outstanding in Circulation"""

        await inter.response.defer()
        

        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/tb/uscc1_amounts_outstanding_circulation?sort=-record_date&limit=1000").json()
        data = r['data']

        df = pd.DataFrame(data)
        df.to_csv('data/outstanding_circulation.csv')
        embeds = []

        for i, row in df.iterrows():
            # Converting all relevant values to float for numerical accuracy
            # Handle amounts outstanding
            for col in ['total_currency_coins_amt', 'total_currency_amt', 'federal_reserve_notes_amt', 
                        'us_notes_amt', 'currency_no_longer_issued_amt', 'total_coins_amt', 
                        'dollar_coins_amt', 'fractional_coins_amt']:
                value = row[col]
                if pd.isna(value):
                    row[col] = "N/A"  # Replace NaN with "N/A"
                else:
                    row[col] = float(value)  # Convert to float if it's not NaN


            embed = disnake.Embed(title=f"🖋️ Accounting Service - Amounts Outstanding in Circulation", 
                                description='```py\nData on the amounts of currency and coins outstanding in circulation.```', 
                                color=disnake.Colour.darker_grey())

            embed.add_field(name=f"Record:", value=f"> Date: **{row['record_date']}**\n> As of Date: **{row['currency_coins_as_of_date']}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{row['record_fiscal_year']}**\n> Qtr: **{row['record_fiscal_quarter']}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{row['record_calendar_year']}**\n> Qtr: **{row['record_calendar_quarter']}**\n> Month: **{row['record_calendar_month']}**\n> Day: **{row['record_calendar_day']}**")
            embed.add_field(name=f"Amounts Outstanding:", value=f"> Total Currency and Coins: **{row['total_currency_coins_amt']}**\n> Total Currency: **{row['total_currency_amt']}**\n> Federal Reserve Notes: **{row['federal_reserve_notes_amt']}**")
            embed.add_field(name=f"More Details:", value=f"> US Notes: **{row['us_notes_amt']}**\n> No Longer Issued: **{row['currency_no_longer_issued_amt']}**\n> Total Coins: **{row['total_coins_amt']}**\n> Dollar Coins: **{row['dollar_coins_amt']}**\n> Fractional Coins: **{row['fractional_coins_amt']}**")

            embeds.append(embed)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/outstanding_circulation.csv'), ephemeral=True)

       
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(AccountingSelect()))



    @accounting.sub_command()
    async def esf_balances(self, inter: disnake.AppCmdInter):
        """🖋️ Balances in the Exchange Stabilization Fund"""

        await inter.response.defer()
        

        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/tb/esf1_balances?sort=-record_date&limit=1000").json()
        data = r['data']

        df = pd.DataFrame(data)
        df.to_csv('data/esf_balances.csv')
        embeds = []

        for i, row in df.iterrows():
            # Handling null values
            activity_thous_amt = float(row["activity_thous_amt"]) if row["activity_thous_amt"] != "null" else "N/A"
            balance_thous_amt = float(row["balance_thous_amt"]) if row["balance_thous_amt"] != "null" else "N/A"

            embed = disnake.Embed(title=f"🖋️ Accounting Service - ESF Balances", 
                                description='```py\nBalances in the Exchange Stabilization Fund.```', 
                                color=disnake.Colour.darker_grey())

            embed.add_field(name=f"Record:", value=f"> Date: **{row['record_date']}**\n> Report Date: **{row['report_date']}**\n> Classification: **{row['classification_desc']}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{row['record_fiscal_year']}**\n> Qtr: **{row['record_fiscal_quarter']}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{row['record_calendar_year']}**\n> Qtr: **{row['record_calendar_quarter']}**\n> Month: **{row['record_calendar_month']}**\n> Day: **{row['record_calendar_day']}**")
            embed.add_field(name=f"Amounts:", value=f"> Activity (Thousand): **{activity_thous_amt}**\n> Balance (Thousand): **{balance_thous_amt}**")

            embeds.append(embed)

        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/esf_balances.csv'), ephemeral=True)

     
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(AccountingSelect()))

accounting = Accounting(commands.Bot)

class AccountingSelect(disnake.ui.Select):
    def __init__(self):
        super().__init__( 
            placeholder='Select A Function -->',
            min_values=1,
            max_values=1,
            custom_id='accountingSelect',
            options=[
                disnake.SelectOption(label='Weekly Treasuries', value="0", description='View weekly treasury offerings.', emoji="🖋️"),
                disnake.SelectOption(label='Marketable Securities', value="1", description='View marketable securities offerings.', emoji="🖋️"),
                disnake.SelectOption(label='Treasury Ownership', value="2", description='View estimated ownership of treasury securities.', emoji="🖋️"),
                disnake.SelectOption(label='Outstanding Circulation', value="3", description='View amounts outstanding in circulation.', emoji="🖋️"),
                disnake.SelectOption(label='ESF Balances', value="5", description='View balances in the Exchange Stabilization Fund.', emoji="🖋️"),
                disnake.SelectOption(label='Federal Securities Dist.', value="6", description=f"View federal securities distributions.")
                # Add more options here as needed
            ]
        )
        



    async def callback(self, inter: disnake.AppCmdInter):
        if self.values[0] == "0":
            await accounting.weekly_treasury_offerings(inter)
        elif self.values[0] == "1":
            await accounting.marketable_securities_offerings(inter)
        elif self.values[0] == "2":
            await accounting.estimated_ownership(inter)
        elif self.values[0] == "3":
            await accounting.outstanding_circulation(inter)
        elif self.values[0] == "5":
            await accounting.esf_balances(inter)
        elif self.values[0] == "6":
            await accounting.federal_securities_distribution(inter)

def setup(bot: commands.Bot):
    bot.add_cog(Accounting(bot))
    print('Accounting commands - READY!')
