import disnake
from disnake.ext import commands

from discord_.bot_menus.pagination import AlertMenus, PageSelect
import requests
import pandas as pd
from apis.helpers import format_large_number
class FiscalService(commands.Cog):
    def __init__(self, bot):
        self.bot=bot





    @commands.slash_command()
    async def fiscal(self, inter:disnake.AppCmdInter):

        pass





    @fiscal.sub_command()
    async def daily_treasury(self, inter:disnake.AppCmdInter):
        """⚖️Daily cash and debt operations of the U.S. Treasury."""

        await inter.response.defer()
        

        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/operating_cash_balance?sort=-record_date").json()
        data = r['data']



        # Initialize an empty DataFrame to store all chunks
        master_df = pd.DataFrame()

        # Assuming data is a list of dictionaries
        for i in range(0, len(data), 4):
            chunk = data[i:i+4]
            
            # Create a DataFrame from the chunk of 4 dictionaries
            chunk_df = pd.DataFrame(chunk)
            
            # Append this chunk DataFrame to the master DataFrame
            master_df = pd.concat([master_df, chunk_df], ignore_index=True)
        master_df.to_csv('data/daily_treasury.csv')
        embeds = []
        for i,row in master_df.iterrows():
            record_date = row['record_date']
            account_type = row['account_type']
            close_balance = float(row['close_today_bal']) * 1000000  if row['close_today_bal'] != 'null' else None
            open_balance = float(row['open_today_bal']) * 1000000 if row['open_today_bal'] != 'null' else None
            open_month_bal = float(row['open_month_bal']) * 1000000 if row['open_month_bal'] != 'null' else None
            open_fiscal_year_bal = float(row['open_fiscal_year_bal']) * 1000000
            table_nbr = row['table_nbr']
            table_nm = row['table_nm']
            sub_table = row['sub_table_name']
            source_number = row['src_line_nbr']
            record_fiscal_year = row['record_fiscal_year']
            record_fiscal_quarter = row['record_fiscal_quarter']
            record_calendar_year = row['record_calendar_year']
            record_calendar_quarter = row['record_calendar_quarter']
            record_calendar_month = row['record_calendar_month']
            record_calendar_day = row['record_calendar_day']
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Daily Treasury", description='```py\nThe Daily Treasury Statement (DTS) dataset contains a series of tables showing the daily cash and debt operations of the U.S. Treasury. The data includes operating cash balance, deposits and withdrawals of cash, public debt transactions, federal tax deposits, income tax refunds issued (by check and electronic funds transfer (EFT)), short-term cash investments, and issues and redemptions of securities. All figures are rounded to the nearest million.```', color=disnake.Colour.darker_grey())
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> SubName: **{sub_table}**\n> Account: **{account_type}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Day Balance:", value=f"> Open: **{format_large_number(open_balance)}**\n> Close: **{format_large_number(close_balance) if close_balance is not None else close_balance}**")
            embed.add_field(name=f"Month Balance:", value=f"> Open: **{format_large_number(open_month_bal)}**")
            embed.add_field(name=f"Year Balance:", value=f"> Open: **{format_large_number(open_fiscal_year_bal)}**")
            embeds.append(embed)
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple)
        button.callback = lambda interaction: interaction.response.send_message(file = disnake.File('daily_treasury.csv'), ephemeral=True)


        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))





    @fiscal.sub_command()
    async def deposits_withdrawals(self, inter:disnake.AppCmdInter):
        """⚖️Deposits and withdrawals from the Treasury General Account"""
        
        await inter.response.defer()
        
        
        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash?sort=-record_date&limit=1000").json()
        data = r['data']
        
        df = pd.DataFrame(data)
        
        df.to_csv('data/withdrawal_deposit.csv')
        embeds = []
        
        for i, row in df.iterrows():
            
            record_date = row["record_date"]
            account_type = row["account_type"]
            transaction_type = row["transaction_type"]
            transaction_catg = row["transaction_catg"]
            transaction_catg_desc = row["transaction_catg_desc"]
            transaction_today_amt = int(row["transaction_today_amt"]) * 1000000
            transaction_mtd_amt = int(row["transaction_mtd_amt"]) * 1000000
            transaction_fytd_amt = int(row["transaction_fytd_amt"]) * 1000000
            table_nbr = row["table_nbr"]
            table_nm = row["table_nm"]
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]
            
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Deposits and Withdrawals", 
                                description='```py\nThis dataset contains information about deposits and withdrawals from the Treasury General Account. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.darker_grey())
                                
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> Account: **{account_type}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Transaction Info:", value=f"> Type: **{transaction_type}**\n> Category: **{transaction_catg} ({transaction_catg_desc})**")
            embed.add_field(name=f"Day Amount:", value=f"> Today: **{format_large_number(transaction_today_amt)}**")
            embed.add_field(name=f"Month Amount:", value=f"> MTD: **{format_large_number(transaction_mtd_amt)}**")
            embed.add_field(name=f"Year Amount:", value=f"> FYTD: **{format_large_number(transaction_fytd_amt)}**")
            
            embeds.append(embed)
            
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/withdrawal_deposit.csv'), ephemeral=True)
        
       
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))


    @fiscal.sub_command()
    async def public_debt(self, inter:disnake.AppCmdInter):
        """⚖️Public Debt Transactions of the U.S. Treasury"""
        
        await inter.response.defer()
   
        
        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/public_debt_transactions?sort=-record_date&limit=1000").json()
        data = r['data']
        
        df = pd.DataFrame(data)
        
        df.to_csv('data/public_debt_transactions.csv')
        embeds = []
        
        for i, row in df.iterrows():
            
            record_date = row["record_date"]
            transaction_type = row["transaction_type"]
            security_market = row["security_market"]
            security_type = row["security_type"]
            security_type_desc = row["security_type_desc"]
            transaction_today_amt = int(row["transaction_today_amt"]) * 1000000
            transaction_mtd_amt = int(row["transaction_mtd_amt"]) * 1000000
            transaction_fytd_amt = int(row["transaction_fytd_amt"]) * 1000000
            table_nbr = row["table_nbr"]
            table_nm = row["table_nm"]
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]
            
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Public Debt Transactions", 
                                description='```py\nThis dataset contains information about public debt transactions. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.darker_grey())
                                
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> Market: **{security_market}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Transaction Info:", value=f"> Type: **{transaction_type}**\n> Security: **{security_type} ({security_type_desc})**")
            embed.add_field(name=f"Day Amount:", value=f"> Today: **{format_large_number(transaction_today_amt)}**")
            embed.add_field(name=f"Month Amount:", value=f"> MTD: **{format_large_number(transaction_mtd_amt)}**")
            embed.add_field(name=f"Year Amount:", value=f"> FYTD: **{format_large_number(transaction_fytd_amt)}**")
            
            embeds.append(embed)
            
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/public_debt_transactions.csv'), ephemeral=True)
        
        
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))


    @fiscal.sub_command()
    async def public_debt_adjusted(self, inter:disnake.AppCmdInter):
        """⚖️Adjustment of Public Debt Transactions to Cash Basis of the U.S. Treasury"""
        
        await inter.response.defer()
        
        
        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/adjustment_public_debt_transactions_cash_basis?sort=-record_date&limit=1000").json()
        data = r['data']
        
        df = pd.DataFrame(data)
        
        df.to_csv('data/adjustment_public_debt_transactions.csv')
        embeds = []
        
        for i, row in df.iterrows():
            
            record_date = row["record_date"]
            transaction_type = row["transaction_type"]
            adj_type = row["adj_type"]
            adj_type_desc = row["adj_type_desc"] if row["adj_type_desc"] else "N/A"
            adj_today_amt = int(row["adj_today_amt"]) * 1000000
            adj_mtd_amt = int(row["adj_mtd_amt"]) * 1000000
            adj_fytd_amt = int(row["adj_fytd_amt"]) * 1000000
            table_nbr = row["table_nbr"]
            table_nm = row["table_nm"]
            sub_table_name = row["sub_table_name"] if row["sub_table_name"] else "N/A"
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]
            
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Adjustment of Public Debt Transactions", 
                                description='```py\nThis dataset contains information about the adjustment of public debt transactions to a cash basis. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.darker_grey())
                                
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> SubTable: **{sub_table_name}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Transaction Info:", value=f"> Type: **{transaction_type}**\n> Adjustment: **{adj_type} ({adj_type_desc})**")
            embed.add_field(name=f"Day Amount:", value=f"> Today: **{format_large_number(adj_today_amt)}**")
            embed.add_field(name=f"Month Amount:", value=f"> MTD: **{format_large_number(adj_mtd_amt)}**")
            embed.add_field(name=f"Year Amount:", value=f"> FYTD: **{format_large_number(adj_fytd_amt)}**")
            
            embeds.append(embed)
            
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/adjustment_public_debt_transactions.csv'), ephemeral=True)
        
 
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))



    @fiscal.sub_command()
    async def debt_subject_to_limit(self, inter:disnake.AppCmdInter):
        """⚖️Debt Subject to Limit of the U.S. Treasury"""
        
        await inter.response.defer()
        
        
        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/debt_subject_to_limit?sort=-record_date&limit=1000").json()
        data = r['data']
        
        df = pd.DataFrame(data)
        
        df.to_csv('data/debt_subject_to_limit.csv')
        embeds = []
        
        for i, row in df.iterrows():
            
            record_date = row["record_date"]
            debt_catg = row["debt_catg"]
            debt_catg_desc = row["debt_catg_desc"] if row["debt_catg_desc"] else "N/A"
            close_today_bal = int(row["close_today_bal"]) * 1000000
            open_today_bal = int(row["open_today_bal"]) * 1000000
            open_month_bal = int(row["open_month_bal"]) * 1000000
            open_fiscal_year_bal = int(row["open_fiscal_year_bal"]) * 1000000
            table_nbr = row["table_nbr"]
            table_nm = row["table_nm"]
            sub_table_name = row["sub_table_name"] if row["sub_table_name"] else "N/A"
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]
            
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Debt Subject to Limit", 
                                description='```py\nThis dataset contains information about the U.S. Treasury\'s debt that is subject to limit. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.darker_grey())
                                
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> SubTable: **{sub_table_name}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Debt Info:", value=f"> Category: **{debt_catg} ({debt_catg_desc})**")
            embed.add_field(name=f"Day Balance:", value=f"> Open: **{format_large_number(open_today_bal)}**\n> Close: **{format_large_number(close_today_bal)}**")
            embed.add_field(name=f"Month Balance:", value=f"> Open: **{format_large_number(open_month_bal)}**")
            embed.add_field(name=f"Year Balance:", value=f"> Open: **{format_large_number(open_fiscal_year_bal)}**")
            
            embeds.append(embed)
            
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/debt_subject_to_limit.csv'), ephemeral=True)
        
   
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))


    @fiscal.sub_command()
    async def inter_agency_tax_transfers(self, inter:disnake.AppCmdInter):
        """⚖️Inter-agency Tax Transfers of the U.S. Treasury"""
        
        await inter.response.defer()
        
        
        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/inter_agency_tax_transfers?sort=-record_date&limit=1000").json()
        data = r['data']
        
        df = pd.DataFrame(data)
        
        df.to_csv('data/inter_agency_tax_transfers.csv')
        embeds = []
        
        for i, row in df.iterrows():
            
            record_date = row["record_date"]
            classification = row["classification"]
            today_amt = int(row["today_amt"]) * 1000000
            mtd_amt = int(row["mtd_amt"]) * 1000000
            fytd_amt = int(row["fytd_amt"]) * 1000000
            table_nbr = row["table_nbr"]
            table_nm = row["table_nm"]
            sub_table_name = row["sub_table_name"] if row["sub_table_name"] else "N/A"
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]
            
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Inter-agency Tax Transfers", 
                                description='```py\nThis dataset contains information about the U.S. Treasury\'s inter-agency tax transfers. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.darker_grey())
                                
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> SubTable: **{sub_table_name}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Transaction Info:", value=f"> Classification: **{classification}**")
            embed.add_field(name=f"Day Amount:", value=f"> Today: **{format_large_number(today_amt)}**")
            embed.add_field(name=f"Month Amount:", value=f"> MTD: **{format_large_number(mtd_amt)}**")
            embed.add_field(name=f"Year Amount:", value=f"> FYTD: **{format_large_number(fytd_amt)}**")
            
            embeds.append(embed)
            
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/inter_agency_tax_transfers.csv'), ephemeral=True)
        
      
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))



    @fiscal.sub_command()
    async def refunds_issued(self, inter:disnake.AppCmdInter):
        """⚖️Income Tax Refunds Issued by the U.S. Treasury"""
        
        await inter.response.defer()
        
        
        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/income_tax_refunds_issued?sort=-record_date&limit=1000").json()
        data = r['data']
        
        df = pd.DataFrame(data)
        
        df.to_csv('data/income_tax_refunds_issued.csv')
        embeds = []
        
        for i, row in df.iterrows():
            
            record_date = row["record_date"]
            tax_refund_type = row["tax_refund_type"]
            tax_refund_type_desc = row["tax_refund_type_desc"] if row["tax_refund_type_desc"] else "N/A"
            tax_refund_today_amt = float(row["tax_refund_today_amt"]) * 1000000
            tax_refund_mtd_amt = float(row["tax_refund_mtd_amt"]) * 1000000
            tax_refund_fytd_amt = float(row["tax_refund_fytd_amt"]) * 1000000
            table_nbr = row["table_nbr"]
            table_nm = row["table_nm"]
            sub_table_name = row["sub_table_name"] if row["sub_table_name"] else "N/A"
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]
            
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Income Tax Refunds Issued", 
                                description='```py\nThis dataset contains information about income tax refunds issued by the U.S. Treasury. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.darker_grey())
                                
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> SubTable: **{sub_table_name}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Transaction Info:", value=f"> Type: **{tax_refund_type} ({tax_refund_type_desc})**")
            embed.add_field(name=f"Day Amount:", value=f"> Today: **{format_large_number(tax_refund_today_amt)}**")
            embed.add_field(name=f"Month Amount:", value=f"> MTD: **{format_large_number(tax_refund_mtd_amt)}**")
            embed.add_field(name=f"Year Amount:", value=f"> FYTD: **{format_large_number(tax_refund_fytd_amt)}**")
            
            embeds.append(embed)
            
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/income_tax_refunds_issued.csv'), ephemeral=True)
        
      
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))


    @fiscal.sub_command()
    async def tax_deposits(self, inter:disnake.AppCmdInter):
        """⚖️Federal Tax Deposits by the U.S. Treasury"""
        
        await inter.response.defer()
        
        
        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/federal_tax_deposits?sort=-record_date&limit=1000").json()
        data = r['data']
        
        df = pd.DataFrame(data)
        
        df.to_csv('data/federal_tax_deposits.csv')
        embeds = []
        
        for i, row in df.iterrows():
            
            record_date = row["record_date"]
            tax_deposit_type = row["tax_deposit_type"]
            tax_deposit_type_desc = row["tax_deposit_type_desc"] if row["tax_deposit_type_desc"] else "N/A"
            tax_deposit_today_amt = float(row["tax_deposit_today_amt"]) * 1000000
            tax_deposit_mtd_amt = float(row["tax_deposit_mtd_amt"]) * 1000000
            tax_deposit_fytd_amt = float(row["tax_deposit_fytd_amt"]) * 1000000
            table_nbr = row["table_nbr"]
            table_nm = row["table_nm"]
            sub_table_name = row["sub_table_name"] if row["sub_table_name"] else "N/A"
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]
            
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Federal Tax Deposits", 
                                description='```py\nThis dataset contains information about federal tax deposits by the U.S. Treasury. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.darker_grey())
                                
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> SubTable: **{sub_table_name}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Transaction Info:", value=f"> Type: **{tax_deposit_type} ({tax_deposit_type_desc})**")
            embed.add_field(name=f"Day Amount:", value=f"> Today: **{format_large_number(tax_deposit_today_amt)}**")
            embed.add_field(name=f"Month Amount:", value=f"> MTD: **{format_large_number(tax_deposit_mtd_amt)}**")
            embed.add_field(name=f"Year Amount:", value=f"> FYTD: **{format_large_number(tax_deposit_fytd_amt)}**")
            
            embeds.append(embed)
            
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/federal_tax_deposits.csv'), ephemeral=True)
        
   
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))



    @fiscal.sub_command()
    async def short_term_cash_investments(self, inter:disnake.AppCmdInter):
        """⚖️Short-Term Cash Investments by the U.S. Treasury"""
        
        await inter.response.defer()
        
        
        r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/short_term_cash_investments?sort=-record_date&limit=1000").json()
        data = r['data']
        
        df = pd.DataFrame(data)
        
        df.to_csv('data/short_term_cash_investments.csv')
        embeds = []
        
        for i, row in df.iterrows():
            
            record_date = row["record_date"]
            transaction_type = row["transaction_type"]
            transaction_type_desc = row["transaction_type_desc"] if row["transaction_type_desc"] else "N/A"
            depositary_type_a_amt = float(row["depositary_type_a_amt"]) * 1000000
            depositary_type_b_amt = float(row["depositary_type_b_amt"]) * 1000000
            depositary_type_c_amt = float(row["depositary_type_c_amt"]) * 1000000
            total_amt = float(row["total_amt"]) * 1000000
            table_nbr = row["table_nbr"]
            table_nm = row["table_nm"]
            sub_table_name = row["sub_table_name"] if row["sub_table_name"] else "N/A"
            src_line_nbr = row["src_line_nbr"]
            record_fiscal_year = row["record_fiscal_year"]
            record_fiscal_quarter = row["record_fiscal_quarter"]
            record_calendar_year = row["record_calendar_year"]
            record_calendar_quarter = row["record_calendar_quarter"]
            record_calendar_month = row["record_calendar_month"]
            record_calendar_day = row["record_calendar_day"]
            
            embed = disnake.Embed(title=f"⚖️ Fiscal Service ⚖️ - Short-Term Cash Investments", 
                                description='```py\nThis dataset contains information about short-term cash investments by the U.S. Treasury. All figures are rounded to the nearest million.```', 
                                color=disnake.Colour.darker_grey())
                                
            embed.add_field(name=f"Record:", value=f"> Name: **{table_nm}**\n> SubTable: **{sub_table_name}**\n> Date: **{record_date}**")
            embed.add_field(name=f"Fiscal Info:", value=f"> Year: **{record_fiscal_year}**\n> Qtr: **{record_fiscal_quarter}**")
            embed.add_field(name=f"Calendar Info:", value=f"> Year: **{record_calendar_year}**\n> Qtr: **{record_calendar_quarter}**\n> Month: **{record_calendar_month}**\n> Day: **{record_calendar_day}**")
            embed.add_field(name=f"Transaction Info:", value=f"> Type: **{transaction_type} ({transaction_type_desc})**")
            embed.add_field(name=f"Depositary Type A Amount:", value=f"> **{format_large_number(depositary_type_a_amt)}**")
            embed.add_field(name=f"Depositary Type B Amount:", value=f"> **{format_large_number(depositary_type_b_amt)}**")
            embed.add_field(name=f"Depositary Type C Amount:", value=f"> **{format_large_number(depositary_type_c_amt)}**")
            embed.add_field(name=f"Total Amount:", value=f"> **{format_large_number(total_amt)}**")
            
            embeds.append(embed)
            
        button = disnake.ui.Button(style=disnake.ButtonStyle.blurple, emoji="📥")
        button.callback = lambda interaction: interaction.response.send_message(file=disnake.File('data/short_term_cash_investments.csv'), ephemeral=True)
        
      
        await inter.edit_original_message(embed=embeds[0], view=AlertMenus(embeds).add_item(ServiceSelect()))


fiscal = FiscalService(commands.Bot)


class ServiceSelect(disnake.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder='Select A Function -->',
            min_values=1,
            max_values=1,
            custom_id='fiscalselect',
            options=[
                disnake.SelectOption(label='Daily Treasury', value="0", description='View daily treasury balance.'),
                disnake.SelectOption(label='Deposits Withdrawals', value="1", description='View deposits and withdrawals.'),
                disnake.SelectOption(label='Public Debt Transactions', value="8", description='View public debt transactions.'),
                disnake.SelectOption(label='Adjustment Public Debt', value="2", description='View adjustment of public debt transactions.'),
                disnake.SelectOption(label='Debt Subject to Limit', value="3", description='View debt subject to limit.'),
                disnake.SelectOption(label='Inter-agency Tax Transfers', value="4", description='View inter-agency tax transfers.'),
                disnake.SelectOption(label='Income Tax Refunds Issued', value="5", description='View income tax refunds issued.'),
                disnake.SelectOption(label='Federal Tax Deposits', value="6", description='View federal tax deposits.'),
                disnake.SelectOption(label='Short-Term Cash Investments', value="7", description='View short-term cash investments.'),
                # Add more functions here as needed
            ]
        )

    async def callback(self, inter:disnake.AppCmdInter):
        if self.values[0] == "0":
            await fiscal.daily_treasury(inter)
        elif self.values[0] == "1":
            await fiscal.deposits_withdrawals(inter)
        elif self.values[0] == "2":
            await fiscal.public_debt_adjusted(inter)
        elif self.values[0] == "8":
            await fiscal.public_debt(inter)
        elif self.values[0] == "3":
            await fiscal.debt_subject_to_limit(inter)
        elif self.values[0] == "4":
            await fiscal.inter_agency_tax_transfers(inter)
        elif self.values[0] == "5":
            await fiscal.refunds_issued(inter)
        elif self.values[0] == "6":
            await fiscal.tax_deposits(inter)
        elif self.values[0] == "7":
            await fiscal.short_term_cash_investments(inter)
        
        # Add more elif statements here for additional functions







def setup(bot: commands.Bot):
    bot.add_cog(FiscalService(bot))
    print(f'FISCAL SERVICE COMMANDS - READY!')