from openai import OpenAI
import os
from dotenv import load_dotenv
load_dotenv()
from fudstop4.apis.sec.sec_sdk import SECSDK
from fudstop4.apis.y_finance.yf_sdk import YfSDK
from fudstop4.apis.ofr.ofr_sdk import OFR, MMF_OFR
import pandas as pd
import requests
from fudstop4.apis.helpers import camel_to_snake_case
import asyncio
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions(database='ofr')
ofr = OFR()

funds = ['Matrix 360 Holdings LLC', 'Aquila Investment Management ', 'Alger Funds', 'NetXD Inc.', 'Sentinel Asset Management', 'EquiTrust Life Insurance Company', 'WisdomTree, Inc.', 'Timothy Plan', 'Natixis Asset Management', 'Texas Capital Bancshares Inc', 'Advanced Asset Management', 'Madison Mosaic', 'Direxion Funds', 'Semper Capital Management', 'Scout Investments, Inc.', 'Selected Funds', 'Value Line', 'Saratoga Capital  Management', 'TCW Investment Management', 'Bishop Street Capital', 'Old Mutual Capital', 'JNF Advisors Inc', 'RMB Asset Management', 'Pyxis Funds', 'SAR Holdings, Inc.', 'Ambassador Funds', 'Popular Inc.', 'Madison Investment', 'Nicholas Funds', 'Trust for Credit Unions', 'Russell', 'TCG Financial Services', 'Weitz Investment Management', 'US Global Investors', 'Milestone', 'Eaton Vance', 'OneAmerica Funds', 'Forward Funds', 'Shelton Capital Management', 'Alpine', 'Pacific Capital Investments', 'Performance Funds', 'Ohio National Investments', 'Sun Life Financial', 'First Investors Management', 'Macquarie', 'Harbor', 'RS Funds', 'Sterling Capital', 'Securian Financial', 'TCW Group, Inc', 'Penn Mutual Life', 'ProFunds', 'Hancock Horizon', 'Pacific Life', 'Calvert', 'Davis Funds', 'RBB Fund', 'State Farm', 'Homestead', 'Mutual of America', 'Williams Capital Management', 'CLS AdvisorOne', 'Miles Capital', 'Victory Capital Management', 'Touchstone', 'Pioneer Investments', 'Meeder Funds', 'GE Asset Management', 'Paydenfunds', 'Valic', 'William Blair', 'Huntington', 'SunAmerica', 'Northwestern Mutual', 'Virtus', 'Hartford Mutual Funds', 'Mass Mutual', 'Glenmede', 'Allianz', 'Lord Abbett', 'Fifth Third', 'Guggenheim Investments', 'Highmark', 'Great-West Funds', 'Lincoln National', 'American Beacon', 'Plan Investment Fund', 'Bank of New York Mellon', 'Janus', 'Ivy Funds', 'GuideStone Funds', 'AXA Equitable', 'PIMCO', 'MainStay Funds', 'Transamerica', 'Voya', 'Thrivent Financial', 'Mount Vernon', 'Gabelli Funds', 'Putnam', 'Principal Funds', 'Reich & Tang', 'Nationwide', 'Public Financial Management', 'Brown Brothers Harriman', 'Cavanal Hill', 'BMO Funds', 'TD Asset Management', 'John Hancock', 'Jackson National', 'PNC Investments', 'CNI Charter', 'American Century', 'Oppenheimer', 'MFS Investment', 'USAA', 'Wilmington Funds', 'AllianceBernstein', 'TIAA-CREF', 'SEI Investments', 'U.S. Bancorp.', 'Columbia', 'Royal Bank of Canada', 'Dimensional', 'Franklin Templeton', 'Bank of America', 'HSBC', 'Prudential', 'DWS Investments', 'T. Rowe Price', 'UBS', 'First American', 'American Funds', 'Invesco', 'Legg Mason', 'Northern Trust Funds', 'State Street', 'Allspring Funds', 'Morgan Stanley', 'Dreyfus', 'Schwab', 'Goldman Sachs', 'Federated', 'Blackrock', 'Vanguard', 'JP Morgan', 'Fidelity']

standardized_funds = [name.lower().replace(" ", "_") for name in funds]


async def main():
    await opts.connect()
    for fund in standardized_funds:


        url_second_level = requests.get(f"https://www.financialresearch.gov/money-market-funds/data/portfolio_risks/{fund}/data.json").json()
        url_second_level = url_second_level['datatable']
        url_second_level_columns = url_second_level['columns']
        title = [i.get('title') for i in url_second_level_columns if i.get('title') != 'Date']
        standardized_title = [name.lower().replace(" ", "_") for name in title]
        for title in standardized_title:





            url_next_level=f"https://www.financialresearch.gov/money-market-funds/data/portfolio_risks/{fund}/{title}/data.json"
    
            url_next_level = requests.get(url_next_level).json()

            datatable = url_next_level['datatable']

            # Extract column names
            column_names = [col["title"] for col in datatable["columns"]]

            # Extract values
            values = datatable["values"]

            # Create DataFrame
            df = pd.DataFrame(values, columns=column_names)
            df['fund'] = fund
   

            await opts.batch_upsert_dataframe(df, table_name=fund, unique_columns=['date'])
            # Convert 'Date' column to datetime if necessary
            
            df['date'] = pd.to_datetime(df['Date'])


            for country in column_names:
                country = country.lower().replace(' ', '_').replace('(','').replace(')','')
                if country == 'date':
                    continue
                url_third_level = requests.get(f"https://www.financialresearch.gov/money-market-funds/data/portfolio_risks/{fund}/{title}/{country}/data.json").json()



                third_datatable = url_third_level['datatable']

                # Extract column names
                third_column_names = [col["title"] for col in third_datatable["columns"]]

                # Extract values
                third_values = third_datatable["values"]

                # Create DataFrame
                df = pd.DataFrame(third_values, columns=third_column_names)
                df = camel_to_snake_case(columns=df.columns)
                print(df)
            

                for credit in third_column_names:
                    if credit == 'Date':
                        continue
                    credit = credit.lower().replace(' ','_')
                    url_fourth_level = requests.get(f"https://www.financialresearch.gov/money-market-funds/data/portfolio_risks/{fund}/{title}/{country}/{credit}/data.json").json()

                    credit = credit.replace('&', '_').replace('-', '_')
                    fourth_datatable = url_fourth_level['datatable']
                    fourth_column_names = [col["title"] for col in fourth_datatable["columns"]]
                    fourth_values = fourth_datatable["values"]
                    # Create DataFrame
                    df = pd.DataFrame(fourth_values, columns=fourth_column_names)
                    df = df.rename(columns={'Date': 'date'})
                    await opts.batch_upsert_dataframe(df=df, table_name=credit, unique_columns=['date'])


asyncio.run(main())