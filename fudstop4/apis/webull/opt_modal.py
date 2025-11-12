import os
from dotenv import load_dotenv
import pandas as pd
load_dotenv()

import asyncio
from datetime import datetime
from apis.webull.webull_options import WebullOptions

options = WebullOptions(host='localhost', user='chuck', database='wb_opts', password='fud', port=5432)



import disnake
from disnake.ext import commands

import disnake
import asyncpg

class OptionModal(disnake.ui.Modal):
    def __init__(self):
        components = [
            disnake.ui.TextInput(
                label="Symbol",
                custom_id="input_1",
                style=disnake.TextInputStyle.short,
                max_length=7,
                placeholder='e.g. SPY'
            ),
            disnake.ui.TextInput(
                label="Expiration",
                placeholder='e.g. 2023-12-15',
                max_length=10,
                custom_id="input_2",
                style=disnake.TextInputStyle.short,
            ),
            disnake.ui.TextInput(
                label="Call or Put?",
                custom_id="input_3",
                style=disnake.TextInputStyle.short,
            ),
            disnake.ui.TextInput(
                label="What Strike?",
                custom_id="input_4",
                placeholder='e.g. 125.5',
                style=disnake.TextInputStyle.short,
            ),
        ]
        super().__init__(title="Option Data Query", custom_id="option_query_modal", components=components)

    async def callback(self, inter: disnake.ModalInteraction):
        await options.connect()
        symbol = inter.text_values['input_1']
        expiration = inter.text_values['input_2']
        call_put = inter.text_values['input_3'].lower()
        strike = inter.text_values['input_4']

        # Ensure that the strike is a float and expiration is a valid date string
    # Convert the strike to a float and the expiration to a date object
        try:
            strike = float(strike)
            expiration = datetime.strptime(expiration, '%Y-%m-%d').date()
        except ValueError:
            await inter.response.send_message("Invalid strike price or expiration date format.", ephemeral=True)
            return

        # Placeholder for database connection
        # conn = await asyncpg.connect('postgresql://user:password@host/database')
        
        query = f"""
        SELECT * FROM options_data
        WHERE symbol = $1
        AND expiry_date = $2
        AND call_put = $3
        AND strike_price = $4
        """
        
        
        records = await options.fetch(query, symbol, expiration, call_put, strike)
    

   
        await inter.response.send_message(f"Query results: {records}")


        print(query)
        print(symbol, expiration, call_put, strike)

        # After performing the query, you would process the records and send back a response
        # For example:
        # formatted_records = process_records(records)
        # await inter.response.send_message(formatted_records, ephemeral=True)


class SQLQueryModal(disnake.ui.Modal):
    def __init__(self):
        components = [
            disnake.ui.TextInput(
                label="Columns",
                custom_id="input_1",
                style=disnake.TextInputStyle.short,
                placeholder='e.g. column1, column2, ...'
            ),
            disnake.ui.TextInput(
                label="WHERE",
                custom_id="input_2",
                style=disnake.TextInputStyle.short,
                placeholder='e.g. column_name = value'
            ),
            disnake.ui.TextInput(
                label="ORDER BY",
                custom_id="input_3",
                style=disnake.TextInputStyle.short,
                placeholder='e.g. column_name ASC/DESC'
            ),
            disnake.ui.TextInput(
                label="LIMIT",
                custom_id="input_4",
                style=disnake.TextInputStyle.short,
                placeholder='e.g. 10'
            ),
        ]
        super().__init__(title="SQL Query Constructor", custom_id="sql_query_modal", components=components)

    async def callback(self, inter: disnake.ModalInteraction):
        await options.connect()
        await inter.response.defer()
        columns = inter.text_values['input_1']
        where = inter.text_values['input_2']
        order_by = inter.text_values['input_3']
        limit = inter.text_values['input_4']

        # Construct the query
        query = f"SELECT {columns} FROM options_data"
        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"

        # Send the constructed query back to the user
        await inter.edit_original_message(f"Constructed SQL Query: ```sql\n{query}\n```")
        await asyncio.sleep(2)
        await inter.edit_original_message(f"> # Querying Database..")
        await asyncio.sleep(2)
        await inter.edit_original_message(f"> # Building CSV file...")

        await asyncio.sleep(4)
        results = await options.fetch(query)    

        df = pd.DataFrame(results)

        df.to_csv('data_query.csv')
        await inter.edit_original_message(file=disnake.File(f"data_query.csv"))
# In your bot command, you would invoke this modal like so: