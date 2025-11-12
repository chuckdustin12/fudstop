import os
from dotenv import load_dotenv
import re
load_dotenv()
import disnake
from disnake import TextInput,TextInputStyle
import pandas as pd
from datetime import datetime
from tabulate import tabulate
from .webull_options import WebullOptions 


options = WebullOptions(user='chuck', database='charlie', host='localhost', port=5432, password='fud')


class WebullModal(disnake.ui.Modal):
    def __init__(self, selected_columns, ticker:str):
        self.connection_string = os.environ.get('WEBULL_OPTIONS')
        
        self.ticker = ticker.upper()
        self.selected_columns = selected_columns
        # Placeholder text for specific fields
        placeholders = {
            'call_put': "Enter 'call' or 'put'",
            'expiry_date': "Enter a date (e.g., '2023-12-31')",
            'open_interest': "Enter a numerical threshold (e.g., '> 1000')",
            'open_interest_change': "Enter a change value (e.g., '< -50')",
            'strike_price': "Enter a strike price (e.g., '= 300')",
            # You can continue to add more fields with specific placeholders if needed.
            'volume': "Enter a volume amount (e.g., '>= 50000')",
            'bid_price': "Enter a bid price threshold (e.g., '< 5.00')",
            'ask_price': "Enter an ask price threshold (e.g., '> 5.00')",
            'theta': "Enter a theta value threshold",
            'gamma': "Enter a gamma value threshold",
            # Add any additional fields relevant to your options data and what users may want to query.
        }
        # Default placeholder for fields not in the dictionary
        default_placeholder = "Enter condition (e.g., '> 100')"

        components = []
        for column in selected_columns:  # Create a TextInput for each selected column
            components.append(
                disnake.ui.TextInput(
                    label=f"{column}",
                    placeholder=placeholders.get(column, default_placeholder),
                    custom_id=f"condition_{column}",
                    style=TextInputStyle.short,
                )
            )
        # Make sure to pass the components to the super().__init__
        super().__init__(title="Query Options Database", components=components)
        
    async def callback(self, inter: disnake.ModalInteraction):
            await options.connect()  # make sure you have a connection method that sets up your pool
            conditions = []
            for column in self.selected_columns:
            
                
                
                user_input = inter.text_values[f'condition_{column}'].strip()

                if user_input:
                    # Directly include user_input in the condition string
                    # Check if the column is 'expiry_date' and wrap the value in single quotes
                    if column == 'expiry_date':
                        match = re.match(r"([<>]=?|=?)\s*(\d{4}-\d{2}-\d{2})$", user_input)
                        operator, number = match.groups()
                        print(operator)
                        # For 'expiry_date', you expect the input in the format 'YYYY-MM-DD'
                        conditions.append(f"{column} {operator} '{user_input}'")
                   

                    if column == 'call_put':
                        # For 'expiry_date', you expect the input in the format 'YYYY-MM-DD'
                        conditions.append(f"{column} = '{user_input}'")


                    if column == 'strike_price':
                        # This regex matches operators followed by an integer or decimal number for strike_price
                        match = re.match(r"([<>]=?|=?)\s*(\d+(?:\.\d+)?)$", user_input)
                        if match:
                            operator, number = match.groups()
                            # Construct the condition string for strike_price
                            condition = f"{column} {operator} {number}"
                            conditions.append(condition)
                        else:
                            # If the input doesn't match the pattern, send an error message
                            await inter.response.send_message(f"Invalid format for strike price: {user_input}. Expected format: '<operator> <number>'")
                            return
                        
                                                    
                    match = None

                    if column == 'strike_price':
                        # Matches numeric conditions for strike_price
                        match = re.match(r"([<>]=?|=?)\s*(\d+(?:\.\d+)?)$", user_input)
                    elif column == 'expiry_date':
                        # Matches date conditions for expiry_date
                        match = re.match(r"([<>]=?|=?)\s*(\d{4}-\d{2}-\d{2})$", user_input)
                    elif column == 'open_interest':
                        # Matches numeric conditions for open_interest
                        match = re.match(r"([<>]=?|=?)\s*(\d+)$", user_input)
                    elif column == 'volume':
                        # Matches numeric conditions for volume
                        match = re.match(r"([<>]=?|=?)\s*(\d+)$", user_input)
                    elif column == 'bid_price':
                        # Matches decimal conditions for bid_price
                        match = re.match(r"([<>]=?|=?)\s*(\d+(?:\.\d+)?)$", user_input)
                    elif column == 'ask_price':
                        # Matches decimal conditions for ask_price
                        match = re.match(r"([<>]=?|=?)\s*(\d+(?:\.\d+)?)$", user_input)
                    elif column == 'theta':
                        # Matches decimal conditions for theta
                        match = re.match(r"([<>]=?|=?)\s*(\d+(?:\.\d+)?)$", user_input)
                    elif column == 'gamma':
                        # Matches decimal conditions for gamma
                        match = re.match(r"([<>]=?|=?)\s*(\d+(?:\.\d+)?)$", user_input)
                    elif column == 'open_interest_change':
                        # Matches numeric conditions which can be negative for open_interest_change
                        match = re.match(r"([<>]=?|=?)\s*(-?\d+)$", user_input)

                    if match:
                        operator, value = match.groups()
                        if column == 'expiry_date':
                            condition = f"{column} {operator} '{value}'"  # Dates need to be wrapped in quotes
                        else:
                            condition = f"{column} {operator} {value}"
                        conditions.append(condition)
                    else:
                        # If the input doesn't match the pattern, send an error message
                        await inter.response.send_message(f"Invalid format for {column}: {user_input}")
                        return
            # Combine all conditions with ' AND '
            condition_str = ' AND '.join(conditions)

            # Construct the query
            selected_fields = ', '.join(self.selected_columns)
            query = f"SELECT symbol, strike_price, call_put, expiry_date FROM options_data WHERE symbol = '{self.ticker}' AND {condition_str} LIMIT 25;"
            print(f"DEBUG QUERY: {query}")
            print(f"CONDITIONS STR DEBUG: {condition_str}")
            print(f"SELECTED FIELDS DEBUG: **{selected_fields}**")

            try:
                data = await options.fetch(query)
                if data:
                    df = pd.DataFrame(data, columns=['sym', 'strike', 'c/p', 'exp'])
                    table = tabulate(df, headers='keys', tablefmt='fancy', showindex=False)
                    embed = disnake.Embed(title="Database Query Results", description=f"```py\n{table}```", color=disnake.Colour.dark_orange())
                    embed.set_footer(text=f'Query: {query}')
                    await inter.response.send_message(embed=embed)
                else:
                    await inter.response.send_message("No data found for the given conditions.")
            except Exception as e:
                await inter.response.send_message(f"An error occurred: {e}")
            finally:
                await options.close_pool()






# Assuming WebullModal is already defined as shown in your previous message.

class VolumeAnalysisModal(disnake.ui.Modal):
    def __init__(self, ticker:str):
        self.ticker = ticker.upper()
        # Define placeholders for the volume_analysis table fields as needed
        placeholders = {
            'total_trades': "Enter total trades (e.g., '> 500')",
            'total_volume': "Enter total volume (e.g., '> 10000')",
            # Add more placeholders as needed
        }
        components = []
        for field, placeholder in placeholders.items():
            components.append(
                disnake.ui.TextInput(
                    label=field.capitalize().replace('_', ' '),
                    placeholder=placeholder,
                    custom_id=f"condition_{field}",
                    style=disnake.TextInputStyle.short,
                )
            )
        super().__init__(title="Filter Volume Analysis", components=components)

    async def callback(self, inter: disnake.ModalInteraction):
        # ... collect conditions for the volume_analysis table ...
        volume_conditions = []
        for component in self.components:
            user_input = inter.text_values[component.custom_id].strip()
            # Assuming user_input is in the form '<operator> <value>'
            match = re.match(r"([<>]=?|=?)\s*(\d+(?:\.\d+)?)$", user_input)
            if match:
                operator, value = match.groups()
                field = component.custom_id.replace('condition_', '')
                volume_conditions.append(f"{field} {operator} {value}")
            else:
                # If the input doesn't match the pattern, send an error message
                await inter.response.send_message(f"Invalid format for {field}: {user_input}")
                return

        # At this point, volume_conditions should contain all the conditions gathered from the modal.
        # You can now pass them to the function that will execute the JOIN query along with the conditions from the first modal.
        # For demonstration purposes, I'm just printing them here.
        print(volume_conditions)




# Assuming you have a class that handles state, like this:
class QueryState:
    def __init__(self):
        self.options_conditions = None
        self.volume_conditions = None

