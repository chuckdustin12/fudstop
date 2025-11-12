import asyncio
import os
import json
import pandas as pd
from typing import List, Dict, Any
from dotenv import load_dotenv

from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from openai import OpenAI

load_dotenv()

opts = PolygonOptions()
client = OpenAI(api_key=os.environ.get('OPENAI_KEY'))

################################################################################
# 1) A "Tool" function that queries the DB, returning data in chunks
################################################################################
async def fetch_from_db(table_name: str,
                        filter_conditions: str = "",
                        order_clause: str = "",
                        limit_clause: str = "",
                        chunk_size: int = 2) -> List[List[Dict[str, Any]]]:
    """
    A function (tool) that:
      1. Fetches the list of columns for a given table.
      2. Dynamically creates a SELECT query using any filter/order/limit logic.
      3. Retrieves the query results from the DB.
      4. Splits the results into 'chunk_size' chunks and returns them as a list of lists.
    """
    await opts.connect()
    # Example: columns for specified table
    columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
    columns_results = await opts.fetch(columns_query)  # returns rows of columns
    columns_list = [row['column_name'] for row in columns_results]

    query = f"""SELECT * from {table_name}"""


    print(query)
    # Execute the query
    results = await opts.fetch(query)

    # Convert to a DataFrame, then a list of dicts
    df = pd.DataFrame(results, columns=columns_list)
    records = df.to_dict(orient='records')

    # Split records into chunk_size
    def chunker(lst: List[Dict[str, Any]], size: int):
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    return columns_list, list(chunker(records, chunk_size))


################################################################################
# 2) Define the function schema (the "tool" definition)
################################################################################
fetch_db_tool_definition = {
    "name": "fetch_from_db",
    "description": "Retrieves rows from a specified table with optional filters, ordering, limiting, and chunking.",
    "parameters": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table in the database to query."
            },
            "filter_conditions": {
                "type": "string",
                "description": "Clause that goes after 'WHERE' (omit the word WHERE)."
            },
            "order_clause": {
                "type": "string",
                "description": "Include 'ORDER BY ...' if needed."
            },
            "limit_clause": {
                "type": "string",
                "description": "Include 'LIMIT ...' if needed."
            },
            "chunk_size": {
                "type": "integer",
                "description": "Number of rows per chunk."
            }
        },
        "required": ["table_name"]
    }
}



async def run_conversation(data):



    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": f"{data}"}],
        tools = [{
    "type": "function",
    "function": {
    "name": "fetch_from_db",
    "description": "Retrieves rows from a specified table with optional filters, ordering, limiting, and chunking.",
    "parameters": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table in the database to query."
            },
            "filter_conditions": {
                "type": "string",
                "description": "Clause that goes after 'WHERE' (omit the word WHERE).",
                "default": ""
            },
            "order_clause": {
                "type": "string",
                "description": "Include 'ORDER BY ...' if needed.",
                "default": ""
            },
            "limit_clause": {
                "type": "string",
                "description": "Include 'LIMIT ...' if needed.",
                "default": ""
            },
            "chunk_size": {
                "type": "integer",
                "description": "Number of rows per chunk.",
                "default": 100
            }
        },
        "required": ["table_name"]  # This ensures 'table_name' is mandatory
    }
},}],

        tool_choice="auto",
        max_tokens=290
    )
    print(response)
    available_functions = { 
        'fetch_from_db': fetch_from_db
}
    

    tool_call = response.choices[0].message.tool_calls[0].function
    args = tool_call.arguments
    function_name = tool_call.name
    function_to_call = available_functions[function_name]
    function_args = json.loads(args)
    columns, function_response = await function_to_call(**function_args)
    
    
    second_response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": f"Categorize and explain these. Here's the columns: {columns} {function_response}"}],
        max_tokens=450,
        stream=True
    )
    for chunk in second_response:
        if chunk.choices[0].delta.content is not None:
            print(chunk.choices[0].delta.content, end="")



asyncio.run(run_conversation(data='Describe to me the mnemonics, categorize them, and what they all mean. You have the notes, series_name, and mnemonic columns to utilize.'))