from openai import OpenAI
import os
from dotenv import load_dotenv
load_dotenv()
import asyncio
from fudstop4.apis.helpers import is_etf
from datetime import date, datetime
from fudstop_tools import fudstop_tools
from fudstop4.apis.webull.webull_trading import WebullTrading
trading = WebullTrading()
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from fudstop4.apis.master.master_sdk import MasterSDK
master = MasterSDK()
opts =WebullOptions()
key = os.environ.get('OPENAI_KEY')
import json
client = OpenAI(api_key=key)
tools = [{
    "type": "function",
    "function": {
        "name": "balance_sheet",
        "description": "Get the balance sheet for a ticker.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "The ticker to query."
                }
            },
            "required": [
                "ticker"
            ],
            "additionalProperties": False
        },
        "strict": True
    },
    "function": {
        "name": "income_statement",
        "description": "Get the income statement info for a ticker.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "The ticker to query."
                }
            },
            "required": [
                "ticker"
            ],
            "additionalProperties": False
        },
        "strict": True
    }
}]
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # Let the base class default method raise the TypeError
        return super().default(obj)

def custom_json_dumps(data):
    return json.dumps(data, cls=CustomJSONEncoder)
async def run_conversation(query):
    # Step 1: send the conversation and available functions to the model
    messages = [{
        "role": "user",
        "content": f"Call the function and go over the results as the options master: {query}"
    }]
    
    response = master.client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        tools=fudstop_tools,
        tool_choice="auto",
    )

    response_message = response.choices[0].message
    tool_calls = response_message.tool_calls

    if tool_calls:
        available_functions = master.available_functions
        messages.append(response_message)

        for tool_call in tool_calls:
            function_name = tool_call.function.name
            print(f"[CALLING FUNCTION] → {function_name}")
            function_to_call = available_functions[function_name]
            function_args = json.loads(tool_call.function.arguments)

            records = await function_to_call(**function_args)
            processed_records = [master.serialize_record(record) for record in records]
            serialized_response = custom_json_dumps(processed_records)

            messages.append({
                "tool_call_id": tool_call.id,
                "role": "tool",
                "name": function_name,
                "content": serialized_response,
            })

        # Step 5: Let GPT handle and present the results
        second_response = master.client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
        )
        return second_response

# Entry point
if __name__ == "__main__":
    query = input("🔎 Enter your query: ").strip()
    ai = asyncio.run(run_conversation(query=query))
    AI_RESPONSE = ai.choices[0].message.content
    print(f"\n📣 GPT RESPONSE:\n{AI_RESPONSE}")