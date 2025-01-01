import os
from dotenv import load_dotenv
load_dotenv()
import json
import disnake
from disnake.ext import commands
from fudstop.apis.polygonio.polygon_database import PolygonDatabase
from openai import OpenAI
import pandas as pd
import datetime
class GPT(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot=bot
        self.client = OpenAI(api_key=os.environ.get('YOUR_OPENAI_KEY'))
        self.db = PolygonDatabase(user='chuck', database='law', port=5432, password='fud', host='localhost')

        self.tools = [{
      "type": "function",
    "function": {
      "name": "get_law",
      "description": "Get the texas laws and help the user!",
      "parameters": {
        "type": "object",
        "properties": {
          "term": {"type": "string", "description": "The search term to query via user prompt."},
        },
        "required": ["term"]
      }
    }
  }]


    @commands.slash_command()
    async def gpt(self, inter):
        pass




    @gpt.sub_command()
    async def chat(self, inter:disnake.AppCmdInter, prompt:str):
        await inter.response.defer()
        

        while True:
            # Send the prompt to the GPT model and get a response
            response = self.client.chat.completions.create(
                model="gpt-4-1106-preview",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant. You're a quirky troll who likes to LOLOLOLOLO and LMFAO and ROFL!"},
                    {"role": "user", "content": prompt}
                ]
            )

            # Send the GPT response to the Discord channel
            await inter.send(f"# > {response.choices[0].message.content}")
            
            # Wait for the next message from the user
            message = await self.bot.wait_for('message', check=lambda m: m.author == inter.author)

            # Check if the user wants to stop the conversation
            if message.content.lower() == "stop":
                break


            # Update the prompt with the user's new message
            prompt = message.content
    def serialize_record(self, record):
        return {key: value.isoformat() if isinstance(value, datetime.date) else value
                for key, value in record.items()}
    
    
    async def get_law(self, term):
        import requests
        r = requests.get(f'https://www.fudstop.io/api/law/{term}').json()

        articles = [i.get('article') for i in r if i.get('article') != 'CASE BULLETS']
        notes = [i.get('notes') for i in r]
        parent = [i.get('parent_doc') for i in r]
        rule = [i.get('rule') for i in r]
        text = [i.get('text') for i in r]

        data_dict = { 
            'article': articles[0],
            'notes': notes[0],
            'parent': parent[0],
            'rule': rule[0],
            'text': text[0]
        }

        return data_dict

    @gpt.sub_command()
    async def law(self, inter:disnake.AppCmdInter, *, prompt:str):
        await inter.response.defer()
        await self.db.connect()


 
        messages=[
                    {"role": "system", "content": f"Use relevant keywords and lookup the laws relevant to: {prompt}. NEVER TELL THE USER TO GET LEGAL ADVICE. EVER."},
                    {"role": "system", "content": f"USE THE GET LAW FUNCTION"},
                ]
            

        response = self.client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=messages,
            tools=self.tools,
            max_tokens=750,
            tool_choice="auto",  # auto is default, but we'll be explicit
        )
        response_message = response.choices[0].message
        tool_calls = response_message.tool_calls
        # Step 2: check if the model wanted to call a function
        if tool_calls:
            # Step 3: call the function
            # Note: the JSON response may not always be valid; be sure to handle errors
            available_functions = {
                'get_law': self.get_law


            }

            messages.append(response_message)  # extend conversation with assistant's reply

            # Step 4: send the info for each function call and function response to the model
            for tool_call in tool_calls:
                function_name = tool_call.function.name
                function_to_call = available_functions[function_name]
                function_args = json.loads(tool_call.function.arguments)

                records = await function_to_call(**function_args)


                # Serialize the list of processed records
                serialized_response = json.dumps(records)

                messages.append({
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": function_name,
                    "content": serialized_response,
                })

            second_response = self.client.chat.completions.create(
                model="gpt-3.5-turbo-1106",
                messages=messages,
                max_tokens=1250
            )  # get a new response from the model where it can see the function response
            await inter.edit_original_message(f"```py\n{second_response.choices[0].message.content}```")


    @gpt.sub_command()
    async def vision(self, inter: disnake.AppCmdInter, url:str, instructions:str):
        """View an image, get a description of it."""
        await inter.response.defer()
        response = self.client.chat.completions.create(
        model="gpt-4-vision-preview",
        messages=[
            {
            "role": "user",
            "content": [
                {"type": "text", "text": f"{instructions}"},
                {
                "type": "image_url",
                "image_url": {
                    "url": url,
                },
                },
            ],
            }
        ],
        max_tokens=300,
        )

        await inter.edit_original_message(f"> {response.choices[0].message.content}")


class GPTView(disnake.ui.View):
    def __init__(self):
        super().__init__(timeout=None)




def setup(bot:commands.Bot):
    bot.add_cog(GPT(bot))
    print(f"YEET")