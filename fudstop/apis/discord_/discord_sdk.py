import os
from dotenv import load_dotenv
load_dotenv()
from .models.application_commands import ApplicationCommands,Applications, FUDSTOPBotCommands

from .models.search import Attachments, Messages
from . import DiscordDBManager
import time
import psycopg2
from .models.webhooks import Webhooks
import random
import requests
from datetime import datetime
import json
import csv

class DiscordSDK(DiscordDBManager):
    def __init__(self, auth):
        self.auth=auth
        self.headers = { 

                "authority": "discord.com",
                "method": "POST",
                "path": "/api/v9/science",
                "scheme": "https",
                "Accept": "*/*",
                'Content-Type': 'application/json',
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Authorization": self.auth,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

        }
        self.fudstop2 = 1182826541063872522
        """
        A toolkit for working with the discord API.

        Your token is retrieved from the developer tools window. 

        >>> Network tab:

        Type a message in a channel while the dev. tools window is open with the 
        "Network" tab selected and filtered by "FETCH/XHR". 


        >>> URL:

        After typing the message - you'll see a URL pop up. Click on it - and scroll 
        down until you see the "Request Headers" section.
        
        Look for "authorization" and copy the token beside it, and paste it into an .env file as YOUR_DISCORD_HTTP_TOKEN=.
        """

    def get_headers(self):
        """YOU WILL NEED YOUR DISCORD AUTHORIZATION TOKEN FROM THE DEVELOPER TOOLS"""

        
        
        headers = {
                "authority": "discord.com",
                "method": "POST",
                "path": "/api/v9/science",
                "scheme": "https",
                "Accept": "*/*",
                'Content-Type': 'application/json',
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Authorization": self.auth,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

        }
        print(headers)
        return headers
    def connect_to_database(self, user:str='chuck', port:int=5432, password:str='fud', host:str='localhost', database:str='markets'):
        """
        Arguments:

        >>> db_config:

         **PASS IN YOUR POSTGRESQL CONFIG INFO:

         >>> database: the database name
         >>> host: the host (localhost)
         >>> port: the port (5432)
         >>> user: your user
         >>> password: your db password
        """
        # Define database connection parameters
        database = "markets"
        user = "chuck"
        port = 5432
        password = "fud"
        host = "localhost"

        # Create a database connection using psycopg2
        try:
            connection = psycopg2.connect(
                database=database,
                user=user,
                port=port,
                password=password,
                host=host
            )
            return connection
        except psycopg2.Error as e:
            print("Error connecting to the database:", e)
            return None
    def get_roles(self, guild_id, role_id):
        """Gets role information for a guild. Use role_counts to get the role IDs."""
        url=f"https://discord.com/api/v9/guilds/{guild_id}/roles/{role_id}"
        r = requests.patch(url, headers=self.headers).json()

        return r
    def search(self, guild_id, author_id:str='375862240601047070', has:str='file'):
        """Searches discord message history
        
        >>> author_id: the id of the author to search

        >>> has: file, embed, link, mentions, image..
        """
        offset = 0
        while True:
            time.sleep(5)
            offset = offset + 25
            url=f"https://discord.com/api/v9/guilds/{guild_id}/messages/search?author_id={author_id}&has={has}&offset={offset}"
            r = requests.get(url, headers=self.headers).json()


            messages = Messages(r)


            attachments = messages.attachments
            attachments = [item for sublist in attachments for item in sublist]

            attachments = Attachments(attachments)

            # Assuming attachments.filename is your list of filenames
            ogg_filenames = [file for file in attachments.filename if file.endswith('.ogg')]
        
            # Now, create or modify the DataFrame
            # If attachments.as_dataframe() provides the full DataFrame, filter it using the ogg_filenames list
            if hasattr(attachments, 'as_dataframe'):
                full_df = attachments.as_dataframe
                # Filter the DataFrame to include only rows with .ogg filenames
                ogg_df = full_df[full_df['filename'].isin(ogg_filenames)]

                for i,row in ogg_df.iterrows():
                    url = row['proxy_url']

                    yield url

            


    def sanitize_filename(self, filename):
        return "".join([c for c in filename if c.isalpha() or c.isdigit() or c in (' ', '.', '_', '-')]).rstrip()

    def get_unique_filename(self, set1, set2, directory):
        max_attempts = 10
        for _ in range(max_attempts):
            current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            random_part = f"{random.choice(set1)}f{random.choice(set2)}"
            filename = f"{current_time}_{random_part}.ogg"
            if not os.path.exists(os.path.join(directory, self.sanitize_filename(filename))):
                return filename
        raise Exception("Failed to generate a unique filename after several attempts")



    def download_voice_messages(self):
        set1 = ['A', 'R', 'G', 'B', 'C', 'Q', 'S', 'T', 'B', 'V']
        set2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        for url in self.search():
            print(f"Processing URL: {url}")

            try:
                response = requests.get(url)

                if response.status_code == 200:
                    directory_path = f"messages/{datetime.datetime.now().strftime('%Y%m%d')}"
                    os.makedirs(directory_path, exist_ok=True)

                    filename = self.get_unique_filename(set1, set2, directory_path)
                    file_path = os.path.join(directory_path, self.sanitize_filename(filename))

                    with open(file_path, 'wb') as file:
                        file.write(response.content)
                    print(f"Downloaded: {file_path}")
                else:
                    print(f"Failed to download from {url}: Status code {response.status_code}")

            except requests.exceptions.RequestException as e:
                print(f"Error downloading from {url}: {e}")
            except Exception as e:
                print(f"Error: {e}")

    def role_counts(self, guild_id):
        url=f"https://discord.com/api/v9/guilds/{guild_id}/roles/member-counts"
        r = requests.get(url, headers=self.headers).json()
        for i in r:
            print(i)

    def create_channel(self, guild_id, name, type='0', channel_description:str=None, with_webhook:bool=False, webhook_name:str=None, permission_overwrites:str=None):
        headers = self.get_headers()

        # Prepare the payload
        payload = {
            "type": type,
            "name": name,
            "description": channel_description,
        }
        
        # Generate permission_overwrites from self.role_dict
        if permission_overwrites is not None:
            permission_overwrites = [{
                "id": role_id,
                "type": 0,
                "deny": "0",
                "allow": "71453223935041"
            } for role_id in self.role_dict.values()]

            payload['permission_overwrites'] = permission_overwrites

        # Make the API request to create the channel
        r = requests.post(f"https://discord.com/api/v9/guilds/{guild_id}/channels", headers=headers, json=payload)
        print(r)
        channel_id = r.json().get('id')

        if with_webhook and channel_id and name is not None:
            webhook_info = self.create_webhook(channel_id=channel_id, name=webhook_name)

            # Assuming create_webhook returns webhook information as a dict with 'url' and other details
            if webhook_info:
                # Write webhook details to CSV
                with open('webhooks.csv', 'a', newline='', encoding='utf-8') as file:
                    csv_writer = csv.writer(file)
                    # Check if file is empty to write headers
                    if file.tell() == 0:
                        csv_writer.writerow(['Channel ID', 'Webhook Name', 'Webhook URL'])
                    csv_writer.writerow([channel_id, webhook_name, webhook_info.get('url')])

                return "Webhook created successfully and saved to CSV."
            else:
                return "Webhook creation failed."
        elif not with_webhook:
            return "Channel created without webhook."
        


    async def application_commands(self):
        """
        >>> Returns: applications_df and commands_df

        >>> usage: applications, commands = application_commands(guild)
        
        """
        url=f"https://discord.com/api/v9/guilds/1182826541063872522/application-commands/1274195708924268584"
        r = requests.get(url, headers=self.headers).json()

        application_commands = r['application_commands']


        return FUDSTOPBotCommands(application_commands)

            

    def get_webhooks(self, guild_id):
        url=f"https://discord.com/api/v9/guilds/{guild_id}/webhooks"
        r = requests.get(url, headers=self.headers).json()


        webhooks = Webhooks(r)


        return webhooks.as_dataframe
    


    def create_webhook(self, channel_id, name):
        """
        Create a webhook in a Discord channel.

        This function sends a POST request to the Discord API to create a webhook
        in the specified channel with the provided name.

        :param channel_id: The ID of the Discord channel where the webhook will be created.
        :param name: The name of the webhook to be created.
        :return: A JSON response containing information about the created webhook.
        """
        url = f"https://discord.com/api/v9/channels/{channel_id}/webhooks"
        payload = {'name': name}
        r = requests.post(url, headers=self.headers, data=json.dumps(payload))
        print(r)
        return r.json()


    def delete_channel(self, channel_id):
        
        url = f"https://discord.com/api/v9/channels/{channel_id}"


        r = requests.delete(url=url, headers=self.headers)


        return r
    
    def create_thread(self, channel_id:str="1193718649404538980", name:str="YOUR THREAD NAME HERE", topic:str='YOUR DESCRIPTION HERE', nsfw:bool=False, invitable:bool=True, locked:bool=False):
        connection = self.connect_to_database()
        self.create_thread_table(connection=connection)
        payload = {"name":name,"type":11,"topic":topic,"bitrate":64000,"user_limit":0,"nsfw":nsfw,"flags":0,"rate_limit_per_user":0,"auto_archive_duration":10080,"locked": locked,"invitable":invitable}
        r = requests.post(f"https://discord.com/api/v9/channels/{channel_id}/threads", headers=self.headers, data=json.dumps(payload)).json()

        thread_id = r.get('id')
        parent_channel = r.get('parent_id')
        name = r.get('name')
        total_messages = r.get('total_messages_sent')
        message_count = r.get('message_count')
        last_message_id = r.get('last_message_id')
        guild = r.get('guild_id')

        # Insert the retrieved attributes into the database
        self.insert_thread(thread_id, parent_channel, name, total_messages, message_count, last_message_id, guild)


        

    def upload_file(self, channel_id, file_path):
        random_id = random.randint(1, 9223372036854775807)
        file_size = os.path.getsize(file_path)
        url=f"https://discord.com/api/v9/channels/{channel_id}/attachments"

        payload = {"files":[{"filename":file_path,"file_size":file_size,"id":random_id,"is_clip":False}]}
        
        r = requests.post(url, headers=self.headers, data=json.dumps(payload)).json()
        
        attachments = r['attachments']

        upload_url = [i.get('upload_url') for i in attachments]

        return upload_url
    

    def create_poll(self, poll_question:str, channel_id:str, tts:bool, answer1, answer2, answer3):
        payload={"mobile_network_type":"unknown","content": "","nonce":channel_id,"tts":tts,"flags":0,"poll":{"question":{"text":poll_question},"answers":[{"poll_media":{"text":answer1}},{"poll_media":{"text":answer2}},{"poll_media":{"text":answer3}}],"allow_multiselect":False,"duration":24,"layout_type":1}}
        endpoint = f"https://discord.com/api/v9/channels/{channel_id}/messages"

        r = requests.post(endpoint, data=json.dumps(payload), headers=self.headers).json()

        print(r)