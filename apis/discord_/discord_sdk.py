import os
import requests
import json
import datetime
import random
from .models.search import Messages, Attachments
from .models.application_commands import Applications, ApplicationCommands
from .models.webhooks import Webhooks
from dotenv import load_dotenv
load_dotenv()
import time

class DiscordSDK:
    def __init__(self):
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
        self.channel_types = { 

            'category': '4',
            
        }

        self.member_permissions = '71453223935041'

        self.role_dict = {
            'LIFETIME MEMBER': 1002249878283493456,
            'youtube support': 1086118401660964914,
            'youtube level1': 1086118401660964916,
            'youtube level2': 1086118401660964917,
            'patreon level1': 896207245853999145,
            'patreon level2': 941029523699400705,
            'patreon level3': 938824920589283348,
            'patreon last': 1145204112481321040

        }
       
        self.guild_id = 888488311927242753 # replace with your guild ID
        self.token = os.environ.get('YOUR_DISCORD_HTTP_TOKEN')
        self.headers = {
            'Authorization': f'{self.token}',
            'Accept': "*/*",
            "Content-Type": "application/json",
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

        }

    def get_roles(self, guild_id, role_id):
        """Gets role information for a guild. Use role_counts to get the role IDs."""
        url=f"https://discord.com/api/v9/guilds/{guild_id}/roles/{role_id}"
        r = requests.patch(url, headers=self.headers).json()

        return r
    def search(self, author_id:str='375862240601047070', has:str='file'):
        """Searches discord message history
        
        >>> author_id: the id of the author to search

        >>> has: file, embed, link, mentions, image..
        """
        offset = 0
        while True:
            time.sleep(5)
            offset = offset + 25
            url=f"https://discord.com/api/v9/guilds/888488311927242753/messages/search?author_id={author_id}&has={has}&offset={offset}"
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

    def create_channel(self, guild_id, name, type='4', channel_description:str=None, with_webhook:bool=False, webhook_name:str=None):
        """
        Create a channel in a Discord guild.
        
        Args:
            guild_id (str): The ID of the guild where the channel will be created.
            name (str): The name of the channel.
            type (str): The type of channel ('0' for text, '4' for category). Defaults to '4'.
        
        Returns:
            Response from the Discord API.
        """

        # Generate permission_overwrites from self.role_dict
        permission_overwrites = [{
            "id": role_id,
            "type": 0,
            "deny": "0",
            "allow": "71453223935041"
        } for role_id in self.role_dict.values()]

        # Prepare the payload
        payload = {
            "type": type,
            "name": name,
            "description": channel_description,
            "permission_overwrites": permission_overwrites
        }

        # Make the API request to create the channel
        r = requests.post(f"https://discord.com/api/v9/guilds/{guild_id}/channels", headers=self.headers, data=json.dumps(payload))

        id = r.json()['id']


        if with_webhook != False and name is not None:
            self.create_webhook(channel_id=id, name=webhook_name)

        


    def application_commands(self, guild):
        """
        >>> Returns: applications_df and commands_df

        >>> usage: applications, commands = application_commands(guild)
        
        """
        url=f"https://discord.com/api/v9/guilds/{guild}/application-command-index"
        r = requests.get(url, headers=self.headers).json()
        applications = Applications(r['applications'])
        application_commands = r['application_commands']
        commands = ApplicationCommands(application_commands)

        applications_df = applications.applications_as_dataframe
        commands_df = commands.as_dataframe



        return applications_df, commands_df


    def get_webhooks(self, guild_id):
        url=f"https://discord.com/api/v9/guilds/{guild_id}/webhooks"
        r = requests.get(url, headers=self.headers).json()


        webhooks = Webhooks(r)


        return webhooks.as_dataframe
    


    def create_webhook(self, channel_id, name):
        url=f"https://discord.com/api/v9/channels/{channel_id}/webhooks"
        payload = {'name': name}
        r = requests.post(url, headers=self.headers, data=json.dumps(payload))


        return r.json()

