import os
import requests
import datetime
import random
from .models.search import Messages, Attachments
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


        self.token = os.environ.get('YOUR_DISCORD_HTTP_TOKEN')
        self.headers = {
            'Authorization': f'{self.token}',
            'Accept': "*/*",
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

        }


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
