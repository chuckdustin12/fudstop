import os
from dotenv import load_dotenv
from discord_.discord_sdk import DiscordSDK
load_dotenv()



class DiscordArchitect(DiscordSDK):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)




    
