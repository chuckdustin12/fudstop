

from fudstop.apis.discord_.discord_sdk import DiscordSDK

sdk = DiscordSDK()
import json
import requests


headers = sdk.headers







channel = sdk.create_channel(guild_id=sdk.guild_id, name='testy', type='0', channel_description='test',with_webhook=True, webhook_name='TESTICLES')
print(channel)