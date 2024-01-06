import os
from dotenv import load_dotenv
import asyncio
load_dotenv()
from discord_webhook import DiscordEmbed, AsyncDiscordWebhook
from datetime import datetime, timedelta

class Embeddings:
    def __init__(self):

        self.fudstop = os.environ.get('fudstop')


        

        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')

        #conditions

        self.accumulation = os.environ.get('accumulation')
        self.fire_sale = os.environ.get('fire_sale')
        self.neutral_zone = os.environ.get('neutral_zone')



    async def send_webhook(self, webhook_url, embed=None):
        """
        Sends webhook to discord when conditions are met.

        Arguments:

        >>> webhook_url: REQUIRED - your discord webhook URL

        >>> embed: OPTIONAL - the embed to go with the feed


        
        """


        webhook = AsyncDiscordWebhook(webhook_url)


        if embed is not None:
            webhook.add_embed(embed)


        await webhook.execute()

    async def volume_analysis_embed(self, condition, webhook_url, data_dict:dict):
        """Sends conditional embeds based on volume analysis
        
        >>> fire_sale, accumulation, neutral_zone
        
        """
        print(data_dict)
        ticker = data_dict.get('ticker')

        embed = DiscordEmbed(title=f"|| {condition} || - {ticker}")

        await self.send_webhook(webhook_url, embed)

        
    async def send_td9_embed(self, timespan, hook, ticker, td9_state):

        webhook = AsyncDiscordWebhook(hook, content=f"<@375862240601047070>")

        embed = DiscordEmbed(title=f"TD9 Signal - {ticker}", description=f"```py\nComponents: The TD9 indicator consists of two main parts: the TD" "Setup and the TD Countdown. Both parts are designed to help traders identify exhaustion points in a trend.```" 

        "```py\nTD Setup: This phase consists of nine consecutive price bars, each closing higher (for a buy setup) or lower (for a sell setup) than" "the close four bars earlier. A buy setup occurs after a downtrend, signaling potential bullish reversal, while a sell setup occurs after an" "uptrend, signaling potential bearish reversal.```"

        "```py\nTD Countdown: Following the completion of the setup, the countdown phase begins, consisting of thirteen price bars. The countdown helps to refine the timing of a potential reversal.```")
        embed.add_embed_field(name=f"Current TD9:", value=f"> **{td9_state}**\n> **{timespan}**")
        embed.set_timestamp()
        # if image_path is not None:
        #     with open(image_path, "rb") as f:
        #         file_content = f.read()
            # webhook.add_file(file=file_content, filename="screenshot.jpg")
        # embed.set_image(url="attachment://screenshot.jpg")
        embed.set_footer(text=f"{ticker} | {timespan} | {td9_state} | data by polygon.io | Implemented by FUDSTOP", icon_url=self.fudstop)
        webhook.add_embed(embed)


        asyncio.create_task(webhook.execute())