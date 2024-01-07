import asyncio
import os
from dotenv import load_dotenv
load_dotenv()
from discord_webhook import DiscordEmbed, AsyncDiscordWebhook


class WebhookFeeds:
    def __init__(self):
        self.embed = DiscordEmbed()



    async def million_dollar_trades(message, conditions, exchange, dollar_cost):

        """Alerts discord when a trade size of $1,000,000 or more is captured via 
        EquityTrade Feed
        
        
        """





        if dollar_cost >= 1000000:
            print(message)
            hook = AsyncDiscordWebhook(os.environ.get('MILLION_DOLLAR_TRADE'), content=f"> $1,000,000 + trade | {message.symbol} | Price: ${message.price} | Size: {message.size} | Time: {timestamp}")


            embed = DiscordEmbed(title=f">> $1,000,000 Dollar Trade - {message.symbol}", description=f"# > **{message.symbol}** just traded for a dollar value of ${format_large_number(dollar_cost)}", content=f"> # $100k+ trade | {message.symbol} | Price: ${message.price} | Size: {message.size}\n# > Exchange: **{message.exchange}**")
            embed.add_embed_field(name=f"Price & Size:", value=f"# > ${message.price}\n# > {message.size}")
            embed.add_embed_field(name=f"Conditions:", value=f"# > **{conditions}**")
            embed.add_embed_field(name=f"Exchange:", value=f"# > **{exchange}")




            await hook.execute()