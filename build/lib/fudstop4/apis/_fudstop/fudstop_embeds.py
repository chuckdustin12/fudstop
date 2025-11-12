from fudstop_middleware.imps import *
import os
import requests
import asyncio
from dotenv import load_dotenv
load_dotenv()

# Wrapper to access snapshot data via attributes
class SingleStockSnapshot:
    def __init__(self, data):
        # data is a list with one dict; extract the first element
        if isinstance(data, list):
            self.data = data[0]
        elif isinstance(data, dict):
            self.data = data
        else:
            raise ValueError("Invalid data format")

    def __getattr__(self, name):
        mapping = {
            'ticker': 'ticker',
            'o': 'open',
            'h': 'high',
            'l': 'low',
            'c': 'close',
            'v': 'volume',
            'vw': 'vwap',
            'trade_price': 'last_tradeprice',
            'trade_size': 'last_tradesize',
            'trade_exchange': 'last_tradeexchange',
            'trade_conditions': 'last_tradeconditions',
            'trade_timestamp': 'last_tadetime',
            'min_o': 'min_open',
            'min_h': 'min_high',
            'min_l': 'min_low',
            'min_c': 'min_close',
            'bid_price': 'bid',
            'bid_size': 'bid_size',
            'ask_price': 'ask',
            'ask_size': 'ask_size',
            'min_v': 'min_volume',
            'min_vw': 'min_vwap',
            'min_trades': 'min_trades',
            # Using min_volume as a placeholder for aggregate minute volume
            'min_av': 'min_volume'
        }
        if name in mapping:
            return self.data.get(mapping[name])
        raise AttributeError(f"'SingleStockSnapshot' object has no attribute '{name}'")

class StockSnapshotEmbedHook(DiscordEmbed):
    def __init__(self, data, ticker):
        snapshot = SingleStockSnapshot(data)
        self.ticker = ticker

        # Determine embed color based on close vs open
        if snapshot.c <= snapshot.o:
            color = hex_color_dict.get('red')
        elif snapshot.c == snapshot.o:
            color = hex_color_dict.get('yellow')
        else:
            color = hex_color_dict.get('green')
        self.color = color

        super().__init__(
            title=f"Stock Snapshot - {self.ticker}",
            description=(
                f"```py\nViewing real-time stock snapshot of {self.ticker}.```"
                f"\n> Last Trade Info:\n\n- Price: **${snapshot.trade_price}**"
                f"\n- Size: **{snapshot.trade_size}**"
                f"\n- Exchange: **{STOCK_EXCHANGES.get(snapshot.trade_exchange)}**"
                f"\n- Conditions: **{snapshot.trade_conditions}**"
                f"\n- Time: **{snapshot.trade_timestamp}**"
                f"\n\n> Last Quote Info:\n\n- Bid: **${snapshot.bid_price}**"
                f"\n- BidSize: **{snapshot.bid_size}**"
                f"\n- Ask: **{snapshot.ask}**"
                f"\n- AskSize: **{snapshot.ask_size}**"
            )
        )

        self.add_embed_field(
            name=f"{self.ticker} Details:",
            value=(
                f"> Open: **${snapshot.o}**"
                f"\n> High: **${snapshot.h}**"
                f"\n> Low: **${snapshot.l}**"
                f"\n> Close: **${snapshot.c}**"
                f"\n> Volume: **{snapshot.v}**"
                f"\n> VWAP: **${snapshot.vw}**"
            )
        )
        self.add_embed_field(
            name="Minute Stats:",
            value=(
                f"> Open: **${snapshot.min_o}**"
                f"\n> High: **${snapshot.min_h}**"
                f"\n> Low: **${snapshot.min_l}**"
                f"\n> Close: **${snapshot.min_c}**"
                f"\n> Volume: **{snapshot.min_v}**"
                f"\n> VWAP: **${snapshot.min_vw}**"
            )
        )
        self.add_embed_field(
            name="Last Minute Trades:",
            value=f"> {snapshot.min_trades} with an aggregate minute volume of **{snapshot.min_av}**."
        )

        self.set_timestamp()
        self.set_footer(text=f"Data provided by Polygon.io | Last updated at {self.timestamp}")

    async def send_stock_embed(self, webhook_url):
        hook = AsyncDiscordWebhook(webhook_url)
        hook.add_embed(self)
        await hook.execute()

if __name__ == '__main__':
    r = requests.get(
        "https://www.fudstop.io/api/stock_snapshot?ticker=AMC",
        headers={'X-API-KEY': 'fudstop4'}
    ).json()
    print(r)
    stocksnap = StockSnapshotEmbedHook(r, 'AMC')
    webhook_url = "https://discord.com/api/webhooks/1207045858105888808/kkXrxa9HwL8DEvnIFoWdcw1W7arOWs0zGn9Ss5MqhUPTm8S_qRL1otXXttxap15mvhe3"
    asyncio.run(stocksnap.send_stock_embed(webhook_url))
