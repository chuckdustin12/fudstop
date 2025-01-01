from discord_webhook import DiscordEmbed,AsyncDiscordWebhook
import asyncio
class OptionDataAnalyzer:
    def __init__(self):
        pass



    async def build_embed(self, hook, description,oi, vol, vol_oi, iv, ask, bid, mid, strike, expiry, call_put, underlying_symbol, underlying_price, gamma, gamma_risk, delta, delta_theta, theta, theta_decay, vega, vega_impact, change_percent, close, open, high, low, vwap, ex_value, in_value, iv_percentile, moneyness, velocity, profit_potential, spread, spread_pct, time_value, trade_exchange, trade_price, trade_size, color):

        webhook = AsyncDiscordWebhook(hook, content=f"<@375862240601047070>")
        embed = DiscordEmbed(title=f"${underlying_price} {underlying_symbol} | {strike} | {call_put} | {expiry}", description=f"# > {underlying_symbol}:\n```py\n{description}```", color=color)
        embed.add_embed_field(name=f"Contract Stats:", value=f"> OPEN: **${open}**\n> HIGH: **${high}**\n> LOW: **${low}**\n> CLOSE: **${close}**\n> VWAP: **${vwap}**\n> CHANGE%: **{change_percent}**")
        embed.add_embed_field(name=f"OI / VOl", value=f"> OI: **{oi}**\n> VOL: **{vol}**\n> RATIO: **{vol_oi}**")
        embed.add_embed_field(name=f"IV:", value=f"> **{iv}**\n> Percentile: **{iv_percentile}**")
        embed.add_embed_field(name=f"VALUE:", value=f"> Intrinsic: **{in_value}**\n> Extrinsic: **{ex_value}**\n> Time Value: **${time_value}**")
        embed.add_embed_field(name=f"GREEKS:", value=f"> Delta: **{delta}**\n> Delta/Theta Ratio: **{delta_theta}**\n> Gamma: **{gamma}**\n> Gamma Risk: **{gamma_risk}**\n> Vega: **{vega}**\n> Vega Impact: **{vega_impact}**\n> Theta: **{theta}**\n> Decay Rate: **{theta_decay}**", inline=False)
        embed.add_embed_field(name=f"Spread:", value=f"> Bid: **${bid}**\n> Mid: **${mid}**\n> Ask: **{ask}**\n> Spread: **{spread}**\n> Spread PCT: **{spread_pct}%**")
        embed.add_embed_field(name=f"Last Trade", value=f"> Size: **{trade_size}**\n> Price: **{trade_price}**\n> Exchange: **{trade_exchange}**")
        embed.add_embed_field(name="Details:", value=f"> Moneyness: **{moneyness}**\n> Velocity: **{velocity}**\n> Profit Potential: **{profit_potential}**")
        

        await self.send_hook(webhook, embed)

    async def send_hook(self, hook: AsyncDiscordWebhook, embed: DiscordEmbed):

        hook.add_embed(embed)
        embed.set_timestamp()
        

        await hook.execute()

