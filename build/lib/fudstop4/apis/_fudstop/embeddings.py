from fudstop_middleware.imps import *


async def stock_snapshots_embed_webhook(ticker, prev_open, prev_high, prev_low, prev_close, prev_volume, prev_vwap, open, high, low, close, volume, vwap, change, change_pct, min_open, min_high, min_low, min_close, min_volume, min_vwap, min_trades, min_timestamp, ask, ask_size, bid, bid_size, last_tradeprice, last_tradesize, last_tradetime, last_tradeconditions, last_tradeexchange, webhook_url):

    hook = AsyncDiscordWebhook(webhook_url)


    if close > open:
        color = hex_color_dict.get('green')
    elif close < open:
        color = hex_color_dict.get('red')

    else:
        color = hex_color_dict.get('grey')


    embed = DiscordEmbed(title=f'Stock Snapshot - {ticker}', description=f"```py\nViewing real-time stock snapshot of {ticker}.```\n> Last Trade Info:\n\n- Price: **${last_tradeprice}**\n- Size: **{last_tradesize}**\n- Exchange: **{STOCK_EXCHANGES.get(last_tradeexchange)}**\n- Conditions: **{last_tradeconditions}**\n- Time: **{last_tradetime}**\n\n> Last Quote Info:\n\n- Bid: **${bid}**\n> BidSize: **{bid_size}**\n> Ask: **{ask}**\n> AskSize: **{ask_size}**", color=color)
    embed.add_embed_field(name=f"Day Stats:", value=f"> Open: **${open}**\n> High: **${high}**\n> Low: **${low}**\n> Now: **${close}**\n> Vol: **{volume}**\n> VWAP: **${vwap}**")
    embed.add_embed_field(name=f"Prev Day:", value=f"> pOpen: **${prev_open}**\n> pHigh: **${prev_high}**\n> pLow: **${prev_low}**\n> pClose: **${prev_close}**\n> pVol: **{prev_volume}**\n> pVWAP: **${prev_vwap}**")
    embed.add_embed_field(name=f"Min Stats:", value=f"> mOpen: **${min_open}**\n> mHigh: **${min_high}**\n> mLow: **${min_low}**\n> mClose: **${min_close}**\n> mVol: **{min_volume}**\n> mVWAP: **${min_vwap}**\n> mTime: **{min_timestamp}**\n> mTrades: **{min_trades}**")
    embed.add_embed_field(name=f"Performance:", value=f"> Change: **{change}**\n> ChangePct: **{change_pct}**")
    embed.set_timestamp()
    embed.set_footer(text=f'Implemented by FUDSTOP - Stock Snapshot - {ticker}')

    hook.add_embed(embed)

    await hook.execute()


async def options_snapshots_embed_webhook(
    break_even, iv, oi, volume, high, low, vwap, open, close, change_percent,
    strike, expiry, call_put, exercise_style, option_symbol, theta, delta, gamma, vega,
    timestamp, conditions, price, trade_size, exchange, ask, bid, bid_size, ask_size, mid,
    change_to_breakeven, underlying_price, ticker, dte, moneyness, liquidity_score, spread,
    intrinsic_value, extrinsic_value, leverage_ratio, spread_pct, return_on_risk, velocity,
    gamma_risk, theta_decay_rate, vega_impact, delta_theta_ratio, sensitivity, contract_type, webhook_url
):

    hook = AsyncDiscordWebhook(webhook_url)

    if contract_type == 'call':
        color = hex_color_dict.get('green')
    elif contract_type == 'put':
        color = hex_color_dict.get('red')


    embed = DiscordEmbed(
        title=f"Options Snapshot - {ticker} {option_symbol}",
        description=(
            f"```py\nViewing real-time options snapshot of {ticker} - {option_symbol}.```"
            f"\n> Last Trade Info:\n\n"
            f"- Price: **${price}**\n"
            f"- Size: **{trade_size}**\n"
            f"- Exchange: **{exchange}**\n"
            f"- Conditions: **{conditions}**\n"
            f"- Time: **{timestamp}**\n\n"
            f"> Last Quote Info:\n\n"
            f"- Bid: **${bid}** (Size: **{bid_size}**)\n"
            f"- Ask: **${ask}** (Size: **{ask_size}**)\n"
            f"- Mid: **${mid}**"
        ),
        color=color
    )

    embed.add_embed_field(
        name="Option Details:",
        value=(
            f"> Strike: **${strike}**\n"
            f"> Expiry: **{expiry}**\n"
            f"> Type: **{call_put}**\n"
            f"> Exercise: **{exercise_style}**\n"
            f"> Underlying Price: **${underlying_price}**\n"
            f"> Ticker: **{ticker}**"
        )
    )

    embed.add_embed_field(
        name="Price Stats:",
        value=(
            f"> Open: **${open}**\n"
            f"> High: **${high}**\n"
            f"> Low: **${low}**\n"
            f"> Now: **${close}**\n"
            f"> Vol: **{volume}**\n"
            f"> VWAP: **${vwap}**"
        )
    )

    embed.add_embed_field(
        name="Greeks:",
        value=(
            f"> Delta: **{round(delta,2)}**\n"
            f"> Gamma: **{round(gamma,2)}**\n"
            f"> Theta: **{round(theta,2)}**\n"
            f"> Vega: **{round(vega,2)}**\n"
            f"> IV: **{round(iv,2)}**"
        )
    )

    embed.add_embed_field(
        name="Market Data:",
        value=(
            f"> Trade Price: **${price}**\n"
            f"> Trade Size: **{trade_size}**\n"
            f"> Change%: **{change_percent}**\n"
            f"> OI: **{oi}**\n"
            f"> Break Even: **{break_even}**\n"
            f"> Change to Breakeven: **{change_to_breakeven}**"
        )
    )

    embed.add_embed_field(
        name="Additional Metrics:",
        value=(
            f"> DTE: **{dte}**\n"
            f"> Moneyness: **{moneyness}**\n"
            f"> Liquidity: **{liquidity_score}**\n"
            f"> Spread: **{spread}** (Pct: **{spread_pct}**)\n"
            f"> Intrinsic: **{intrinsic_value}**\n"
            f"> Extrinsic: **{extrinsic_value}**\n"
            f"> Leverage: **{leverage_ratio}**\n"
            f"> Return on Risk: **{return_on_risk}**"
        )
    )

    embed.add_embed_field(
        name="Risk Metrics:",
        value=(
            f"> Velocity: **{velocity}**\n"
            f"> Gamma Risk: **{gamma_risk}**\n"
            f"> Theta Decay: **{theta_decay_rate}**\n"
            f"> Vega Impact: **{vega_impact}**\n"
            f"> Delta/Theta Ratio: **{delta_theta_ratio}**\n"
            f"> Sensitivity: **{sensitivity}**"
        )
    )

    embed.set_timestamp()
    embed.set_footer(text=f'Implemented by FUDSTOP - Options Snapshot - {option_symbol}')

    hook.add_embed(embed)
    await hook.execute()