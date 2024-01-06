from dataclasses import dataclass, fields
from typing import List, Optional

from discord_webhook import DiscordEmbed


@dataclass
class Day:
    change: float
    change_percent: float
    close: float
    high: float
    last_updated: int
    low: float
    open: float
    previous_close: float
    volume: int
    vwap: float

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class Details:
    contract_type: str
    exercise_style: str
    expiration_date: str
    shares_per_contract: int
    strike_price: float
    ticker: str

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class Greeks:
    delta: float
    gamma: float
    theta: float
    vega: float

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class LastQuote:
    ask: float
    ask_size: int
    bid: float
    bid_size: int
    last_updated: int
    midpoint: float
    timeframe: Optional[str] = None  # adding this new field

    @classmethod
    def from_dict(cls, data: dict) -> 'LastQuote':
        return cls(
            ask=data.get('ask'),
            ask_size=data.get('ask_size'),
            bid=data.get('bid'),
            bid_size=data.get('bid_size'),
            last_updated=data.get('last_updated'),
            midpoint=data.get('midpoint'),
            timeframe=data.get('timeframe')  # account for the new field here too
        )


@dataclass
class LastTrade:
    conditions: List[int]
    exchange: int
    price: float
    sip_timestamp: int
    size: int
    timeframe: str

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class UnderlyingAsset:
    ticker: Optional[str] = None
    value: Optional[str] = None
    change_to_break_even: Optional[str] = None
    price: Optional[str] = None

    @classmethod
    def from_dict(cls, data):
        # Extract only the keys that are defined in the dataclass
        keys = {f.name for f in fields(cls)}
        data = {k: v for k, v in data.items() if k in keys}
        return cls(**data)



@dataclass
class OptionsSnapshotResult:
    break_even_price: Optional[float] = None
    day: Optional[Day] = None
    details: Optional[Details] = None
    greeks: Optional[Greeks] = None
    ticker: Optional[str] = None
    implied_volatility: Optional[float] = None
    last_quote: Optional[LastQuote] = None
    last_trade: Optional[LastTrade] = None
    open_interest: Optional[int] = None
    underlying_asset: Optional[UnderlyingAsset] = None

    @classmethod
    def from_dict(cls, data: dict) -> 'OptionsSnapshotResult':
        # Extract only the keys that are defined in the dataclass
        keys = {f.name for f in fields(cls)}
        data = {k: v for k, v in data.items() if k in keys}
        
        return cls(
            break_even_price=data.get('break_even_price'),
            day=Day.from_dict(data.get('day')) if data.get('day') else None,
            details=Details.from_dict(data.get('details')) if data.get('details') else None,
            greeks=Greeks.from_dict(data.get('greeks')) if data.get('greeks') else None,
            implied_volatility=data.get('implied_volatility'),
            last_quote=LastQuote.from_dict(data.get('last_quote')) if data.get('last_quote') else None,
            last_trade=LastTrade.from_dict(data.get('last_trade')) if data.get('last_trade') else None,
            open_interest=data.get('open_interest'),
            underlying_asset=UnderlyingAsset.from_dict(data.get('underlying_asset')) if data.get('underlying_asset') else None
        )

    @classmethod
    def from_list(cls, data: List[dict]) -> List['OptionsSnapshotResult']:
        return [cls.from_dict(item) for item in data]
    

    def to_embed(self) -> DiscordEmbed:
        """Create a Discord embed from this article."""
        embed = DiscordEmbed(title=f"Ticker 📰 News", description=f"```py\nOption snapshot result - {self.details.ticker}```")
        # Adding author as a field
        if self.day is not None:
            if self.day.change and self.day.change_percent and self.day.close and self.day.high and self.day.low and self.day.open and self.day.vwap and self.day.previous_close:
                embed.add_embed_field(name="Day Stats:", value=f"> Open: **${self.day.open}**\n> High: **${self.day.high}**\n> Last: **${self.day.close}**\n> Low: **${self.day.low}\n> VWAP: **${self.day.vwap}**\n> Prev. Close: **${self.day.previous_close}**", inline=False)
        
        # Adding publisher name as a field
        if self.details is not None:
            if self.details.contract_type and self.details.exercise_style and self.details.expiration_date and self.details.strike_price and self.details.ticker:
                embed.add_embed_field(name="Option Details:", value=f"> Type: **{self.details.contract_type}**\n> Strike: **${self.details.strike_price}**\n> Expiry: **{self.details.expiration_date}**\n> Style: **{self.details.exercise_style}**\n> Sym: **{self.details.ticker}**")

        
        # Adding published date as a field
        if self.greeks is not None:
            if self.greeks.vega and self.greeks.gamma and self.greeks.delta and self.greeks.theta:
                embed.add_embed_field(name="Greeks:", value=f"> Delta: **{self.greeks.delta}**\n> Gamma: **{self.greeks.gamma}**\n> Vega: **{self.greeks.vega}**\n> Theta: **{self.greeks.theta}**")
        
        if self.last_quote is not None:
            if self.last_quote.ask and self.last_quote.bid and self.last_quote.bid_size and self.last_quote.ask_size and self.last_quote.midpoint:
                embed.add_embed_field(name=f"Last Quote:", value=f"> Bid: **${self.last_quote.bid}**\n> Bid Size: **{self.last_quote.bid_size}**\n> Mid: **${self.last_quote.midpoint}**\n> Ask: **${self.last_quote.ask}**\n> Ask Size: **{self.last_quote.ask_size}**")
        
        # Adding image to embed
        if self.last_trade is not None:
            if self.last_trade.conditions and self.last_trade.exchange and self.last_trade.price and self.last_trade.sip_timestamp and self.last_trade.size:

                embed.add_embed_field(name=f"Last Trade:", value=f"> Size: **{self.last_trade.size}**\n> Price: **${self.last_trade.price}**\n> Exchange: **{self.last_trade.exchange}**\n> Conditions: **{self.last_trade.conditions}**\n> Timestamp: **{self.last_trade.sip_timestamp}**")
        
        if self.underlying_asset is not None:
            if self.underlying_asset.change_to_break_even and self.underlying_asset.ticker:
                value_or_price = self.underlying_asset.value if self.underlying_asset.value else self.underlying_asset.price
                embed.add_embed_field(
                    name="Underlying:",
                    value=f"> Value/Price: **${value_or_price}**\n"
                        f"> Change to Break Even: **{self.underlying_asset.change_to_break_even}**\n"
                        f"> Ticker: **{self.underlying_asset.ticker}**"
                )

        return embed
    