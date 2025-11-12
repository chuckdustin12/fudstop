import pandas as pd
from .scanner import ScanResults
import requests

class DipTraderSDK:
    def __init__(self):
        """THIS IS NOT ASYNC - IT IS SYNCHRONOUS!"""
        self.config = {}

    def create_stock_filter(
        self,
        # REQUIRED PARAMETERS (with default values)
        current_price_min=0,
        current_price_max=300,
        percent_change_min=-50,
        percent_change_max=50,
        intraday_percent_change_min=-50,
        intraday_percent_change_max=50,
        percent_change_1min_min=-10,
        percent_change_1min_max=10,
        percent_change_5min_min=-20,
        percent_change_5min_max=20,
        float_rotations_min=0,
        float_rotations_max=100,
        volume_min=30000,
        volume_max=100000000,
        volume_1min_min=0,
        volume_1min_max=1000000,
        volume_5min_min=0,
        volume_5min_max=1000000,
        capital_min=0,
        capital_max=500000000,
        capital_1min_min=0,
        capital_1min_max=20000000,
        capital_5min_min=0,
        capital_5min_max=50000000,
        market_cap_min=0,
        market_cap_max=100000,
        float_min=0,
        float_max=500,
        rvol_min=0,
        rvol_max=10,
        rvol_change_min=-100,
        rvol_change_max=500,
        volatility_min=0,
        volatility_max=300,
        atr_min=0,
        atr_max=5,
        range_min=0,
        range_max=100,
        fifty_two_week_range_min=0,
        fifty_two_week_range_max=100,
        spread_min=0,
        spread_max=1,
        bid_size_min=0,
        bid_size_max=100000,
        ask_size_min=0,
        ask_size_max=100000,
        percent_from_vwap_min=-10,
        percent_from_vwap_max=10,
        latest_vwap_cross=30,
        percent_of_average_one_min_volume_min=0,
        percent_of_average_one_min_volume_max=200,
        volume_trend="Any",
        latest_lod=30,
        latest_hod=30,
        is_gapping="Either",
        is_halted="Either",
        order_by="volumeDesc",
        columns=["name", "currentPrice", "percentChange", "oneMinuteVolume", "fiveMinuteVolume",
                 "totalVolume", "totalCapital", "float", "rVol", "volatility"],
        hash_key="object:10986",
    ):
        """
        Dynamically creates a stock filter configuration.
        Required fields always present. Optional fields use default values from payload.
        """

        # Base filter structure
        filter_config = {
            "currentPriceMin": current_price_min,
            "currentPriceMax": current_price_max,
            "percentChangeMin": percent_change_min,
            "percentChangeMax": percent_change_max,
            "intradayPercentChangeMin": intraday_percent_change_min,
            "intradayPercentChangeMax": intraday_percent_change_max,
            "percentChange1MinMin": percent_change_1min_min,
            "percentChange1MinMax": percent_change_1min_max,
            "percentChange5MinMin": percent_change_5min_min,
            "percentChange5MinMax": percent_change_5min_max,
            "floatRotationsMin": float_rotations_min,
            "floatRotationsMax": float_rotations_max,
            "volumeMin": volume_min,
            "volumeMax": volume_max,
            "volume1MinMin": volume_1min_min,
            "volume1MinMax": volume_1min_max,
            "volume5MinMin": volume_5min_min,
            "volume5MinMax": volume_5min_max,
            "capitalMin": capital_min,
            "capitalMax": capital_max,
            "capital1MinMin": capital_1min_min,
            "capital1MinMax": capital_1min_max,
            "capital5MinMin": capital_5min_min,
            "capital5MinMax": capital_5min_max,
            "marketCapMin": market_cap_min,
            "marketCapMax": market_cap_max,
            "floatMin": float_min,
            "floatMax": float_max,
            "rVolMin": rvol_min,
            "rVolMax": rvol_max,
            "rVolChangeMin": rvol_change_min,
            "rVolChangeMax": rvol_change_max,
            "volatilityMin": volatility_min,
            "volatilityMax": volatility_max,
            "atrMin": atr_min,
            "atrMax": atr_max,
            "rangeMin": range_min,
            "rangeMax": range_max,
            "fiftyTwoWeekRangeMin": fifty_two_week_range_min,
            "fiftyTwoWeekRangeMax": fifty_two_week_range_max,
            "spreadMin": spread_min,
            "spreadMax": spread_max,
            "bidSizeMin": bid_size_min,
            "bidSizeMax": bid_size_max,
            "askSizeMin": ask_size_min,
            "askSizeMax": ask_size_max,
            "percentFromVwapMin": percent_from_vwap_min,
            "percentFromVwapMax": percent_from_vwap_max,
            "latestVwapCross": latest_vwap_cross,
            "percentOfAverageOneMinVolumeMin": percent_of_average_one_min_volume_min,
            "percentOfAverageOneMinVolumeMax": percent_of_average_one_min_volume_max,
            "volumeTrend": volume_trend,
            "latestLod": latest_lod,
            "latestHod": latest_hod,
            "isGapping": is_gapping,
            "isHalted": is_halted,
            "orderBy": order_by,
            "columns": columns,
            "$$hashKey": hash_key,
        }

        return {"filter": filter_config}

    def create_strategy(self, **kwargs):
        """
            "currentPriceMin": current_price_min,
            "currentPriceMax": current_price_max,
            "percentChangeMin": percent_change_min,
            "percentChangeMax": percent_change_max,
            "intradayPercentChangeMin": intraday_percent_change_min,
            "intradayPercentChangeMax": intraday_percent_change_max,
            "percentChange1MinMin": percent_change_1min_min,
            "percentChange1MinMax": percent_change_1min_max,
            "percentChange5MinMin": percent_change_5min_min,
            "percentChange5MinMax": percent_change_5min_max,
            "floatRotationsMin": float_rotations_min,
            "floatRotationsMax": float_rotations_max,
            "volumeMin": volume_min,
            "volumeMax": volume_max,
            "volume1MinMin": volume_1min_min,
            "volume1MinMax": volume_1min_max,
            "volume5MinMin": volume_5min_min,
            "volume5MinMax": volume_5min_max,
            "capitalMin": capital_min,
            "capitalMax": capital_max,
            "capital1MinMin": capital_1min_min,
            "capital1MinMax": capital_1min_max,
            "capital5MinMin": capital_5min_min,
            "capital5MinMax": capital_5min_max,
            "marketCapMin": market_cap_min,
            "marketCapMax": market_cap_max,
            "floatMin": float_min,
            "floatMax": float_max,
            "rVolMin": rvol_min,
            "rVolMax": rvol_max,
            "rVolChangeMin": rvol_change_min,
            "rVolChangeMax": rvol_change_max,
            "volatilityMin": volatility_min,
            "volatilityMax": volatility_max,
            "atrMin": atr_min,
            "atrMax": atr_max,
            "rangeMin": range_min,
            "rangeMax": range_max,
            "fiftyTwoWeekRangeMin": fifty_two_week_range_min,
            "fiftyTwoWeekRangeMax": fifty_two_week_range_max,
            "spreadMin": spread_min,
            "spreadMax": spread_max,
            "bidSizeMin": bid_size_min,
            "bidSizeMax": bid_size_max,
            "askSizeMin": ask_size_min,
            "askSizeMax": ask_size_max,
            "percentFromVwapMin": percent_from_vwap_min,
            "percentFromVwapMax": percent_from_vwap_max,
            "latestVwapCross": latest_vwap_cross,
            "percentOfAverageOneMinVolumeMin": percent_of_average_one_min_volume_min,
            "percentOfAverageOneMinVolumeMax": percent_of_average_one_min_volume_max,
            "volumeTrend": volume_trend,
            "latestLod": latest_lod,
            "latestHod": latest_hod,
            "isGapping": is_gapping,
            "isHalted": is_halted,
            "orderBy": order_by,
            "columns": columns,
            "$$hashKey": hash_key,
        """

        filter_config = self.create_stock_filter(**kwargs)

        try:
            response = requests.post("https://www.diptraders.net/getScannerData", json=filter_config)
            response.raise_for_status()
            r = response.json()
        except requests.RequestException as e:
            print(f"Error fetching strategy data: {e}")
            return None

        return ScanResults(r)
  





    async def strategy_intraday_scalping(self):
        # Example: "Intraday Scalping" Strategy
        intraday_scalping = self.create_strategy(
            # Very tight price window for fast moves
            current_price_min=1,
            current_price_max=100,
            # Focus on intraday changes
            intraday_percent_change_min=-10,
            intraday_percent_change_max=10,
            # 1-min and 5-min changes must be noticeable
            percent_change_1min_min=-2,
            percent_change_1min_max=2,
            percent_change_5min_min=-3,
            percent_change_5min_max=3,
            # High volume is good for scalping
            volume_min=500000,
            volume_max=50000000,
            order_by="percentChange1MinDesc"
        )

        print("\nIntraday Scalping Strategy Results:")
        return intraday_scalping

    async def strategy_gap_and_go(self):
        # Example: "Gap & Go" Strategy
        gap_and_go = self.create_strategy(
            # Must be gapping (can be up or down)
            is_gapping="Yes",
            intraday_percent_change_min=-10,
            intraday_percent_change_max=10,
            # Decent daily volume
            volume_min=300000,
            # Positive or negative open
            percent_change_min=-10,
            percent_change_max=15,
            # High relative volume indicates premarket attention
            rvol_min=1,
            order_by="intradayPercentChangeDesc"
        )

        print("\nGap & Go Strategy Results:")
        return gap_and_go


    async def strategy_high_volatility(self):
        high_volatility = self.create_strategy(
            current_price_min=1,
            current_price_max=300,
            # Widen these to catch more
            percent_change_min=-30,
            percent_change_max=30,
            intraday_percent_change_min=-30,
            intraday_percent_change_max=30,
            # Lower min volume if we suspect some small-caps
            volume_min=50000,
            volume_max=100000000,
            # And so on...
            order_by="percentChangeDesc",
            is_gapping="Either"  # Don't force a gap
        )
        return high_volatility



    async def strategy_volume_surge(self):
        volume_surge_strategy = self.create_strategy(
            # Price range wide enough to catch many stocks
            current_price_min=1,
            current_price_max=300,
            
            # Not too tight on % changes
            percent_change_min=-15,
            percent_change_max=15,

            # Focus on strong relative volume
            rvol_min=1.5,  
            rvol_max=10,

            # Slightly loosen intraday constraints
            intraday_percent_change_min=-10,
            intraday_percent_change_max=10,

            # Basic volume constraints
            volume_min=100000,
            volume_max=100000000,

            order_by="rVolDesc",  # Sort by highest relative volume
            is_gapping="Either"
        )

        print("Volume Surge Strategy Results:")
        return volume_surge_strategy


    async def strategy_small_cap_breakout(self):

        small_cap_breakout = self.create_strategy(
            current_price_min=0.50,
            current_price_max=15,
            
            # Keep % changes open to find big moves
            percent_change_min=-20,
            percent_change_max=30,

            # Focus on low market cap
            market_cap_min=0,
            market_cap_max=300000000,   # ~300M

            # Sufficient daily volume for liquidity
            volume_min=50000,
            volume_max=50000000,
            
            order_by="percentChangeDesc",
            is_gapping="Either"
        )

        print("\nSmall Cap Breakout Results:")
        return small_cap_breakout


    async def strategy_intraday_reversal(self):
        intraday_reversal = self.create_strategy(
            current_price_min=2,
            current_price_max=100,

            # Big intraday swings
            intraday_percent_change_min=-15,
            intraday_percent_change_max=15,

            # Some 5-min volatility
            percent_change_5min_min=-3,
            percent_change_5min_max=3,

            # Enough volume to be tradeable
            volume_min=300000,
            volume_max=100000000,

            order_by="intradayPercentChangeDesc",
            is_gapping="Either"
        )

        print("\nIntraday Reversal Results:")
        return intraday_reversal


    async def strategy_dip_buy_potential(self):

        dip_buy_potential = self.create_strategy(
            current_price_min=1,
            current_price_max=200,

            # Looking for stocks that have dipped
            percent_change_min=-10,
            percent_change_max=0,  

            # A small 1-min or 5-min bounce
            percent_change_1min_min=0,
            percent_change_1min_max=3,
            percent_change_5min_min=-1,
            percent_change_5min_max=3,

            volume_min=100000,
            order_by="percentChange1MinDesc",
            is_gapping="Either"
        )

        print("\nDip Buy Potential Results:")
        return dip_buy_potential





    async def strategy_short_squeeze_radar(self):
        short_squeeze_radar = self.create_strategy(
            current_price_min=1,
            current_price_max=100,

            float_min=0,
            float_max=100,

            rvol_min=1,
            rvol_max=10,

            percent_change_min=-5,
            percent_change_max=15,

            volume_min=300000,
            order_by="rVolDesc",
            is_gapping="Either"
        )

        return short_squeeze_radar
    

    async def strategy_oversold_rebound(self):

        oversold_rebound = self.create_strategy(
            current_price_min=1,
            current_price_max=100,

            # Down on the day
            percent_change_min=-15,
            percent_change_max=-2,

            # Slight sign of bounce in last 5 min
            percent_change_5min_min=0,
            percent_change_5min_max=3,

            rvol_min=1,  # We want some volume
            volume_min=50000,
            volume_max=50000000,
            order_by="percentChange5MinDesc",
            is_gapping="Either"
        )

        print("\nOversold Rebound Results:")
        return oversold_rebound
