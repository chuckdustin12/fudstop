import pandas as pd
class DealerPositioning:
    def __init__(self, data):
        self.charm_by_date = [i.get('charm_by_date') for i in data]
        self.charm_by_expiration = [i.get('charm_by_expiration') for i in data]
        self.charm_by_strike = [i.get('charm_by_strike') for i in data]
        self.dealer_data_latest = [i.get('dealer_data_latest') for i in data]
        self.delta_by_date = [i.get('delta_by_date') for i in data]
        self.dgex_by_date = [i.get('dgex_by_date') for i in data]
        self.dgex_by_strike = [i.get('dgex_by_strike') for i in data]
        self.gex_by_date = [i.get('gex_by_date') for i in data]
        self.gex_by_strike = [i.get('gex_by_strike') for i in data]
        self.gex_stats = [i.get('gex_stats') for i in data]
        self.ncd_by_date = [i.get('ncd_by_date') for i in data]
        self.npd_by_date = [i.get('npd_by_date') for i in data]
        self.positioning_by_exp_strike = [i.get('positioning_by_exp_strike') for i in data]
        self.positioning_by_expiration = [i.get('positioning_by_expiration') for i in data]
        self.positioning_by_strike = [i.get('positioning_by_strike') for i in data]
        self.skew_dict = [i.get('skew_dict') for i in data]
        self.term_structure = [i.get('term_structure') for i in data]
        self.term_structure_per_strike = [i.get('term_structure_per_strike') for i in data]
        self.term_structures = [i.get('term_structures') for i in data]
        self.vanna_by_date = [i.get('vanna_by_date') for i in data]
        self.vanna_by_expiration = [i.get('vanna_by_expiration') for i in data]
        self.vanna_by_strike = [i.get('vanna_by_strike') for i in data]
        self.vanna_stats = [i.get('vanna_stats') for i in data]
        self.vannagex_by_strike = [i.get('vannagex_by_strike') for i in data]
        self.vol_surfaces = [i.get('vol_surfaces') for i in data]
        self.volsurface_per_exp = [i.get('volsurface_per_exp') for i in data]


        self.data_dict = {
            'charm_by_date': self.charm_by_date,
            'charm_by_expiration': self.charm_by_expiration,
            'charm_by_strike': self.charm_by_strike,
            'dealer_data_latest': self.dealer_data_latest,
            'delta_by_date': self.delta_by_date,
            'dgex_by_date': self.dgex_by_date,
            'dgex_by_strike': self.dgex_by_strike,
            'gex_by_date': self.gex_by_date,
            'gex_by_strike': self.gex_by_strike,
            'gex_stats': self.gex_stats,
            'ncd_by_date': self.ncd_by_date,
            'npd_by_date': self.npd_by_date,
            'positioning_by_exp_strike': self.positioning_by_exp_strike,
            'positioning_by_expiration': self.positioning_by_expiration,
            'positioning_by_strike': self.positioning_by_strike,
            'skew_dict': self.skew_dict,
            'term_structure': self.term_structure,
            'term_structure_per_strike': self.term_structure_per_strike,
            'term_structures': self.term_structures,
            'vanna_by_date': self.vanna_by_date,
            'vanna_by_expiration': self.vanna_by_expiration,
            'vanna_by_strike': self.vanna_by_strike,
            'vanna_stats': self.vanna_stats,
            'vannagex_by_strike': self.vannagex_by_strike,
            'vol_surfaces': self.vol_surfaces,
            'volsurface_per_exp': self.volsurface_per_exp
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class ZeroDteFlow:
    def __init__(self, ticker_data):

        self.call_or_put = [i.get('call_or_put') for i in ticker_data]
        self.delta = [i.get('delta') for i in ticker_data]
        self.gamma = [i.get('gamma') for i in ticker_data]
        self.premium_spent = [i.get('premium_spent') for i in ticker_data]
        self.side = [i.get('side') for i in ticker_data]
        self.stock_price = [i.get('stock_price') for i in ticker_data]
        self.strike_price = [i.get('strike_price') for i in ticker_data]
        self.time = [i.get('time') for i in ticker_data]
        self.volume = [i.get('volume') for i in ticker_data]



        self.data_dict = { 
            'call_put': self.call_or_put,
            'delta': self.delta,
            'gamma': self.gamma,
            'premium_spent': self.premium_spent,
            'side': self.side,
            'stock_price': self.stock_price,
            'strike': self.strike_price,
            'timestamp': self.time,
            'volume': self.volume
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class DarkPoolPrints:
    def __init__(self, data):

        self.dates = [i.get('date_and_minute') for i in data]
        self.open = [i.get('open') for i in data]
        self.volume = [i.get('volume') for i in data]
        self.close = [i.get('close') for i in data]
        self.low = [i.get('low') for i in data]
        self.high = [i.get('high') for i in data]


        self.data_dict = { 
            'dates': self.dates,
            'open': self.open,
            'volume': self.volume,
            'close': self.close,
            'low' : self.low,
            'high': self.high
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class LargestOrders:
    def __init__(self, largest_orders):


        self.ask = [i.get('ask') for i in largest_orders]
        self.bid = [i.get('bid') for i in largest_orders]
        self.call_or_put = [i.get('call_or_put') for i in largest_orders]
        self.expiration_date = [i.get('expiration_date') for i in largest_orders]
        self.oi = [i.get('oi') for i in largest_orders]
        self.premium_spent = [i.get('premium_spent') for i in largest_orders]
        self.side = [i.get('side') for i in largest_orders]
        self.sorting_key = [i.get('sorting_key') for i in largest_orders]
        self.stock_price = [i.get('stock_price') for i in largest_orders]
        self.strike_price = [i.get('strike_price') for i in largest_orders]
        self.symbol = [i.get('symbol') for i in largest_orders]
        self.time = [i.get('time') for i in largest_orders]
        self.today_date = [i.get('today_date') for i in largest_orders]
        self.total_volume = [i.get('total_volume') for i in largest_orders]
        self.vol_over_oi = [i.get('vol_over_oi') for i in largest_orders]
        self.volume = [i.get('volume') for i in largest_orders]


        # Organize all data into a dictionary
        self.data_dict = {
            'ask': self.ask,
            'bid': self.bid,
            'call_or_put': self.call_or_put,
            'expiration_date': self.expiration_date,
            'oi': self.oi,
            'premium_spent': self.premium_spent,
            'side': self.side,
            'sorting_key': self.sorting_key,
            'stock_price': self.stock_price,
            'strike_price': self.strike_price,
            'symbol': self.symbol,
            'time': self.time,
            'today_date': self.today_date,
            'total_volume': self.total_volume,
            'vol_over_oi': self.vol_over_oi,
            'volume': self.volume
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class DailyMarketData:
    def __init__(self, date, oi_call, oi_put, premiums_call, premiums_put, volume_call, volume_put):
        self.date = date
        self.oi_call = oi_call
        self.oi_put = oi_put
        self.premiums_call = premiums_call
        self.premiums_put = premiums_put
        self.volume_call = volume_call
        self.volume_put = volume_put



class OptionsDashboardData:
    def __init__(self, data):

        contract_dominance_dictionary = data.get('contract_dominance_dictionary')
        current_date_to_use = data.get('current_date_to_use')
        daily_heatmap = data.get('daily_heatmap')
        flow_stats = data.get('flow_stats')
        historical_prems_and_volume_dictionary = data.get('historical_prems_and_volume_dictionary')
        largest_orders = data.get('largest_orders')
        oi_aggregate_historical = data.get('oi_aggregate_historical')
        oi_by_strikes = data.get('oi_by_strikes')
        oi_granular_historical = data.get('oi_granular_historical')
        stock_historical_data = data.get('stock_historical_data')
        strikes_dictionary = data.get('strikes_dictionary')


        ticker_iv_data = data.get('ticker_iv_data')


        unusual_contracts = data.get('unusual_contracts')
        weekly_heatmap = data.get('weekly_heatmap')


        
        dominance_df = pd.DataFrame(contract_dominance_dictionary)

        self.dominance_df = dominance_df.drop(columns=[2])
        self.daily_heatmap_df = pd.DataFrame(daily_heatmap)
        self.weekly_heatmap_df = pd.DataFrame(weekly_heatmap)

        self.unusual_options_df = pd.DataFrame(unusual_contracts)

        self.ticker_iv_df = pd.DataFrame(ticker_iv_data)
        self.stock_historical_df = pd.DataFrame(stock_historical_data)

        self.oi_by_strikes_df = pd.DataFrame(oi_by_strikes)
        self.largest_orders_df = pd.DataFrame(largest_orders)
        self.historical_prems_and_volume_df = pd.DataFrame(historical_prems_and_volume_dictionary)
        flow_stats_df = pd.DataFrame(flow_stats)
        self.flow_stats_df = flow_stats_df.transpose()



        self.all_dfs = pd.concat([
            self.dominance_df, 
            self.daily_heatmap_df, 
            self.weekly_heatmap_df, 
            self.unusual_options_df, 
            self.ticker_iv_df, 
            self.stock_historical_df, 
            self.oi_by_strikes_df, 
            self.largest_orders_df, 
            self.historical_prems_and_volume_df, 
            self.flow_stats_df
        ], axis=0, ignore_index=True)




class TickerDarkPoolLevels:
    def __init__(self, data, ticker, period, num_periods):
        self.price = [i.get('price') for i in data]
        self.side = [i.get('side') for i in data]
        self.time = [i.get('time') for i in data]
        self.value = [i.get('value') for i in data]

        self.data_dict = { 
            'ticker': ticker,
            'price': self.price,
            'side': self.side,
            'time': self.time,
            'value': self.value,
            'period': period,
            'num_periods': num_periods,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)


class BiggestDarkPoolTrades:
    def __init__(self, data):

        self.bidAsk = [i.get('bidAsk') for i in data]
        self.darkpool_or_block = [i.get('darkpool_or_block') for i in data]
        self.price = [i.get('price') for i in data]
        self.sector = [i.get('sector') for i in data]
        self.shares_proportion = [i.get('shares_proportion') for i in data]
        self.symbol = [i.get('symbol') for i in data]
        self.time = [i.get('time') for i in data]
        self.today_date = [i.get('today_date') for i in data]
        self.trade_size = [i.get('trade_size') for i in data]
        self.trade_type = [i.get('trade_type') for i in data]
        self.value = [i.get('value') for i in data]

        self.data_dict = { 
            'bid_ask': self.bidAsk,
            'darkpool_or_block': self.darkpool_or_block,
            'price': self.price,
            'sector': self.sector,
            'shares_proportion': self.shares_proportion,
            'symbol': self.symbol,
            'timestamp': self.time,
            'today_date': self.today_date,
            'trade_size': self.trade_size,
            'trade_type': self.trade_type,
            'value': self.value
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)



class StockDashboardData:
    def __init__(self, data):

        analyst_recommendations = data.get('analyst_recommendations')
        block_trades = data.get('block_trades')
        correlated_stocks = data.get('correlated_stocks')
        current_day_quotes = data.get('current_day_quotes')
        earnings_data = data.get('earnings_data')
        estimates = data.get('estimates')
        insider_trades = data.get('insider_trades')
        institutional_ownership = data.get('institutional_ownership')
        news = data.get('news')
        previous_earnings = data.get('previous_earnings')
        profitable_stats = data.get('profitable_stats')
        scany = data.get('scany')



        self.ticker = data.get('ticker')
        upgrade_downgrades = data.get('upgrade_downgrades')


        self.upgrades_downgrades_df = pd.DataFrame(upgrade_downgrades)

        self.technical_scan_df = pd.DataFrame(scany)
        self.institutional_ownership_df = pd.DataFrame(institutional_ownership, index=[0])
        self.analyst_ratings_df = pd.DataFrame(analyst_recommendations)