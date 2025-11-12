import pandas as pd


class MarketTide:
    def __init__(self, data):

        self.date = [i.get('date') for i in data]
        self.net_call_premium = [i.get('net_call_premium') for i in data]
        self.net_put_premium = [i.get('net_put_premium') for i in data]
        self.net_volume = [i.get('net_volume') for i in data]
        self.timestamp = [i.get('timestamp') for i in data]



        self.data_dict = { 
            'date': self.date,
            'call_premium': self.net_call_premium,
            'put_premium': self.net_put_premium,
            'volume': self.net_volume,
            'timestamp': self.timestamp
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)



class AllTide:
    def __init__(self, data):

        self.date = [i.get('date') for i in data]
        self.net_call_premium = [i.get('net_call_premium') for i in data]
        self.net_put_premium = [i.get('net_put_premium') for i in data]
        self.net_volume = [i.get('net_volume') for i in data]
        self.timestamp = [i.get('timestamp') for i in data]
        self.price = [i.get('spy_price') for i in data]



        self.data_dict = { 
            'date': self.date,
            'price': self.price,
            'call_premium': self.net_call_premium,
            'put_premium': self.net_put_premium,
            'volume': self.net_volume,
            'timestamp': self.timestamp
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)




class ETFFlow:
    def __init__(self, data):
        self.avg30_call_volume = [i.get('avg30_call_volume') for i in data]
        self.avg30_put_volume = [i.get('avg30_put_volume') for i in data]
        self.avg30_stock_volume = [i.get('avg30_stock_volume') for i in data]
        self.avg_30_day_call_volume = [i.get('avg_30_day_call_volume') for i in data]
        self.avg_30_day_put_volume = [i.get('avg_30_day_put_volume') for i in data]
        self.avg_7_day_call_volume = [i.get('avg_7_day_call_volume') for i in data]
        self.avg_7_day_put_volume = [i.get('avg_7_day_put_volume') for i in data]
        self.bearish_premium = [i.get('bearish_premium') for i in data]
        self.bullish_premium = [i.get('bullish_premium') for i in data]
        self.call_premium = [i.get('call_premium') for i in data]
        self.call_volume = [i.get('call_volume') for i in data]
        self.full_name = [i.get('full_name') for i in data]
        self.high = [i.get('high') for i in data]
        self.last = [i.get('last') for i in data]
        self.low = [i.get('low') for i in data]
        self.marketcap = [i.get('marketcap') for i in data]
        self.open = [i.get('open') for i in data]
        self.prev_close = [i.get('prev_close') for i in data]
        self.prev_date = [i.get('prev_date') for i in data]
        self.put_premium = [i.get('put_premium') for i in data]
        self.put_volume = [i.get('put_volume') for i in data]
        self.ticker = [i.get('ticker') for i in data]
        self.volume = [i.get('volume') for i in data]
        self.week52_high = [i.get('week52_high') for i in data]
        self.week52_low = [i.get('week52_low') for i in data]


        self.data_dict = {
            'avg30_call_volume': self.avg30_call_volume,
            'avg30_put_volume': self.avg30_put_volume,
            'avg30_stock_volume': self.avg30_stock_volume,
            'avg_30_day_call_volume': self.avg_30_day_call_volume,
            'avg_30_day_put_volume': self.avg_30_day_put_volume,
            'avg_7_day_call_volume': self.avg_7_day_call_volume,
            'avg_7_day_put_volume': self.avg_7_day_put_volume,
            'bearish_premium': self.bearish_premium,
            'bullish_premium': self.bullish_premium,
            'call_premium': self.call_premium,
            'call_volume': self.call_volume,
            'full_name': self.full_name,
            'high': self.high,
            'last': self.last,
            'low': self.low,
            'marketcap': self.marketcap,
            'open': self.open,
            'prev_close': self.prev_close,
            'prev_date': self.prev_date,
            'put_premium': self.put_premium,
            'put_volume': self.put_volume,
            'ticker': self.ticker,
            'volume': self.volume,
            'week52_high': self.week52_high,
            'week52_low': self.week52_low
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)




class TopNetPremium:
    def __init__(self, data):


        self.net_call_prem = [i.get('net_call_prem') for i in data]
        self.net_call_prem_rank = [i.get('net_call_prem_rank') for i in data]
        self.net_prem_rank = [i.get('net_prem_rank') for i in data]
        self.net_put_prem = [i.get('net_put_prem') for i in data]
        self.net_put_prem_rank = [i.get('net_put_prem_rank') for i in data]
        self.ticker = [i.get('ticker') for i in data]

        self.data_dict = { 

            'call_prem': self.net_call_prem,
            'call_prem_rank': self.net_call_prem_rank,
            'prem_rank': self.net_prem_rank,
            'put_prem': self.net_put_prem,
            'put_prem_rank': self.net_put_prem_rank,
            'ticker': self.ticker
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)




class TopSectorPremium:
    def __init__(self, data):
        self.net_call_prem = [i.get('net_call_prem') for i in data]
        self.net_call_prem_rank = [i.get('net_call_prem_rank') for i in data]
        self.net_prem_rank = [i.get('net_prem_rank') for i in data]
        self.net_put_prem = [i.get('net_put_prem') for i in data]
        self.net_put_prem_rank = [i.get('net_put_prem_rank') for i in data]
        self.ticker = [i.get('ticker') for i in data]



        self.data_dict = { 

            'call_prem': self.net_call_prem,
            'call_prem_rank': self.net_call_prem_rank,
            'prem_rank': self.net_prem_rank,
            'put_prem': self.net_put_prem,
            'put_prem_rank': self.net_put_prem_rank,
            'ticker': self.ticker
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)




class DarkPools:
    def __init__(self, data):
        self.avg30_volume = [i.get('avg30_volume') for i in data]
        self.canceled = [i.get('canceled') for i in data]
        self.created_at = [i.get('created_at') for i in data]
        self.executed_at = [i.get('executed_at') for i in data]
        self.ext_hour_sold_codes = [i.get('ext_hour_sold_codes') for i in data]
        self.issue_type = [i.get('issue_type') for i in data]
        self.market_center = [i.get('market_center') for i in data]
        self.nbbo_ask = [i.get('nbbo_ask') for i in data]
        self.nbbo_ask_quantity = [i.get('nbbo_ask_quantity') for i in data]
        self.nbbo_bid = [i.get('nbbo_bid') for i in data]
        self.nbbo_bid_quantity = [i.get('nbbo_bid_quantity') for i in data]
        self.premium = [i.get('premium') for i in data]
        self.price = [i.get('price') for i in data]
        self.sale_cond_codes = [i.get('sale_cond_codes') for i in data]
        self.sector = [i.get('sector') for i in data]
        self.size = [i.get('size') for i in data]
        self.ticker = [i.get('ticker') for i in data]
        self.tracking_id = [i.get('tracking_id') for i in data]
        self.trade_code = [i.get('trade_code') for i in data]
        self.trade_settlement = [i.get('trade_settlement') for i in data]
        self.volume = [i.get('volume') for i in data]



        self.data_dict = {
            'avg30_volume': self.avg30_volume,
            'canceled': self.canceled,
            'created_at': self.created_at,
            'executed_at': self.executed_at,
            'ext_hour_sold_codes': self.ext_hour_sold_codes,
            'issue_type': self.issue_type,
            'market_center': self.market_center,
            'ask': self.nbbo_ask,
            'ask_quantity': self.nbbo_ask_quantity,
            'bid': self.nbbo_bid,
            'bid_quantity': self.nbbo_bid_quantity,
            'premium': self.premium,
            'price': self.price,
            'sale_code': self.sale_cond_codes,
            'sector': self.sector,
            'size': self.size,
            'ticker': self.ticker,  # Renamed to avoid conflict with existing 'ticker' key
            'tracking_id': self.tracking_id,
            'trade_code': self.trade_code,
            'trade_settlement': self.trade_settlement,
            'volume': self.volume  # Renamed to avoid conflict with existing 'volume' key
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)




class TickerAggregates:
    def __init__(self, data):

        self.call_amount_ask_side = [i.get('call_amount_ask_side') for i in data]
        self.call_amount_bid_side = [i.get('call_amount_bid_side') for i in data]
        self.call_premium = [i.get('call_premium') for i in data]
        self.call_premium_ask_side = [i.get('call_premium_ask_side') for i in data]
        self.call_premium_bid_side = [i.get('call_premium_bid_side') for i in data]
        self.call_volume = [i.get('call_volume') for i in data]
        self.call_volume_ask_side = [i.get('call_volume_ask_side') for i in data]
        self.call_volume_bid_side = [i.get('call_volume_bid_side') for i in data]
        self.date = [i.get('date') for i in data]
        self.put_amount_ask_side = [i.get('put_amount_ask_side') for i in data]
        self.put_amount_bid_side = [i.get('put_amount_bid_side') for i in data]
        self.put_premium = [i.get('put_premium') for i in data]
        self.put_premium_ask_side = [i.get('put_premium_ask_side') for i in data]
        self.put_premium_bid_side = [i.get('put_premium_bid_side') for i in data]
        self.put_volume = [i.get('put_volume') for i in data]
        self.put_volume_ask_side = [i.get('put_volume_ask_side') for i in data]
        self.put_volume_bid_side = [i.get('put_volume_bid_side') for i in data]
        self.ticker_symbol = [i.get('ticker_symbol') for i in data]


        self.data_dict = {
            'call_amount_ask_side': self.call_amount_ask_side,
            'call_amount_bid_side': self.call_amount_bid_side,
            'call_premium': self.call_premium,
            'call_premium_ask_side': self.call_premium_ask_side,
            'call_premium_bid_side': self.call_premium_bid_side,
            'call_volume': self.call_volume,
            'call_volume_ask_side': self.call_volume_ask_side,
            'call_volume_bid_side': self.call_volume_bid_side,
            'date': self.date,
            'put_amount_ask_side': self.put_amount_ask_side,
            'put_amount_bid_side': self.put_amount_bid_side,
            'put_premium': self.put_premium,
            'put_premium_ask_side': self.put_premium_ask_side,
            'put_premium_bid_side': self.put_premium_bid_side,
            'put_volume': self.put_volume,
            'put_volume_ask_side': self.put_volume_ask_side,
            'put_volume_bid_side': self.put_volume_bid_side,
            'ticker_symbol': self.ticker_symbol
        }


    
        self.as_dataframe = pd.DataFrame(self.data_dict)




class AnalystResults:
    def __init__(self, data):
        self.firm_name = [i.get('firm_name') for i in data]
        self.total_recommendations = [i.get('total_recommendations') for i in data]
        self.expert_uid = [i.get('expert_uid') for i in data]
        self.analyst_name = [i.get('analyst_name') for i in data]
        self.success_rate = [i.get('success_rate') for i in data]
        self.stock_price = [i.get('stock_price') for i in data]
        self.added_on_timestamp = [i.get('added_on_timestamp') for i in data]
        self.analyst_rank = [i.get('analyst_rank') for i in data]
        self.timestamp = [i.get('timestamp') for i in data]
        self.sector = [i.get('sector') for i in data]
        self.url = [i.get('url') for i in data]
        self.article_site = [i.get('article_site') for i in data]
        self.stock_ticker = [i.get('stock_ticker') for i in data]
        self.company_name = [i.get('company_name') for i in data]
        self.action = [i.get('action') for i in data]
        self.number_of_ranked_experts = [i.get('number_of_ranked_experts') for i in data]
        self.created_at = [i.get('created_at') for i in data]
        self.recommendation_date = [i.get('recommendation_date') for i in data]
        self.operation_id = [i.get('operation_id') for i in data]
        self.id = [i.get('id') for i in data]
        self.article_title = [i.get('article_title') for i in data]
        self.converted_price_target = [i.get('converted_price_target') for i in data]
        self.full_name = [i.get('full_name') for i in data]
        self.curr_stock_price = [i.get('curr_stock_price') for i in data]
        self.marketcap = [i.get('marketcap') for i in data]
        self.num_of_stars = [i.get('num_of_stars') for i in data]
        self.quote = [i.get('quote') for i in data]
        self.recommendation = [i.get('recommendation') for i in data]
        self.expert_picture_url = [i.get('expert_picture_url') for i in data]
        self.unique_operation_id = [i.get('unique_operation_id') for i in data]
        self.price_target = [i.get('price_target') for i in data]
        self.excess_return = [i.get('excess_return') for i in data]
        self.good_recommendations = [i.get('good_recommendations') for i in data]



        self.data_dict = {
            'firm_name': self.firm_name,
            'total_recommendations': self.total_recommendations,
            'expert_uid': self.expert_uid,
            'analyst_name': self.analyst_name,
            'success_rate': self.success_rate,
            'stock_price': self.stock_price,
            'added_on_timestamp': self.added_on_timestamp,
            'analyst_rank': self.analyst_rank,
            'timestamp': self.timestamp,
            'sector': self.sector,
            'url': self.url,
            'article_site': self.article_site,
            'stock_ticker': self.stock_ticker,
            'company_name': self.company_name,
            'action': self.action,
            'number_of_ranked_experts': self.number_of_ranked_experts,
            'created_at': self.created_at,
            'recommendation_date': self.recommendation_date,
            'operation_id': self.operation_id,
            'id': self.id,
            'article_title': self.article_title,
            'converted_price_target': self.converted_price_target,
            'full_name': self.full_name,
            'curr_stock_price': self.curr_stock_price,
            'marketcap': self.marketcap,
            'num_of_stars': self.num_of_stars,
            'quote': self.quote,
            'recommendation': self.recommendation,
            'expert_picture_url': self.expert_picture_url,
            'unique_operation_id': self.unique_operation_id,
            'price_target': self.price_target,
            'excess_return': self.excess_return,
            'good_recommendations': self.good_recommendations
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class MarketState:
    def __init__(self, data):
        self.avg_30_day_call_oi = [i.get('avg_30_day_call_oi') for i in data]
        self.avg_30_day_call_volume = [i.get('avg_30_day_call_volume') for i in data]
        self.avg_30_day_put_oi = [i.get('avg_30_day_put_oi') for i in data]
        self.avg_30_day_put_volume = [i.get('avg_30_day_put_volume') for i in data]
        self.avg_3_day_call_volume = [i.get('avg_3_day_call_volume') for i in data]
        self.avg_3_day_put_volume = [i.get('avg_3_day_put_volume') for i in data]
        self.avg_7_day_call_volume = [i.get('avg_7_day_call_volume') for i in data]
        self.avg_7_day_put_volume = [i.get('avg_7_day_put_volume') for i in data]
        self.bearish_premium = [i.get('bearish_premium') for i in data]
        self.bullish_premium = [i.get('bullish_premium') for i in data]
        self.call_open_interest = [i.get('call_open_interest') for i in data]
        self.call_premium = [i.get('call_premium') for i in data]
        self.call_volume = [i.get('call_volume') for i in data]
        self.call_volume_ask_side = [i.get('call_volume_ask_side') for i in data]
        self.call_volume_bid_side = [i.get('call_volume_bid_side') for i in data]
        self.close = [i.get('close') for i in data]
        self.date = [i.get('date') for i in data]
        self.implied_move = [i.get('implied_move') for i in data]
        self.implied_move_perc = [i.get('implied_move_perc') for i in data]
        self.iv_rank = [i.get('iv_rank') for i in data]
        self.net_premium = [i.get('net_premium') for i in data]
        self.open = [i.get('open') for i in data]
        self.put_call_ratio = [i.get('put_call_ratio') for i in data]
        self.put_open_interest = [i.get('put_open_interest') for i in data]
        self.put_premium = [i.get('put_premium') for i in data]
        self.put_volume = [i.get('put_volume') for i in data]
        self.put_volume_ask_side = [i.get('put_volume_ask_side') for i in data]
        self.put_volume_bid_side = [i.get('put_volume_bid_side') for i in data]
        self.ticker = [i.get('ticker') for i in data]
        self.total_open_interest = [i.get('total_open_interest') for i in data]
        self.volatility = [i.get('volatility') for i in data]


        self.data_dict = {
            'avg_30_day_call_oi': self.avg_30_day_call_oi,
            'avg_30_day_call_volume': self.avg_30_day_call_volume,
            'avg_30_day_put_oi': self.avg_30_day_put_oi,
            'avg_30_day_put_volume': self.avg_30_day_put_volume,
            'avg_3_day_call_volume': self.avg_3_day_call_volume,
            'avg_3_day_put_volume': self.avg_3_day_put_volume,
            'avg_7_day_call_volume': self.avg_7_day_call_volume,
            'avg_7_day_put_volume': self.avg_7_day_put_volume,
            'bearish_premium': self.bearish_premium,
            'bullish_premium': self.bullish_premium,
            'call_open_interest': self.call_open_interest,
            'call_premium': self.call_premium,
            'call_volume': self.call_volume,
            'call_volume_ask_side': self.call_volume_ask_side,
            'call_volume_bid_side': self.call_volume_bid_side,
            'close': self.close,
            'date': self.date,
            'implied_move': self.implied_move,
            'implied_move_perc': self.implied_move_perc,
            'iv_rank': self.iv_rank,
            'net_premium': self.net_premium,
            'open': self.open,
            'put_call_ratio': self.put_call_ratio,
            'put_open_interest': self.put_open_interest,
            'put_premium': self.put_premium,
            'put_volume': self.put_volume,
            'put_volume_ask_side': self.put_volume_ask_side,
            'put_volume_bid_side': self.put_volume_bid_side,
            'ticker': self.ticker,
            'total_open_interest': self.total_open_interest,
            'volatility': self.volatility
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)




class CompanyData:
    def __init__(self, data):


        self.annouce_time = data.get('annouce_time')
        self.year2_change_percent = data.get('year2_change_percent')
        self.week52_high = data.get('week52_high')
        self.day30_change_percent = data.get('day30_change_percent')
        self.pe_ratio = data.get('pe_ratio')
        self.float_shares = data.get('float_shares')
        self.year1_change_percent = data.get('year1_change_percent')
        self.has_investment_arm = data.get('has_investment_arm')
        self.month3_change_percent = data.get('month3_change_percent')
        self.ttm_dividend_rate = data.get('ttm_dividend_rate')
        self.day50_moving_avg = data.get('day50_moving_avg')
        self.description = data.get('description')
        self.beta = data.get('beta')
        self.month1_change_percent = data.get('month1_change_percent')
        self.ttm_eps = data.get('ttm_eps')
        self.week52_low = data.get('week52_low')
        self.symbol = data.get('symbol')
        self.max_change_percent = data.get('max_change_percent')
        self.issue_type = data.get('issue_type')
        self.sector = data.get('sector')
        self.week52_low_split_adjustonly = data.get('week52_low_split_adjustonly')
        self.avg30_volume = data.get('avg30_volume')
        self.s_float = data.get('s_float')
        self.tags = data.get('tags')
        self.tags = ','.join(self.tags)
        self.insider_percent_ownership = data.get('insider_percent_ownership')
        self.avg10_volume = data.get('avg10_volume')
        self.month6_change_percent = data.get('month6_change_percent')
        self.ex_dividend_date = data.get('ex_dividend_date')
        self.is_spac = data.get('is_spac')
        self.created_at = data.get('created_at')
        self.outstanding = data.get('outstanding')
        self.warrant_links = data.get('warrant_links')
        self.id = data.get('id')
        self.next_dividend_date = data.get('next_dividend_date')
        self.week52_change = data.get('week52_change')
        self.full_name = data.get('full_name')
        self.year5_change_percent = data.get('year5_change_percent')
        self.marketcap = data.get('marketcap')
        self.employees = data.get('employees')
        self.ytd_change_percent = data.get('ytd_change_percent')
        self.week52_high_split_adjustonly = data.get('week52_high_split_adjustonly')
        self.website = data.get('website')
        self.logo = data.get('logo')
        self.day200_moving_avg = data.get('day200_moving_avg')
        self.industry_type = data.get('industry_type')
        self.dividend_yield = data.get('dividend_yield')
        self.day5_change_percent = data.get('day5_change_percent')
        self.current_fiscal_period = data.get('current_fiscal_period')
        self.short_int = data.get('short_int')
        self.next_earnings_date = data.get('next_earnings_date')



        self.data_dict = {
            'annouce_time': self.annouce_time,
            'year2_change_percent': self.year2_change_percent,
            'week52_high': self.week52_high,
            'day30_change_percent': self.day30_change_percent,
            'pe_ratio': self.pe_ratio,
            'float_shares': self.float_shares,
            'year1_change_percent': self.year1_change_percent,
            'has_investment_arm': self.has_investment_arm,
            'month3_change_percent': self.month3_change_percent,
            'ttm_dividend_rate': self.ttm_dividend_rate,
            'day50_moving_avg': self.day50_moving_avg,
            'description': self.description,
            'beta': self.beta,
            'month1_change_percent': self.month1_change_percent,
            'ttm_eps': self.ttm_eps,
            'week52_low': self.week52_low,
            'symbol': self.symbol,
            'max_change_percent': self.max_change_percent,
            'issue_type': self.issue_type,
            'sector': self.sector,
            'week52_low_split_adjustonly': self.week52_low_split_adjustonly,
            'avg30_volume': self.avg30_volume,
            's_float': self.s_float,
            'tags': self.tags,
            'insider_percent_ownership': self.insider_percent_ownership,
            'avg10_volume': self.avg10_volume,
            'month6_change_percent': self.month6_change_percent,
            'ex_dividend_date': self.ex_dividend_date,
            'is_spac': self.is_spac,
            'created_at': self.created_at,
            'outstanding': self.outstanding,
            'warrant_links': self.warrant_links,
            'id': self.id,
            'next_dividend_date': self.next_dividend_date,
            'week52_change': self.week52_change,
            'full_name': self.full_name,
            'year5_change_percent': self.year5_change_percent,
            'marketcap': self.marketcap,
            'employees': self.employees,
            'ytd_change_percent': self.ytd_change_percent,
            'week52_high_split_adjustonly': self.week52_high_split_adjustonly,
            'website': self.website,
            'logo': self.logo,
            'day200_moving_avg': self.day200_moving_avg,
            'industry_type': self.industry_type,
            'dividend_yield': self.dividend_yield,
            'day5_change_percent': self.day5_change_percent,
            'current_fiscal_period': self.current_fiscal_period,
            'short_int': self.short_int,
            'next_earnings_date': self.next_earnings_date
        }


        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])



class HistoricChains:
    def __init__(self, data):
        self.ask_volume = [i.get('ask_volume') for i in data]
        self.avg_price = [i.get('avg_price') for i in data]
        self.bid_volume = [i.get('bid_volume') for i in data]
        self.cross_volume = [i.get('cross_volume') for i in data]
        self.date = [i.get('date') for i in data]
        self.floor_volume = [i.get('floor_volume') for i in data]
        self.high_price = [i.get('high_price') for i in data]
        self.implied_volatility = [i.get('implied_volatility') for i in data]
        self.iv_high = [i.get('iv_high') for i in data]
        self.iv_low = [i.get('iv_low') for i in data]
        self.last_price = [i.get('last_price') for i in data]
        self.last_tape_time = [i.get('last_tape_time') for i in data]
        self.low_price = [i.get('low_price') for i in data]
        self.mid_volume = [i.get('mid_volume') for i in data]
        self.multi_leg_volume = [i.get('multi_leg_volume') for i in data]
        self.neutral_volume = [i.get('neutral_volume') for i in data]
        self.no_side_volume = [i.get('no_side_volume') for i in data]
        self.open_interest = [i.get('open_interest') for i in data]
        self.stock_multi_leg_volume = [i.get('stock_multi_leg_volume') for i in data]
        self.sweep_volume = [i.get('sweep_volume') for i in data]
        self.total_premium = [i.get('total_premium') for i in data]
        self.trades = [i.get('trades') for i in data]
        self.volume = [i.get('volume') for i in data]


        self.data_dict = {
        'ask_volume': self.ask_volume,
        'avg_price': self.avg_price,
        'bid_volume': self.bid_volume,
        'cross_volume': self.cross_volume,
        'date': self.date,
        'floor_volume': self.floor_volume,
        'high_price': self.high_price,
        'implied_volatility': self.implied_volatility,
        'iv_high': self.iv_high,
        'iv_low': self.iv_low,
        'last_price': self.last_price,
        'last_tape_time': self.last_tape_time,
        'low_price': self.low_price,
        'mid_volume': self.mid_volume,
        'multi_leg_volume': self.multi_leg_volume,
        'neutral_volume': self.neutral_volume,
        'no_side_volume': self.no_side_volume,
        'open_interest': self.open_interest,
        'stock_multi_leg_volume': self.stock_multi_leg_volume,
        'sweep_volume': self.sweep_volume,
        'total_premium': self.total_premium,
        'trades': self.trades,
        'volume': self.volume
            }
        


        self.as_dataframe = pd.DataFrame(self.data_dict)




class DailyOptionBars:
    def __init__(self, data, option_symbol):
        self.option_symbol = option_symbol

        self.ask_volume = [i.get('ask_volume') for i in data]
        self.bid_volume = [i.get('bid_volume') for i in data]
        self.mid_volume = [i.get('mid_volume') for i in data]
        self.no_volume = [i.get('no_volume') for i in data]
        self.tape_time = [i.get('tape_time') for i in data]


        self.data_dict = {
        'option_symbol': self.option_symbol,
        'ask_volume': self.ask_volume,
        'bid_volume': self.bid_volume,
        'mid_volume': self.mid_volume,
        'no_volume': self.no_volume,
        'tape_time': self.tape_time
    }
        



        self.as_dataframe = pd.DataFrame(self.data_dict)





class OptionSummary:
    def __init__(self, data):
        self.ffexern90_60 = [i.get('ffexern90_60') for i in data]
        self.exerndlt25iv90d = [i.get('exerndlt25iv90d') for i in data]
        self.exerndlt75iv30d = [i.get('exerndlt75iv30d') for i in data]
        self.exerndlt75iv20d = [i.get('exerndlt75iv20d') for i in data]
        self.exerndlt95iv10d = [i.get('exerndlt95iv10d') for i in data]
        self.iv90d = [i.get('iv90d') for i in data]
        self.exerndlt75iv6m = [i.get('exerndlt75iv6m') for i in data]
        self.dlt25iv1y = [i.get('dlt25iv1y') for i in data]
        self.iv30d = [i.get('iv30d') for i in data]
        self.next_div = [i.get('next_div') for i in data]
        self.ex_ern_iv20d = [i.get('ex_ern_iv20d') for i in data]
        self.fexern30_20 = [i.get('fexern30_20') for i in data]
        self.dlt75iv20d = [i.get('dlt75iv20d') for i in data]
        self.exerndlt95iv20d = [i.get('exerndlt95iv20d') for i in data]
        self.stock_price = [i.get('stock_price') for i in data]
        self.ann_idiv = [i.get('ann_idiv') for i in data]
        self.dlt75iv30d = [i.get('dlt75iv30d') for i in data]
        self.borrow30 = [i.get('borrow30') for i in data]
        self.exerndlt95iv60d = [i.get('exerndlt95iv60d') for i in data]
        self.exerndlt25iv30d = [i.get('exerndlt25iv30d') for i in data]
        self.fwd90_60 = [i.get('fwd90_60') for i in data]
        self.exerndlt95iv6m = [i.get('exerndlt95iv6m') for i in data]
        self.ex_ern_iv6m = [i.get('ex_ern_iv6m') for i in data]
        self.exerndlt25iv20d = [i.get('exerndlt25iv20d') for i in data]
        self.dlt75iv1y = [i.get('dlt75iv1y') for i in data]
        self.ffwd180_90 = [i.get('ffwd180_90') for i in data]
        self.exerndlt95iv1y = [i.get('exerndlt95iv1y') for i in data]
        self.exerndlt5iv90d = [i.get('exerndlt5iv90d') for i in data]
        self.exerndlt25iv1y = [i.get('exerndlt25iv1y') for i in data]
        self.exerndlt25iv6m = [i.get('exerndlt25iv6m') for i in data]
        self.dlt95iv60d = [i.get('dlt95iv60d') for i in data]
        self.dlt25iv30d = [i.get('dlt25iv30d') for i in data]
        self.iv60d = [i.get('iv60d') for i in data]
        self.fbfexern180_90 = [i.get('fbfexern180_90') for i in data]
        self.exerndlt5iv20d = [i.get('exerndlt5iv20d') for i in data]
        self.updated_at = [i.get('updated_at') for i in data]
        self.ex_ern_iv10d = [i.get('ex_ern_iv10d') for i in data]
        self.contango = [i.get('contango') for i in data]
        self.dlt25iv90d = [i.get('dlt25iv90d') for i in data]
        self.fbfexern90_30 = [i.get('fbfexern90_30') for i in data]
        self.fbfwd90_60 = [i.get('fbfwd90_60') for i in data]
        self.iv6m = [i.get('iv6m') for i in data]
        self.totalerrorconf = [i.get('totalerrorconf') for i in data]
        self.dlt75iv10d = [i.get('dlt75iv10d') for i in data]
        self.fwd30_20 = [i.get('fwd30_20') for i in data]
        self.exerndlt75iv10d = [i.get('exerndlt75iv10d') for i in data]
        self.dlt5iv20d = [i.get('dlt5iv20d') for i in data]
        self.rslp2y = [i.get('rslp2y') for i in data]
        self.iv10d = [i.get('iv10d') for i in data]
        self.fexern60_30 = [i.get('fexern60_30') for i in data]
        self.fbfwd180_90 = [i.get('fbfwd180_90') for i in data]
        self.iv1y = [i.get('iv1y') for i in data]
        self.fwd180_90 = [i.get('fwd180_90') for i in data]
        self.dlt25iv10d = [i.get('dlt25iv10d') for i in data]
        self.ffwd30_20 = [i.get('ffwd30_20') for i in data]
        self.riskfree2y = [i.get('riskfree2y') for i in data]
        self.iv20d = [i.get('iv20d') for i in data]
        self.skewing = [i.get('skewing') for i in data]
        self.ffwd60_30 = [i.get('ffwd60_30') for i in data]
        self.ffwd90_60 = [i.get('ffwd90_60') for i in data]
        self.exerndlt5iv6m = [i.get('exerndlt5iv6m') for i in data]
        self.ffexern90_30 = [i.get('ffexern90_30') for i in data]
        self.dlt25iv6m = [i.get('dlt25iv6m') for i in data]
        self.exerndlt5iv10d = [i.get('exerndlt5iv10d') for i in data]
        self.ffexern60_30 = [i.get('ffexern60_30') for i in data]
        self.fbfexern90_60 = [i.get('fbfexern90_60') for i in data]
        self.rdrv30 = [i.get('rdrv30') for i in data]
        self.exerndlt5iv30d = [i.get('exerndlt5iv30d') for i in data]
        self.date = [i.get('date') for i in data]
        self.fbfexern30_20 = [i.get('fbfexern30_20') for i in data]
        self.dlt5iv60d = [i.get('dlt5iv60d') for i in data]
        self.exerndlt95iv30d = [i.get('exerndlt95iv30d') for i in data]
        self.dlt25iv60d = [i.get('dlt25iv60d') for i in data]
        self.dlt75iv90d = [i.get('dlt75iv90d') for i in data]
        self.fexern90_60 = [i.get('fexern90_60') for i in data]
        self.dlt95iv20d = [i.get('dlt95iv20d') for i in data]
        self.mw_adj30 = [i.get('mw_adj30') for i in data]
        self.fbfwd30_20 = [i.get('fbfwd30_20') for i in data]
        self.dlt95iv10d = [i.get('dlt95iv10d') for i in data]
        self.dlt5iv1y = [i.get('dlt5iv1y') for i in data]
        self.iee_earn_effect = [i.get('iee_earn_effect') for i in data]
        self.implied_earnings_move = [i.get('implied_earnings_move') for i in data]
        self.fwd60_30 = [i.get('fwd60_30') for i in data]
        self.created_at = [i.get('created_at') for i in data]
        self.exerndlt5iv1y = [i.get('exerndlt5iv1y') for i in data]
        self.dlt95iv1y = [i.get('dlt95iv1y') for i in data]
        self.ffexern180_90 = [i.get('ffexern180_90') for i in data]
        self.fexern90_30 = [i.get('fexern90_30') for i in data]
        self.ex_ern_iv60d = [i.get('ex_ern_iv60d') for i in data]
        self.implied_next_div = [i.get('implied_next_div') for i in data]
        self.ex_ern_iv30d = [i.get('ex_ern_iv30d') for i in data]
        self.dlt5iv90d = [i.get('dlt5iv90d') for i in data]
        self.dlt5iv10d = [i.get('dlt5iv10d') for i in data]
        self.dlt95iv90d = [i.get('dlt95iv90d') for i in data]
        self.rip = [i.get('rip') for i in data]
        self.rslp30 = [i.get('rslp30') for i in data]
        self.exerndlt5iv60d = [i.get('exerndlt5iv60d') for i in data]
        self.dlt95iv6m = [i.get('dlt95iv6m') for i in data]
        self.exerndlt75iv90d = [i.get('exerndlt75iv90d') for i in data]
        self.exerndlt25iv60d = [i.get('exerndlt25iv60d') for i in data]
        self.ex_ern_iv90d = [i.get('ex_ern_iv90d') for i in data]
        self.exerndlt25iv10d = [i.get('exerndlt25iv10d') for i in data]
        self.confidence = [i.get('confidence') for i in data]
        self.exerndlt95iv90d = [i.get('exerndlt95iv90d') for i in data]
        self.fexern180_90 = [i.get('fexern180_90') for i in data]
        self.fbfwd60_30 = [i.get('fbfwd60_30') for i in data]
        self.ticker = [i.get('ticker') for i in data]
        self.rvol2y = [i.get('rvol2y') for i in data]
        self.dlt95iv30d = [i.get('dlt95iv30d') for i in data]
        self.implied_move = [i.get('implied_move') for i in data]
        self.ffwd90_30 = [i.get('ffwd90_30') for i in data]
        self.fwd90_30 = [i.get('fwd90_30') for i in data]
        self.fbfexern60_30 = [i.get('fbfexern60_30') for i in data]
        self.dlt5iv30d = [i.get('dlt5iv30d') for i in data]
        self.exerndlt75iv1y = [i.get('exerndlt75iv1y') for i in data]
        self.rdrv2y = [i.get('rdrv2y') for i in data]
        self.dlt75iv60d = [i.get('dlt75iv60d') for i in data]
        self.rvol30 = [i.get('rvol30') for i in data]
        self.riskfree30 = [i.get('riskfree30') for i in data]
        self.ann_act_div = [i.get('ann_act_div') for i in data]
        self.fbfwd90_30 = [i.get('fbfwd90_30') for i in data]
        self.dlt5iv6m = [i.get('dlt5iv6m') for i in data]
        self.mw_adj2y = [i.get('mw_adj2y') for i in data]
        self.ffexern30_20 = [i.get('ffexern30_20') for i in data]
        self.ex_ern_iv1y = [i.get('ex_ern_iv1y') for i in data]
        self.dlt75iv6m = [i.get('dlt75iv6m') for i in data]
        self.exerndlt75iv60d = [i.get('exerndlt75iv60d') for i in data]
        self.dlt25iv20d = [i.get('dlt25iv20d') for i in data]
        self.borrow2y = [i.get('borrow2y') for i in data]

        self.data_dict = {
            'ffexern90_60': self.ffexern90_60,
            'exerndlt25iv90d': self.exerndlt25iv90d,
            'exerndlt75iv30d': self.exerndlt75iv30d,
            'exerndlt75iv20d': self.exerndlt75iv20d,
            'exerndlt95iv10d': self.exerndlt95iv10d,
            'iv90d': self.iv90d,
            'exerndlt75iv6m': self.exerndlt75iv6m,
            'dlt25iv1y': self.dlt25iv1y,
            'iv30d': self.iv30d,
            'next_div': self.next_div,
            'ex_ern_iv20d': self.ex_ern_iv20d,
            'fexern30_20': self.fexern30_20,
            'dlt75iv20d': self.dlt75iv20d,
            'exerndlt95iv20d': self.exerndlt95iv20d,
            'stock_price': self.stock_price,
            'ann_idiv': self.ann_idiv,
            'dlt75iv30d': self.dlt75iv30d,
            'borrow30': self.borrow30,
            'exerndlt95iv60d': self.exerndlt95iv60d,
            'exerndlt25iv30d': self.exerndlt25iv30d,
            'fwd90_60': self.fwd90_60,
            'exerndlt95iv6m': self.exerndlt95iv6m,
            'ex_ern_iv6m': self.ex_ern_iv6m,
            'exerndlt25iv20d': self.exerndlt25iv20d,
            'dlt75iv1y': self.dlt75iv1y,
            'ffwd180_90': self.ffwd180_90,
            'exerndlt95iv1y': self.exerndlt95iv1y,
            'exerndlt5iv90d': self.exerndlt5iv90d,
            'exerndlt25iv1y': self.exerndlt25iv1y,
            'exerndlt25iv6m': self.exerndlt25iv6m,
            'dlt95iv60d': self.dlt95iv60d,
            'dlt25iv30d': self.dlt25iv30d,
            'iv60d': self.iv60d,
            'fbfexern180_90': self.fbfexern180_90,
            'exerndlt5iv20d': self.exerndlt5iv20d,
            'updated_at': self.updated_at,
            'ex_ern_iv10d': self.ex_ern_iv10d,
            'contango': self.contango,
            'dlt25iv90d': self.dlt25iv90d,
            'fbfexern90_30': self.fbfexern90_30,
            'fbfwd90_60': self.fbfwd90_60,
            'iv6m': self.iv6m,
            'totalerrorconf': self.totalerrorconf,
            'dlt75iv10d': self.dlt75iv10d,
            'fwd30_20': self.fwd30_20,
            'exerndlt75iv10d': self.exerndlt75iv10d,
            'dlt5iv20d': self.dlt5iv20d,
            'rslp2y': self.rslp2y,
            'iv10d': self.iv10d,
            'fexern60_30': self.fexern60_30,
            'fbfwd180_90': self.fbfwd180_90,
            'iv1y': self.iv1y,
            'fwd180_90': self.fwd180_90,
            'dlt25iv10d': self.dlt25iv10d,
            'ffwd30_20': self.ffwd30_20,
            'riskfree2y': self.riskfree2y,
            'iv20d': self.iv20d,
            'skewing': self.skewing,
            'ffwd60_30': self.ffwd60_30,
            'ffwd90_60': self.ffwd90_60,
            'exerndlt5iv6m': self.exerndlt5iv6m,
            'ffexern90_30': self.ffexern90_30,
            'dlt25iv6m': self.dlt25iv6m,
            'exerndlt5iv10d': self.exerndlt5iv10d,
            'ffexern60_30': self.ffexern60_30,
            'fbfexern90_60': self.fbfexern90_60,
            'rdrv30': self.rdrv30,
            'exerndlt5iv30d': self.exerndlt5iv30d,
            'dlt95iv1y': self.dlt95iv1y,
            'ffexern180_90': self.ffexern180_90,
            'fexern90_30': self.fexern90_30,
            'ex_ern_iv60d': self.ex_ern_iv60d,
            'implied_next_div': self.implied_next_div,
            'ex_ern_iv30d': self.ex_ern_iv30d,
            'dlt5iv90d': self.dlt5iv90d,
            'dlt5iv10d': self.dlt5iv10d,
            'dlt95iv90d': self.dlt95iv90d,
            'rip': self.rip,
            'rslp30': self.rslp30,
            'exerndlt5iv60d': self.exerndlt5iv60d,
            'dlt95iv6m': self.dlt95iv6m,
            'exerndlt75iv90d': self.exerndlt75iv90d,
            'exerndlt25iv60d': self.exerndlt25iv60d,
            'ex_ern_iv90d': self.ex_ern_iv90d,
            'exerndlt25iv10d': self.exerndlt25iv10d,
            'confidence': self.confidence,
            'exerndlt95iv90d': self.exerndlt95iv90d,
            'fexern180_90': self.fexern180_90,
            'fbfwd60_30': self.fbfwd60_30,
            'ticker': self.ticker,
            'rvol2y': self.rvol2y,
            'dlt95iv30d': self.dlt95iv30d,
            'implied_move': self.implied_move,
            'ffwd90_30': self.ffwd90_30,
            'fwd90_30': self.fwd90_30,
            'fbfexern60_30': self.fbfexern60_30,
            'dlt5iv30d': self.dlt5iv30d,
            'exerndlt75iv1y': self.exerndlt75iv1y,
            'rdrv2y': self.rdrv2y,
            'dlt75iv60d': self.dlt75iv60d,
            'rvol30': self.rvol30,
            'riskfree30': self.riskfree30,
            'ann_act_div': self.ann_act_div,
            'fbfwd90_30': self.fbfwd90_30,
            'dlt5iv6m': self.dlt5iv6m,
            'mw_adj2y': self.mw_adj2y,
            'ffexern30_20': self.ffexern30_20,
            'ex_ern_iv1y': self.ex_ern_iv1y,
            'dlt75iv6m': self.dlt75iv6m,
            'exerndlt75iv60d': self.exerndlt75iv60d,
            'dlt25iv20d': self.dlt25iv20d,
            'borrow2y': self.borrow2y,
            'date': self.date
        }



        self.as_dataframe = pd.DataFrame(self.data_dict)