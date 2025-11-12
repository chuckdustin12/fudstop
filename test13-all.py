from fudstop4.apis.occ.occ_sdk import occSDK
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from fudstop4.apis.webull.webull_ta import WebullTA
from fudstop4.apis.webull.webull_trading import WebullTrading
from fudstop4.apis.y_finance.yf_sdk import YfSDK
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.helpers import format_large_numbers_in_dataframe2, is_etf, generate_webull_headers
db = PolygonOptions()
import numpy as np
from asyncpg.exceptions import UndefinedColumnError
from fudstop4.apis.polygonio.async_polygon_sdk import Polygon
import logging
import asyncpg
poly = Polygon()
yf = YfSDK()
trading = WebullTrading()
ta = WebullTA()
opts = WebullOptions()
occ = occSDK()
from itertools import islice
CONTRACT_MULTIPLIER = 100
import asyncio

import pandas as pd
import aiohttp
async def gamma_exposure(ticker: str) -> pd.DataFrame:
    """
    Calculate gamma exposure (GEX) for the given ticker and return a single DataFrame
    containing:
        1) Option-level data with gamma exposure columns.
        2) A 'cumulative_signed_gamma' column (sorted by strike).
        3) A final summary row appended at the bottom with:
           - Ticker
           - Underlying price
           - Max positive gamma & corresponding strike
           - Max negative gamma & corresponding strike
           - Approx 'zero gamma' strike (where cumulative gamma crosses zero)
    
    Requirements:
        The underlying data must contain the columns:
            'strike', 'expiry', 'oi', 'oi_change', 'iv', 'gamma',
            'delta', and 'call_put' (CALL or PUT).
        The data is expected to be accessible via:
            all_options_data_for_ticker.as_dataframe

    Returns:
        A single Pandas DataFrame with all details + one appended "summary" row.
    """

    # 1) Fetch all option data
    all_options_data_for_ticker = await opts.multi_options(
        ticker,
        headers=generate_webull_headers()
    )

    # 2) Get the current underlying price
    underlying_price = await poly.get_price(ticker)

    # 3) Confirm that 'call_put' (CALL or PUT) exists
    if 'call_put' not in all_options_data_for_ticker.as_dataframe.columns:
        raise ValueError(
            "We need a 'call_put' column (CALL or PUT) to do advanced gamma calculations. "
            "Please add it if your data feed can provide it."
        )

    # 4) Create a DataFrame of only relevant columns
    df = all_options_data_for_ticker.as_dataframe[[
        'strike', 'expiry', 'oi', 'oi_change', 'iv', 'gamma', 'delta', 'call_put'
    ]].copy()

    # 5) Convert any columns to numeric as needed
    numeric_cols = ['strike', 'oi', 'oi_change', 'iv', 'gamma', 'delta']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 6) Drop any rows with NaN in critical columns
    df.dropna(subset=['strike', 'gamma', 'oi'], inplace=True)

    # 7) Calculate per-contract gamma exposure
    #    "gamma_exposure" ~ gamma * underlying_price^2 * CONTRACT_MULTIPLIER * oi
    df['gamma_exposure'] = (
        df['gamma'] * (underlying_price ** 2) * CONTRACT_MULTIPLIER * df['oi']
    )

    # 8) Create a "signed" gamma exposure to treat puts as negative
    df['signed_gamma_exposure'] = np.where(
        df['call_put'].str.upper() == 'PUT',
        -df['gamma_exposure'],
        df['gamma_exposure']
    )

    # 9) Summaries for net signed gamma
    total_call_gamma = df.loc[
        df['call_put'].str.upper() == 'CALL',
        'gamma_exposure'
    ].sum()
    total_put_gamma = df.loc[
        df['call_put'].str.upper() == 'PUT',
        'gamma_exposure'
    ].sum()
    net_signed_gamma = df['signed_gamma_exposure'].sum()

    # 10) Sort by strike for cumulative gamma analysis
    df.sort_values('strike', inplace=True)

    # 11) Calculate cumulative signed gamma (ascending strikes)
    df['cumulative_signed_gamma'] = df['signed_gamma_exposure'].cumsum()

    # 12) Identify maximum positive/negative single-contract gamma exposures
    #     (NOT cumulative, but per contract)
    max_pos_idx = df['signed_gamma_exposure'].idxmax()
    max_neg_idx = df['signed_gamma_exposure'].idxmin()

    max_pos_gamma_value = df.loc[max_pos_idx, 'signed_gamma_exposure']
    max_pos_gamma_strike = df.loc[max_pos_idx, 'strike']

    max_neg_gamma_value = df.loc[max_neg_idx, 'signed_gamma_exposure']
    max_neg_gamma_strike = df.loc[max_neg_idx, 'strike']

    # 13) Approximate "zero gamma" strike via cumulative gamma crossing
    #     We'll do a simple linear interpolation between the sign change.
    zero_gamma_strike = None
    csg = df['cumulative_signed_gamma'].values
    strikes_arr = df['strike'].values

    for i in range(1, len(df)):
        prev_gamma = csg[i - 1]
        curr_gamma = csg[i]
        if (prev_gamma <= 0 < curr_gamma) or (prev_gamma >= 0 > curr_gamma):
            # We found a sign change region. Let's approximate the crossing
            # by linear interpolation between strikes_arr[i-1] and strikes_arr[i].
            x0, x1 = strikes_arr[i - 1], strikes_arr[i]
            y0, y1 = prev_gamma, curr_gamma
            # fraction along the interval
            frac = abs(y0) / abs(y1 - y0) if (y1 != y0) else 0
            zero_gamma_strike = x0 + frac * (x1 - x0)
            break

    # 14) Create a summary row to append to the DataFrame
    summary_row = {
        'strike': None,
        'expiry': None,
        'oi': None,
        'oi_change': None,
        'iv': None,
        'gamma': None,
        'delta': None,
        'call_put': 'SUMMARY_ROW',
        'gamma_exposure': None,
        'signed_gamma_exposure': None,
        'cumulative_signed_gamma': None
    }

    # Additional summary fields to show in the same row:
    summary_row['ticker'] = ticker
    summary_row['underlying_price'] = underlying_price
    summary_row['max_pos_gamma_value'] = max_pos_gamma_value
    summary_row['max_pos_gamma_strike'] = max_pos_gamma_strike
    summary_row['max_neg_gamma_value'] = max_neg_gamma_value
    summary_row['max_neg_gamma_strike'] = max_neg_gamma_strike
    summary_row['zero_gamma_strike'] = zero_gamma_strike
    summary_row['total_call_gamma'] = total_call_gamma
    summary_row['total_put_gamma'] = total_put_gamma
    summary_row['net_signed_gamma'] = net_signed_gamma

    # Append the summary row at the bottom
    summary_df = pd.DataFrame([summary_row], index=[len(df)])
    final_df = pd.concat([df, summary_df], ignore_index=True)

    return final_df

async def all(ticker):
    try:
        short_int = await trading.get_short_interest(ticker)

        info = await occ.stock_info(ticker)

        iv_percentile30D, iv_percentile60D, iv_percentile90D = info.ivp30,info.ivp60, info.ivp90
        iv_rank30D, iv_rank60D, iv_rank90D, iv_rank120D, iv_rank150D, iv_rank180D  = info.ivr30, info.ivr60, info.ivr90, info.ivr120, info.ivr150, info.ivr180
        historic_iv10D, historic_iv20D, historic_iv30D, historic_iv60D, historic_iv90D, historic_iv120D, historic_iv150D, historic_iv180D = info.hv10, info.hv20, info.hv30, info.hv60, info.hv90, info.hv120, info.hv150, info.hv180
        parkinsons10D, parkinsons20D, parkinsons30D, parkinsons60D, parkinsons90D, parkinsons120D, parkinsons150D, parkinsons180D = info.hvp10, info.hvp20, info.hvp30, info.hvp60, info.hvp90, info.hvp120, info.hvp150, info.hvp180


        volatility_rank = info.volatileRank

        sentiment = info.sentiment


        multiquote = await trading.multi_quote([ticker])

        volatility = multiquote.vibrateRatio[0]
        stock_vol = multiquote.volume[0]
        avg10d_stock_vol = multiquote.avgVol10D[0]
        avg3m_stock_vol = multiquote.avgVol3M[0]
        stock_open = multiquote.open[0]
        stock_high = multiquote.high[0]
        stock_low = multiquote.low[0]
        stock_close = multiquote.close[0]

        stock_52w_high = multiquote.fiftyTwoWkHigh[0]
        stock_52w_low = multiquote.fiftyTwoWkLow[0]

        stock_change = multiquote.change[0]
        stock_change_pct = multiquote.changeRatio[0]

        next_er = multiquote.nextEarningDay[0]
        shares_outstanding = multiquote.outstandingShares[0]
        total_shares = multiquote.totalShares[0]
        

        async with aiohttp.ClientSession(headers={'X-API-KEY': 'fudstop4'}) as session:
            async with session.get(f"https://www.fudstop.io/api/skew?ticker={ticker}") as resp:

                data = await resp.json()

                nearest_skew_strike = [i.get('strike') for i in data]

                nearest_skew_strike = nearest_skew_strike[0]

        if nearest_skew_strike > stock_close:
            skew = 'call_skew'

        elif nearest_skew_strike < stock_close:
            skew = 'put_skew'


        all_opts = await opts.multi_options(ticker, headers=generate_webull_headers())

            # Create separate DataFrames for Calls and Puts
        calls_df = all_opts.as_dataframe[all_opts.as_dataframe["call_put"].str.lower() == "call"].copy()
        puts_df = all_opts.as_dataframe[all_opts.as_dataframe["call_put"].str.lower() == "put"].copy()
            
        # Gamma
        avg_call_gamma = round(calls_df['gamma'].mean(skipna=True), 2)
        avg_put_gamma = round(puts_df['gamma'].mean(skipna=True), 2)

        # Theta
        avg_call_theta = round(calls_df['theta'].mean(skipna=True), 2)
        avg_put_theta = round(puts_df['theta'].mean(skipna=True), 2)

        # Delta
        avg_call_delta = round(calls_df['delta'].mean(skipna=True), 2)
        avg_put_delta = round(puts_df['delta'].mean(skipna=True), 2)

        # Rho
        avg_call_rho = round(calls_df['rho'].mean(skipna=True), 2)
        avg_put_rho = round(puts_df['rho'].mean(skipna=True), 2)

        # Vega
        avg_call_vega = round(calls_df['vega'].mean(skipna=True), 2)
        avg_put_vega = round(puts_df['vega'].mean(skipna=True), 2)

        # Implied Volatility (IV)
        avg_call_iv = round(calls_df['iv'].mean(skipna=True), 2)
        avg_put_iv = round(puts_df['iv'].mean(skipna=True), 2)


        total_call_volume = calls_df['volume'].sum()
        total_put_volume = puts_df['volume'].sum()


        total_call_oi = calls_df['oi'].sum()
        total_put_oi = puts_df['oi'].sum()


        total_call_net_oi_change = calls_df['oi'].sum()
        total_put_net_oi_change = puts_df['oi'].sum()


        technicals_query = f"""SELECT * from plays where ticker = '{ticker}'"""

        results = await db.fetch(technicals_query)

        technicals_df = pd.DataFrame(results, columns=await db.get_table_columns('plays'))
        # Create individual DataFrames by filtering the timespan column
        m1_df = technicals_df[technicals_df['timespan'] == 'm1'].copy()
        m5_df = technicals_df[technicals_df['timespan'] == 'm5'].copy()
        m15_df = technicals_df[technicals_df['timespan'] == 'm15'].copy()
        m30_df = technicals_df[technicals_df['timespan'] == 'm30'].copy()
        m60_df = technicals_df[technicals_df['timespan'] == 'm60'].copy()
        m120_df = technicals_df[technicals_df['timespan'] == 'm120'].copy()
        m240_df = technicals_df[technicals_df['timespan'] == 'm240'].copy()
        d_df = technicals_df[technicals_df['timespan'] == 'd'].copy()
        w_df = technicals_df[technicals_df['timespan'] == 'w'].copy()


        _1min_rsi = round(m1_df['rsi'].to_list()[0],2)
        _5min_rsi = round(m5_df['rsi'].to_list()[0],2)
        _15min_rsi = round(m15_df['rsi'].to_list()[0],2)
        _30min_rsi = round(m30_df['rsi'].to_list()[0],2)
        _60min_rsi = round(m60_df['rsi'].to_list()[0],2)
        _120min_rsi = round(m120_df['rsi'].to_list()[0],2)
        _240min_rsi = round(m240_df['rsi'].to_list()[0],2)
        day_rsi = round(d_df['rsi'].to_list()[0],2)
        week_rsi = round(w_df['rsi'].to_list()[0],2)
        
        _1min_td_buy = m1_df['td_buy_count'].to_list()[0]
        _5min_td_buy = m5_df['td_buy_count'].to_list()[0]
        _15min_td_buy = m15_df['td_buy_count'].to_list()[0]
        _30min_td_buy = m30_df['td_buy_count'].to_list()[0]
        _60min_td_buy = m60_df['td_buy_count'].to_list()[0]
        _120min_td_buy = m120_df['td_buy_count'].to_list()[0]
        _240min_td_buy = m240_df['td_buy_count'].to_list()[0]
        day_td_buy = d_df['td_buy_count'].to_list()[0]
        week_td_buy = w_df['td_buy_count'].to_list()[0]


        _1min_td_sell = m1_df['td_sell_count'].to_list()[0]
        _5min_td_sell = m5_df['td_sell_count'].to_list()[0]
        _15min_td_sell = m15_df['td_sell_count'].to_list()[0]
        _30min_td_sell = m30_df['td_sell_count'].to_list()[0]
        _60min_td_sell = m60_df['td_sell_count'].to_list()[0]
        _120min_td_sell = m120_df['td_sell_count'].to_list()[0]
        _240min_td_sell = m240_df['td_sell_count'].to_list()[0]
        day_td_sell = d_df['td_sell_count'].to_list()[0]
        week_td_sell = w_df['td_sell_count'].to_list()[0]
        
        


        news = await trading.ai_news(symbol=ticker, headers=generate_webull_headers())
    




        analysts = await trading.get_analyst_ratings(ticker)
        analyst_suggestion = analysts.rating_suggestion

        vol_anal = await trading.volume_analysis(ticker)

        stock_buy_vol_pct = float(vol_anal.buyPct)
        stock_sell_vol_pct = float(vol_anal.sellPct)
        stock_neut_vol_pct = float(vol_anal.nPct)
        stock_avg_price = float(vol_anal.avePrice)
        stock_total_trades = float(vol_anal.totalNum)


        

        



        async with aiohttp.ClientSession(headers={'X-API-KEY': 'fudstop4'}) as session:
            async with session.get(f"https://www.fudstop.io/api/target_strike?ticker={ticker}&target_strike={nearest_skew_strike}") as resp:
                data = await resp.json()

                formatted_data = [
            {key.lower().replace(" ", "_"): value for key, value in item.items()}
            for item in data
        ]

        async with aiohttp.ClientSession() as session:
            cap_flow, hist = await trading.capital_flow(ticker, session=session)

            


        dict = { 

            'ticker': ticker,
            'stock_open': stock_open,
            'stock_high': stock_high,
            'stock_low': stock_low,
            'stock_close': stock_close,
            'stock_volume': stock_vol,
            'stock_buy_vol_pct': stock_buy_vol_pct,
            'stock_sell_vol_pct': stock_sell_vol_pct,
            'stock_neut_vol_pct': stock_neut_vol_pct,
            'stock_avg_price': stock_avg_price,
            'stock_num_trades': stock_total_trades,
            'stock_avg10d_volume': avg10d_stock_vol,
            'stock_avg3m_volume': avg3m_stock_vol,
            'stock_change': stock_change,
            'stock_change_pct': stock_change_pct,
            'stock_52w_high': stock_52w_high,
            'stock_52w_low': stock_52w_low,
            'stock_sentiment': sentiment,
            'next_er': next_er,
            'shares_outstanding': shares_outstanding,
            'total_shares': total_shares,
            'shorted_shares': short_int.short_int[0],
            'short_pct_of_float': (float(short_int.short_int[0]) / total_shares) * 100 if total_shares > 0 else None,
      
            'days_to_cover': float(short_int.days_to_cover[0]),
            'volatility': volatility,
            'volatility_rank': volatility_rank,
            'skew': skew,
            'skew_strike': nearest_skew_strike,


            ##BETA##

            'beta10d': info.beta10D,
            'beta20d': info.beta20D,
            'beta30d': info.beta30D,
            'beta60d': info.beta60D,
            'beta90d': info.beta90D,
            'beta120d': info.beta120D,
            'beta150d': info.beta150D,
            'beta180d': info.beta180D,

            ## IV METRICS ##
            'iv_rank30': iv_rank30D,
            'iv_rank60': iv_rank60D,
            'iv_rank90': iv_rank90D,
            'iv_rank120': iv_rank120D,
            'iv_rank150': iv_rank150D,
            'iv_rank180': iv_rank180D,

            'iv_percentile30': iv_percentile30D,
            'iv_percentile60': iv_percentile60D,
            'iv_percentile90': iv_percentile90D,

            'historic_iv10': historic_iv10D,
            'historic_iv20': historic_iv20D,
            'historic_iv30': historic_iv30D,
            'historic_iv60': historic_iv60D,
            'historic_iv90': historic_iv90D,
            'historic_iv120': historic_iv120D,
            'historic_iv150': historic_iv150D,
            'historic_iv180': historic_iv180D,
            'iv_parkinsons10': parkinsons10D,
            'iv_parkinsons20': parkinsons20D,
            'iv_parkinsons30': parkinsons30D,
            'iv_parkinsons60': parkinsons60D,
            'iv_parkinsons90': parkinsons90D,
            'iv_parkinsons120': parkinsons120D,
            'iv_parkinsons150': parkinsons150D,
            'iv_parkinsons180': parkinsons180D,

        
            ##greeks

            'avg_call_delta': avg_call_delta,
            'avg_call_gamma': avg_call_gamma,
            'avg_call_rho': avg_call_rho,
            'avg_call_theta': avg_call_theta,
            'avg_call_vega': avg_call_vega,
            'avg_call_iv': avg_call_iv,
            'avg_put_delta': avg_put_delta,
            'avg_put_gamma': avg_put_gamma,
            'avg_put_rho': avg_put_rho,
            'avg_put_theta': avg_put_theta,
            'avg_put_vega': avg_put_vega,
            'avg_put_iv': avg_put_iv,


            #options stats


            'total_call_oi': total_call_oi,
            'total_call_net_oi_change': total_call_net_oi_change,
            'total_put_oi': total_put_oi,
            'total_put_net_oi_change': total_put_net_oi_change,
            'total_call_volume': total_call_volume,
            'total_put_volume': total_put_volume,

            'call_buy_vol': formatted_data[0].get('buy_calls_volume', 0),
            'call_bought_avg_price': formatted_data[0].get('buy_calls_avg_price', 0),
            'call_buy_transactions': formatted_data[0].get('buy_calls_transactions', 0),
            
            'call_sell_vol': formatted_data[0].get('sell_calls volume', 0),
            'call_sold_avg_price': formatted_data[0].get('sell_calls_avg_price', 0),
            'call_sell_transactions': formatted_data[0].get('sell_calls_transactions', 0),
            
            'call_neutral_vol': formatted_data[0].get('neutral_calls_volume', 0),
            'call_neutral_avg_price': formatted_data[0].get('neutral_calls_avg_price', 0),
            'call_neutral_transactions': formatted_data[0].get('neutral_calls_transactions', 0),
            
            'call_unknown_vol': formatted_data[0].get('unknown_calls_volume', 0),
            'call_unknown_avg_price': formatted_data[0].get('unknown_calls_avg_price', 0),
            'call_unknown_transactions': formatted_data[0].get('unknown_calls_transactions', 0),
            
            'put_buy_vol': formatted_data[0].get('buy_puts_volume', 0),
            'put_bought_avg_price': formatted_data[0].get('buy_puts_avg_price', 0),
            'put_buy_transactions': formatted_data[0].get('buy_puts_transactions', 0),
            
            'put_sell_vol': formatted_data[0].get('sell_puts_volume', 0),
            'put_sold_avg_price': formatted_data[0].get('sell_puts_avg_price', 0),
            'put_sell_transactions': formatted_data[0].get('sell_puts_transactions', 0),
            
            'put_neutral_vol': formatted_data[0].get('neutral_puts_volume', 0),
            'put_neutral_avg_price': formatted_data[0].get('neutral_puts_avg_price', 0),
            'put_neutral_transactions': formatted_data[0].get('neutral_puts_transactions', 0),
            
            'put_unknown_vol': formatted_data[0].get('unknown_puts_volume', 0),
            'put_unknown_avg_price': formatted_data[0].get('unknown_puts_avg_price', 0),
            'put_unknown_transactions': formatted_data[0].get('unknown_puts_transactions', 0),

            '_1min_rsi': _1min_rsi,
            '_5min_rsi': _5min_rsi,
            '_15min_rsi': _15min_rsi,
            '_30min_rsi': _30min_rsi,
            '_1hour_rsi': _60min_rsi,
            '_2hour_rsi': _120min_rsi,
            '_4hour_rsi': _240min_rsi,
            'day_rsi': day_rsi,
            'week_rsi': week_rsi,


            '_1min_td_buy': _1min_td_buy,
            '_5min_td_buy': _5min_td_buy,
            '_15min_td_buy': _15min_td_buy,
            '_30min_td_buy': _30min_td_buy,
            '_1hour_td_buy': _60min_td_buy,
            '_2hour_td_buy': _120min_td_buy,
            '_4hour_td_buy': _240min_td_buy,
            'day_td_buy': day_td_buy,
            'week_td_buy': week_td_buy,

            '_1min_td_sell': _1min_td_sell,
            '_5min_td_sell': _5min_td_sell,
            '_15min_td_sell': _15min_td_sell,
            '_30min_td_sell': _30min_td_sell,
            '_1hour_td_sell': _60min_td_sell,
            '_2hour_td_sell': _120min_td_sell,
            '_4hour_td_sell': _240min_td_sell,
            'day_td_sell': day_td_sell,
            'week_td_sell': week_td_sell,

            '_1min_upper_bb_trend': m1_df['upper_bb_trend'].to_list()[0],
            '_5min_upper_bb_trend': m5_df['upper_bb_trend'].to_list()[0],
            '_15min_upper_bb_trend': m15_df['upper_bb_trend'].to_list()[0],
            '_30min_upper_bb_trend': m30_df['upper_bb_trend'].to_list()[0],
            '_1hour_upper_bb_trend': m60_df['upper_bb_trend'].to_list()[0],
            '_2hour_upper_bb_trend': m120_df['upper_bb_trend'].to_list()[0],
            '_4hour_upper_bb_trend': m240_df['upper_bb_trend'].to_list()[0],
            'day_upper_bb_trend': d_df['upper_bb_trend'].to_list()[0],
            'week_upper_bb_trend': w_df['upper_bb_trend'].to_list()[0],
    

            '_1min_lower_bb_trend': m1_df['lower_bb_trend'].to_list()[0],
            '_5min_lower_bb_trend': m5_df['lower_bb_trend'].to_list()[0],
            '_15min_lower_bb_trend': m15_df['lower_bb_trend'].to_list()[0],
            '_30min_lower_bb_trend': m30_df['lower_bb_trend'].to_list()[0],
            '_1hour_lower_bb_trend': m60_df['lower_bb_trend'].to_list()[0],
            '_2hour_lower_bb_trend': m120_df['lower_bb_trend'].to_list()[0],
            '_4hour_lower_bb_trend': m240_df['lower_bb_trend'].to_list()[0],
            'day_lower_bb_trend': d_df['lower_bb_trend'].to_list()[0],
            'week_lower_bb_trend': w_df['lower_bb_trend'].to_list()[0],




            '_1min_lower_bb_angle': m1_df['lower_bb_rel_angle'].to_list()[0],
            '_5min_lower_bb_angle': m5_df['lower_bb_rel_angle'].to_list()[0],
            '_15min_lower_bb_angle': m15_df['lower_bb_rel_angle'].to_list()[0],
            '_30min_lower_bb_angle': m30_df['lower_bb_rel_angle'].to_list()[0],
            '_1hour_lower_bb_angle': m60_df['lower_bb_rel_angle'].to_list()[0],
            '_2hour_lower_bb_angle': m120_df['lower_bb_rel_angle'].to_list()[0],
            '_4hour_lower_bb_angle': m240_df['lower_bb_rel_angle'].to_list()[0],
            'day_lower_bb_angle': d_df['lower_bb_rel_angle'].to_list()[0],
            'week_lower_bb_angle': w_df['lower_bb_rel_angle'].to_list()[0],


            '_1min_upper_bb_angle': m1_df['upper_bb_rel_angle'].to_list()[0],
            '_5min_upper_bb_angle': m5_df['upper_bb_rel_angle'].to_list()[0],
            '_15min_upper_bb_angle': m15_df['upper_bb_rel_angle'].to_list()[0],
            '_30min_upper_bb_angle': m30_df['upper_bb_rel_angle'].to_list()[0],
            '_1hour_upper_bb_angle': m60_df['upper_bb_rel_angle'].to_list()[0],
            '_2hour_upper_bb_angle': m120_df['upper_bb_rel_angle'].to_list()[0],
            '_4hour_upper_bb_angle': m240_df['upper_bb_rel_angle'].to_list()[0],
            'day_upper_bb_angle': d_df['upper_bb_rel_angle'].to_list()[0],


            '_1min_middle_bb_angle': m1_df['middle_bb_rel_angle'].to_list()[0],
            '_5min_middle_bb_angle': m5_df['middle_bb_rel_angle'].to_list()[0],
            '_15min_middle_bb_angle': m15_df['middle_bb_rel_angle'].to_list()[0],
            '_30min_middle_bb_angle': m30_df['middle_bb_rel_angle'].to_list()[0],
            '_1hour_middle_bb_angle': m60_df['middle_bb_rel_angle'].to_list()[0],
            '_2hour_middle_bb_angle': m120_df['middle_bb_rel_angle'].to_list()[0],
            '_4hour_middle_bb_angle': m240_df['middle_bb_rel_angle'].to_list()[0],
            'day_middle_bb_angle': d_df['middle_bb_rel_angle'].to_list()[0],
            'week_middle_bb_angle': w_df['middle_bb_rel_angle'].to_list()[0],

            '_1min_macd_curvature': m1_df['macd_curvature'].to_list()[0],
            '_5min_macd_curvature': m5_df['macd_curvature'].to_list()[0],
            '_15min_macd_curvature': m15_df['macd_curvature'].to_list()[0],
            '_30min_macd_curvature': m30_df['macd_curvature'].to_list()[0],
            '_1hour_macd_curvature': m60_df['macd_curvature'].to_list()[0],
            '_2hour_macd_curvature': m120_df['macd_curvature'].to_list()[0],
            '_4hour_macd_curvature': m240_df['macd_curvature'].to_list()[0],
            'day_macd_curvature': d_df['macd_curvature'].to_list()[0],
            'week_macd_curvature': w_df['macd_curvature'].to_list()[0],
  


            '_1min_vwap': m1_df['vwap'].to_list()[0],
            '_5min_vwap': m5_df['vwap'].to_list()[0],
            '_15min_vwap': m15_df['vwap'].to_list()[0],
            '_30min_vwap': m30_df['vwap'].to_list()[0],
            '_1hour_vwap': m60_df['vwap'].to_list()[0],
            '_2hour_vwap': m120_df['vwap'].to_list()[0],
            '_4hour_vwap': m240_df['vwap'].to_list()[0],
            'day_vwap': d_df['vwap'].to_list()[0],
            'week_vwap': w_df['vwap'].to_list()[0],



            'news_sentiment': news.sentiment[0],
            'latest_news_title': news.title[0],
            'latest_news_time': news.newsTime[0],
            'latest_news_summary': news.summary[0],
            'latest_news_source': news.sourceName[0],


            ## ORDER FLOW
            'large_netflow': cap_flow.largeNetFlow,
            'newlarge_netflow': cap_flow.newLargeNetFlow,
            'major_netflow': cap_flow.majorNetFlow,
            'medium_netflow': cap_flow.mediumNetFlow,
            'small_netflow': cap_flow.smallNetFlow,


            ## analyst

            'analyst_suggestion': analyst_suggestion

    }

        

        async with aiohttp.ClientSession(headers={'X-API-KEY': 'fudstop4'}) as session:
            async with session.get(f"https://www.fudstop.io/api/major_holdings?ticker={ticker}") as resp:

                data = await resp.json()

                metric = [i.get('metric') for i in data]
                value = [i.get('value') for i in data]
                for m, v in zip(metric, value):
                    dict[m] = v  # Assign each metric as a key, and its corresponding value

        if not is_etf(ticker):

            eps = multiquote.eps[0]
            eps_ttm = multiquote.epsTtm[0]
            bps = multiquote.bps[0]
            fwd_pe = multiquote.forwardPe[0]
            indicated_pe = multiquote.indicatedPe[0]
            pe_ratio = multiquote.pe[0]
                
            dict.update({'eps': eps, 'eps_ttm': eps_ttm, 'bps': bps, 'fwd_pe': fwd_pe, 'indicated_pe': indicated_pe, 'pe_ratio': pe_ratio})
    

            sec_filings = await yf.sec_filings(ticker)
            cost_dist = await trading.new_cost_dist(ticker, start_date=opts.eight_days_ago, end_date=opts.today)

            profit_ratio = cost_dist.closeProfitRatio[0]

            sec_date, sec_type, sec_url = sec_filings['date'].to_list()[0], sec_filings['type'].to_list()[0], sec_filings['edgar_url'].to_list()[0]
            inst_data = await trading.institutional_holding(ticker)
            owned_by_institutions = inst_data.stat.holding_ratio



        async with aiohttp.ClientSession(headers={'X-API-KEY': 'fudstop4'}) as session:

            async with session.get(f"https://www.fudstop.io/api/volume_summary?ticker={ticker}") as resp:

                data = await resp.json()

                for item in data:
                    expiry = item.get("expiry")  # Ensure expiry exists
                    call_volume = item.get("call_volume", 0)
                    put_volume = item.get("put_volume", 0)

                    # Create column names dynamically
                    calls_col = f"_{expiry.replace('-', '_')}_calls"
                    puts_col = f"_{expiry.replace('-', '_')}_puts"

                    # Store in dictionary
                    dict[calls_col] = call_volume
                    dict[puts_col] = put_volume

                    # Debugging print
                    print(f"Added: {calls_col} -> {call_volume}, {puts_col} -> {put_volume}")

        url = f"https://www.fudstop.io/api/oi_by_strike?ticker={ticker}"

        async with aiohttp.ClientSession(headers={'X-API-KEY': 'fudstop4'}) as session:
            async with session.get(url) as resp:
                data = await resp.json()

                for item in data:
                    strike = item.get("strike")  # Ensure strike exists


                    call_oi = item.get("call_oi", 0)
                    put_oi = item.get("put_oi", 0)
                    call_oi_change = item.get("call_oi_change", 0)
                    put_oi_change = item.get("put_oi_change", 0)  

                    dict[call_oi_change] = call_oi_change
                    dict[put_oi_change] = put_oi_change


                    dict.update({'call_oi': call_oi, 'put_oi': put_oi})

                    # Debugging print
                    print(f"Added: {calls_col} -> {call_oi}, {puts_col} -> {put_oi}")
            


            df = pd.DataFrame(dict, index=[0])


            gex_df = await gamma_exposure(ticker)


            # Drop columns where all values are NA before merging
            df.dropna(axis=1, how='all', inplace=True)
            gex_df.dropna(axis=1, how='all', inplace=True)

            # Merge on 'ticker' and update missing values
            df = df.merge(gex_df, on="ticker", how="left", suffixes=("", "_gex"))

            # Fill missing values from gex_df where applicable
            shared_columns = ['strike', 'expiry', 'oi', 'oi_change', 'iv', 'gamma', 'delta', 'call_put']
            try:
                # Fill missing values from gex_df where applicable
                for col in shared_columns:
                    if col in df.columns and f"{col}_gex" in df.columns:
                        df[col] = df[col].combine_first(df[f"{col}_gex"])  # Fill missing values
                        df.drop(columns=[f"{col}_gex"], inplace=True)  # Drop the duplicate column

                # 🚀 Attempt upsert (Will throw "column does not exist" error if missing)
                await db.batch_upsert_dataframe(df, table_name='master', unique_columns=['ticker'])

            except asyncpg.exceptions.PostgresError as e:
                error_message = str(e)

                # 🔍 Check if the error is a "column does not exist" error (PostgreSQL 42703)
                if "42703" in error_message:
                    missing_column = error_message.split('"')[1]  # Extract column name from error message
                    logging.warning(f"⚠️ Column '{missing_column}' is missing in 'master'. Attempting to add it dynamically.")

                    # 🔍 Infer the column type dynamically (default to TEXT)
                    inferred_type = "TEXT"
                    if any(keyword in missing_column for keyword in ["oi", "gamma", "delta", "iv", "calls", "puts"]):
                        inferred_type = "NUMERIC"

                    # 🚀 Add the missing column dynamically
                    await db.add_column_to_table(table_name='master', column_name=missing_column, column_type=inferred_type)

                    # ✅ Retry the operation now that the column exists
                    await db.batch_upsert_dataframe(df, table_name='master', unique_columns=['ticker'])
                    logging.info(f"✅ Successfully re-executed batch upsert after adding '{missing_column}'.")
                else:
                    logging.error(f"❌ Database Error: {e}")

            except Exception as e:
                logging.error(f"❌ General Error: {e}")

    except UndefinedColumnError as error:
        print(error)
    except Exception as e:
        print(e)

from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
async def process_in_batches(batch_size=7):
    """
    Process tickers in batches of `batch_size`.
    """
    await db.connect()

    # Create an iterator over the tickers
    ticker_iter = iter(most_active_tickers)

    while True:
        for i in most_active_tickers:

            await all(i)
     

asyncio.run(process_in_batches())


