from fudstop4.apis.polygonio.polygon_options import PolygonOptions

from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
opts = PolygonOptions()



import asyncio

import pandas as pd

async def main():
    tickers_placeholder = ', '.join(f"'{ticker}'" for ticker in most_active_tickers)
    await opts.connect()
    query = f"""WITH RankedShorts AS (
    SELECT
        symbol,
        date,
        percent_shorted,
        -- Assign a grouping number whenever percent_shorted drops below 60%
        SUM(CASE WHEN percent_shorted >= 60 THEN 0 ELSE 1 END) 
            OVER (PARTITION BY symbol ORDER BY date) AS streak_group
    FROM public.short_vol
    WHERE symbol IN ({tickers_placeholder}) -- Pre-defined ticker list
),
Streaks AS (
    SELECT
        symbol,
        COUNT(*) AS num_days_over_60,
        MAX(percent_shorted) AS max_percent_shorted,
        MIN(date) AS start_date,
        MAX(date) AS end_date
    FROM RankedShorts
    WHERE percent_shorted >= 60 -- Only count days where percent shorted is above 60%
    GROUP BY symbol, streak_group
)
SELECT
    symbol,
    max_percent_shorted AS highest_percent_shorted,
    num_days_over_60 AS streak_length,
    start_date,
    end_date
FROM Streaks
ORDER BY num_days_over_60 DESC, highest_percent_shorted DESC;"""
    

    results = await opts.fetch(query)


    df = pd.DataFrame(results, columns=['ticker', 'pct_shorted', 'streak', 'start', 'fin'])

    await opts.batch_upsert_dataframe(df, table_name='short_streak', unique_columns=['ticker', 'streak'])


asyncio.run(main())