import asyncio
from apis.polygonio.async_polygon_sdk import Polygon
from list_sets.ticker_lists import most_active_tickers
polygon = Polygon(host='localhost', user='chuck', database='market_data', password='fud', port=5432)
async def get_2_4_8_rsi(ticker, timespan, window):
    windows_thresholds = {
        '2': (1, 99),  # (low_threshold, high_threshold)
        '4': (10, 90),
        '8': (20, 80),
    }
    try:

        # Assuming the polygon.rsi method returns an instance of RSI class
        rsi_obj = await polygon.rsi(ticker, timespan=timespan, window=str(window), limit=1)
        if rsi_obj and hasattr(rsi_obj, 'rsi_value'):
            rsi = rsi_obj.rsi_value
            if isinstance(rsi, list) and rsi:  # Ensure rsi is a list and not empty
                rsi_value = rsi[0]
                low_threshold, high_threshold = windows_thresholds[str(window)]
                if rsi_value <= low_threshold or rsi_value >= high_threshold:
                    return (ticker, rsi_value, window)
    except Exception as e:
        print(f"Error processing {ticker} for window {window}: {e}")
    return None

async def scan_2_4_8():
    windows = [2, 4, 8]
    tasks = [get_2_4_8_rsi(ticker, 'day', window) for ticker in most_active_tickers for window in windows]
    results = await asyncio.gather(*tasks)
    
    # Filter out None results
    valid_results = [result for result in results if result]
    
    # Now, check if any ticker meets the criteria for all windows
    ticker_results = {}
    for result in valid_results:
        ticker, rsi, window = result
        if ticker not in ticker_results:
            ticker_results[ticker] = []
        ticker_results[ticker].append(window)
    
    # Find tickers that have all windows
    all_results=[]
    for ticker, windows_list in ticker_results.items():
        if all(w in windows_list for w in windows):
            all_results.append(ticker)



    return all_results