import asyncio
import pandas as pd
from fudstop4.apis.webull.webull_trading import WebullTrading
from fudstop4._markets.list_sets.ticker_lists import energy
trading = WebullTrading()



async def get_short_pct_float(ticker):
    """
    Fetches short interest, shares outstanding, and volume data for a given ticker.
    Calculates Short % of Float.
    """
    try:
        # Fetch short interest and multi-quote data
        short_int = await trading.get_short_interest(ticker)
        multi_quote = await trading.multi_quote(ticker)

        # Ensure the response objects contain data
        if not short_int or not multi_quote:
            print(f"⚠️ No data found for {ticker}")
            return None

        # Use dot notation to access attributes correctly
        if multi_quote.close[0] < 1.00:  # Ignore penny stocks
            return None

        short_interest = short_int.short_int[0]  # Accessing list element
        shares_outstanding = multi_quote.outstandingShares[0]  # Access list element
        avg_volume = multi_quote.avgVol10D[0]  # Access list element
        today_volume = multi_quote.volume[0]  # Access today's volume

        # Ensure values are numeric and prevent division by zero
        if shares_outstanding and shares_outstanding > 0:
            short_pct_float = (float(short_interest) / shares_outstanding) * 100
        else:
            return None  # Skip if shares_outstanding is invalid

        # Check if stock qualifies
        low_float = shares_outstanding < 50_000_000  # Low float < 50M shares
        high_short = float(short_pct_float) > 20  # High Short % of Float > 20%
        low_volume = today_volume < float(avg_volume)  # Today's volume lower than avg

        if low_float and high_short and low_volume:
            return {
                "Ticker": ticker,
                "Short Interest": short_interest,
                "Shares Outstanding": shares_outstanding,
                "Short % of Float": round(short_pct_float, 2),
                "10D Avg Volume": avg_volume,
                "Today Volume": today_volume
            }
        else:
            return None  # Doesn't qualify

    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")
        return None

async def run_scanner():
    """
    Runs the scanner on all tickers in the energy sector, filtering for low float, high short stocks.
    Returns and prints the final DataFrame.
    """
    tasks = [get_short_pct_float(ticker) for ticker in energy]
    results = await asyncio.gather(*tasks)

    # Filter out None values
    filtered_results = [r for r in results if r is not None]

    # Convert to DataFrame
    if filtered_results:
        df = pd.DataFrame(filtered_results)
        print("\n📊 Final Low Float High Short Scanner Results:\n")
        print(df)
        return df
    else:
        print("\n⚠️ No qualifying stocks found.\n")
        return None

# Run the scanner
if __name__ == "__main__":
    asyncio.run(run_scanner())