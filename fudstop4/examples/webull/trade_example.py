from apis.webull.webull_trading import WebullTrading

import asyncio

wb = WebullTrading()



async def main():
    ticker='AAPL'
    analyst_ratings = await wb.get_analyst_ratings(ticker)


    #DOT NOTATION
    strong_buy = analyst_ratings.strongbuy
    buy = analyst_ratings.buy
    sell = analyst_ratings.sell
    hold = analyst_ratings.hold
    underperform = analyst_ratings.underperform
    print(f"Strong: {strong_buy} | Buy: {buy} | Hold: {hold} | Underperform: {underperform} | Sell: {sell}")


    data_dict = analyst_ratings.data_dict

    df = analyst_ratings.df
    df['ticker'] = ticker

    print(df)




import asyncio

async def all_main():
    # Initialize the WebullTrading class
    webull_trading = WebullTrading()

    # Define a sample ticker symbol for demonstration
    sample_ticker = 'AAPL'

    # Run each function one by one
    bars = await webull_trading.get_bars(sample_ticker)
    print("Bars:", bars)

    stock_quote = await webull_trading.get_stock_quote(sample_ticker)
    print("Stock Quote:", stock_quote.df)

    analyst_ratings = await webull_trading.get_analyst_ratings(sample_ticker)
    print("Analyst Ratings:", analyst_ratings.df)

    short_interest = await webull_trading.get_short_interest(sample_ticker)
    print("Short Interest:", short_interest.df)

    inst_holding = await webull_trading.institutional_holding(sample_ticker)
    print("Institutional Holding:", inst_holding.to_dict())

    vol_analysis = await webull_trading.volume_analysis(sample_ticker)
    print("Volume Analysis:", vol_analysis.df)

    cost_dist = await webull_trading.cost_distribution(sample_ticker)
    print("Cost Distribution:", cost_dist.df)

    news_data = await webull_trading.news(sample_ticker)
    print("News:", news_data.df)

    balance_sheet_data = await webull_trading.balance_sheet(sample_ticker)
    print("Balance Sheet:", balance_sheet_data.df)

    cash_flow_data = await webull_trading.cash_flow(sample_ticker)
    print("Cash Flow:", cash_flow_data.df)

    income_statement_data = await webull_trading.income_statement(sample_ticker)
    print("Income Statement:", income_statement_data.df)

    capital_flow_data = await webull_trading.capital_flow(sample_ticker)
    print("Capital Flow:", capital_flow_data.df)

    etf_holdings_data = await webull_trading.etf_holdings(sample_ticker)
    print("ETF Holdings:", etf_holdings_data.df)




asyncio.run(all_main())
#asyncio.run(main())