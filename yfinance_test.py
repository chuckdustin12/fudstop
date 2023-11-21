from fudstop.apis.y_finance.yf_sdk import yfSDK


import yfinance as yf



candles = yf.Ticker('AAPL')._download_options()

print(candles)
