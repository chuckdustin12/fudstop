import requests

def get_ticker_id(symbol: str):
    url = f"https://quotes-gw.webullfintech.com/api/search/pc/tickers?brokerId=8&keyword={symbol}&pageIndex=1&pageSize=20"


    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    print(data)
    matches = data.get("data", [])
    
    # Filter by exact match on symbol if possible
    for item in matches:
        if item.get("symbol", "").upper() == symbol.upper():
            return item["tickerId"]

    # If no exact match, return first available result
    if matches:
        return matches[0]["tickerId"]

    return None

# Example usage:
if __name__ == "__main__":
    ticker = "AMC"
    ticker_id = get_ticker_id(ticker)
    if ticker_id:
        print(f"Ticker ID for {ticker}: {ticker_id}")
    else:
        print(f"Ticker ID for {ticker} not found.")
