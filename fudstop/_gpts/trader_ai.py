from imps import *




async def rsi_check(db, rsi_threshold: int, condition: str, timeframes: list):

    # Create the condition query based on oversold or overbought
    if condition == "oversold":
        query_conditions = " AND ".join([f"rsi_{tf} <= {rsi_threshold}" for tf in timeframes])
        description = f"These are tickers that are oversold across multiple timeframes with RSI near or below {rsi_threshold}."
    elif condition == "overbought":
        query_conditions = " AND ".join([f"rsi_{tf} >= {rsi_threshold}" for tf in timeframes])
        description = f"These are tickers that are overbought across multiple timeframes with RSI near or above {rsi_threshold}."

    # Add a condition to filter records from the last 3 minutes
    time_condition = "insertion_timestamp >= NOW() - INTERVAL '3 minutes'"

    # Combine the conditions
    query = f"SELECT DISTINCT ticker FROM master_all_two WHERE {query_conditions} AND {time_condition}"

    results = await db.fetch(query)

    # Convert the list of Record objects to a DataFrame
    if results:
        df = pd.DataFrame([dict(record) for record in results])  # Unpack the Record objects
        print("DataFrame columns:", df.columns)
        
        # If 'ticker' is in the DataFrame, return it as a dictionary
        if 'ticker' in df.columns:
            return df[['ticker']].to_dict(orient='list')
        else:
            print("Ticker column not found in DataFrame")
            return {'ticker': []}
 


async def macd_multi(db, signal: str, timeframes: list):


    # Create the condition query based on the MACD signal (bullish or bearish)
    query_conditions = " AND ".join([f"macd_{tf} = '{signal}'" for tf in timeframes])

    # Add a condition to filter records from the last 3 minutes
    time_condition = "insertion_timestamp >= NOW() - INTERVAL '3 minutes'"

    # Combine the conditions
    query = f"SELECT DISTINCT ticker FROM master_all_two WHERE {query_conditions} AND {time_condition}"

    results = await db.fetch(query)

    # Convert the list of Record objects to a DataFrame
    if results:
        df = pd.DataFrame([dict(record) for record in results])  # Unpack the Record objects
        print("DataFrame columns:", df.columns)

        # If 'ticker' is in the DataFrame, return it as a dictionary
        if 'ticker' in df.columns:
            return df[['ticker']].to_dict(orient='list')
        else:
            print("Ticker column not found in DataFrame")
            return {'ticker': []}






async def td9_check(db, signal: str, timeframes: list):

    # Create the condition query based on the TD9 signal ("BUY" or "SELL") for the given timeframes
    query_conditions = " AND ".join([f"td9_{tf} = '{signal}'" for tf in timeframes])

    # Add a condition to filter records from the last 3 minutes (if necessary)
    time_condition = "insertion_timestamp >= NOW() - INTERVAL '3 minutes'"

    # Combine the conditions with the time filter
    query = f"SELECT DISTINCT ticker FROM master_all_two WHERE {query_conditions} AND {time_condition}"

    results = await db.fetch(query)

    # Convert the list of Record objects to a DataFrame
    if results:
        df = pd.DataFrame([dict(record) for record in results])  # Unpack the Record objects
        print("DataFrame columns:", df.columns)

        # If 'ticker' is in the DataFrame, return it as a dictionary
        if 'ticker' in df.columns:
            return df[['ticker']].to_dict(orient='list')
        else:
            print("Ticker column not found in DataFrame")
            return {'ticker': []}
    else:
        return {'ticker': []}





"1727720400,569.89,569.41,569.90,569.41,571.47,828471,571.24"

"1727720340,569.97,569.89,570.20,569.84,571.47,179806,571.23"