#!/usr/bin/env python3
"""
Module: stock_quotes.py
-------------------------
This module defines classes to process stock quote data from the Polygon.io API.

Classes:
    - StockQuotes: Processes a list of stock quote dictionaries and produces a Pandas DataFrame.
    - LastStockQuote: Processes a single stock quote dictionary and produces a Pandas DataFrame.

Usage:
    >>> from stock_quotes import StockQuotes, LastStockQuote
    >>> quotes = StockQuotes(results)  # where results is a list of dictionaries
    >>> df_quotes = quotes.as_dataframe
    >>> last_quote = LastStockQuote(result_dict)  # where result_dict is a dictionary
    >>> df_last_quote = last_quote.as_dataframe
"""

import pandas as pd


class StockQuotes:
    """
    Processes a list of stock quotes returned from an API response and converts them into a DataFrame.

    Attributes:
        as_dataframe (pd.DataFrame): DataFrame representation of the processed stock quotes.
    """

    def __init__(self, results: list[dict]) -> None:
        """
        Initialize the StockQuotes object.

        Args:
            results (list[dict]): A list of dictionaries containing stock quote data.
        """
        # Extract and process each field using list comprehensions.
        self.ask_exchange = [item.get('ask_exchange') for item in results]
        self.ask_price = [item.get('ask_price') for item in results]
        self.ask_size = [item.get('ask_size') for item in results]
        self.bid_exchange = [item.get('bid_exchange') for item in results]
        self.bid_price = [item.get('bid_price') for item in results]
        self.bid_size = [item.get('bid_size') for item in results]
        # Ensure that indicators are a list to avoid errors during join.
        self.indicators = [','.join(item.get('indicators') or []) for item in results]
        self.participant_timestamp = [item.get('participant_timestamp') for item in results]
        self.sequence_number = [item.get('sequence_number') for item in results]
        self.sip_timestamp = [item.get('sip_timestamp') for item in results]
        self.tape = [item.get('tape') for item in results]

        self.data_dict = { 
            'ask_exchange': self.ask_exchange,
            'bid_exchange': self.bid_exchange,
            'ask_size': self.ask_size,
            'ask_price': self.ask_price,
            'bid_size': self.bid_size,
            'bid_price': self.bid_price,
            'timestamp': self.sip_timestamp,
            'tape': self.tape
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)


class LastStockQuote:
    """
    Processes a single stock quote response and converts it into a DataFrame.

    Attributes:
        as_dataframe (pd.DataFrame): DataFrame representation of the processed stock quote.
    """

    def __init__(self, results: dict) -> None:
        """
        Initialize the LastStockQuote object.

        Args:
            results (dict): A dictionary containing the last stock quote data.
        """
        self.ask_price = results.get('P', 0)
        self.ask_size = results.get('S', 0)
        self.ticker = results.get('T')
        self.ask_exchange = results.get('X')
        self.indicators = results.get('i', 0)
        self.bid_price = results.get('p')
        self.bid_size = results.get('s')
        self.timestamp = results.get('t')
        self.bid_exchange = results.get('x')
        self.tape = results.get('z')
        self.conditions = results.get('c', 0)

        self.data_dict = { 
            'ask': self.ask_price,
            'ask_size': self.ask_size,
            'ask_exchange': self.ask_exchange,
            'bid': self.bid_price,
            'bid_size': self.bid_size,
            'bid_exchange': self.bid_exchange,
            'indicators': self.indicators,
            'conditions': self.conditions,
            'timestamp': self.timestamp,
            'tape': self.tape
        }

        # Create a single-row DataFrame
        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])


# if __name__ == '__main__':
#     # Example usage for testing purposes
#     sample_results = [
#         {
#             "ask_exchange": "NYSE",
#             "ask_price": 150.25,
#             "ask_size": 100,
#             "bid_exchange": "NASDAQ",
#             "bid_price": 150.15,
#             "bid_size": 200,
#             "indicators": ["A", "B"],
#             "participant_timestamp": "2024-01-01T12:00:00Z",
#             "sequence_number": 123456,
#             "sip_timestamp": "2024-01-01T12:00:05Z",
#             "tape": "1"
#         }
#     ]
#     stock_quotes = StockQuotes(sample_results)
#     print("StockQuotes DataFrame:")
#     print(stock_quotes.as_dataframe)

#     sample_last_quote = {
#         "P": 150.25,
#         "S": 100,
#         "T": "AAPL",
#         "X": "NYSE",
#         "i": "A",
#         "p": 150.15,
#         "s": 200,
#         "t": "2024-01-01T12:00:05Z",
#         "x": "NASDAQ",
#         "z": "1",
#         "c": [1, 2]
#     }
#     last_quote = LastStockQuote(sample_last_quote)
#     print("\nLastStockQuote DataFrame:")
#     print(last_quote.as_dataframe)
