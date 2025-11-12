import datetime
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.webull.webull_trading import WebullTrading
opts = PolygonOptions(database='fudstop3')
trading = WebullTrading()
import pandas as pd
def serialize_record(record):
    # Check if the record is a Pandas DataFrame or Series
    if isinstance(record, pd.DataFrame):
        # Convert DataFrame to a dictionary
        return record.to_dict(orient='records')
    elif isinstance(record, pd.Series):
        # Convert Series to a list
        return record.tolist()
    else:
        # For any other type, return it as is or handle serialization differently
        return record

def test():
    return 'Ok'

available_functions = {
    "short_interest": trading.get_short_interest,
    "news": trading.news,
    "analyst_ratings": trading.get_analyst_ratings,
    "volume_analysis": trading.volume_analysis,
    "institutional_holding": trading.institutional_holding,
    "balance_sheet": trading.balance_sheet,
    "income_statement": trading.income_statement,
    "cash_flow": trading.cash_flow,
    "company_brief": trading.company_brief,
    "cost_distribution": trading.cost_distribution,
    "capital_flow": trading.capital_flow,
    "etf_holdings": trading.etf_holdings,
    "multi_quote": trading.multi_quote,
    "stock_quote": trading.stock_quote,
}


tools =[
    {
    "type": "function",
    "function": {
        "name": "filter_options",
        "description": "Filter options based on several different keyword arguments.",
        "parameters": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "Identifier for the ticker."
                },
                "ticker_symbol": {
                    "type": "string",
                    "description": "Ticker symbol."
                },
                "strike": {
                    "type": "object",
                    "properties": {
                        "name": "strike",
                        "type": "float",
                        "description": "Strike price."
                    }
                },
                "strike_min": {
                    "type": "object",
                    "properties": {
                        "name": "strike_min",
                        "type": "float",
                        "description": "Minimum strike price."
                    }
                },
                "strike_max": {
                    "type": "object",
                    "properties": {
                        "name": "strike_max",
                        "type": "float",
                        "description": "Maximum strike price."
                    }
                },
                "expiry": {
                    "type": "object",
                    "properties": {
                        "name": "expiry",
                        "type": "date",
                        "description": "Expiry date."
                    }
                },
                "expiry_min": {
                    "type": "object",
                    "properties": {
                        "name": "expiry_min",
                        "type": "date",
                        "description": "Minimum expiry date."
                    }
                },
                "expiry_max": {
                    "type": "object",
                    "properties": {
                        "name": "expiry_max",
                        "type": "date",
                        "description": "Maximum expiry date."
                    }
                },
                "open": {
                    "type": "object",
                    "properties": {
                        "name": "open",
                        "type": "float",
                        "description": "Open price."
                    }
                },
                "open_min": {
                    "type": "object",
                    "properties": {
                        "name": "open_min",
                        "type": "float",
                        "description": "Minimum open price."
                    }
                },
                "open_max": {
                    "type": "object",
                    "properties": {
                        "name": "open_max",
                        "type": "float",
                        "description": "Maximum open price."
                    }
                },
                "high": {
                    "type": "object",
                    "properties": {
                        "name": "high",
                        "type": "float",
                        "description": "High price."
                    }
                },
                "high_min": {
                    "type": "object",
                    "properties": {
                        "name": "high_min",
                        "type": "float",
                        "description": "Minimum high price."
                    }
                },
                "high_max": {
                    "type": "object",
                    "properties": {
                        "name": "high_max",
                        "type": "float",
                        "description": "Maximum high price."
                    }
                },
                "low": {
                    "type": "object",
                    "properties": {
                        "name": "low",
                        "type": "float",
                        "description": "Low price."
                    }
                },
                "low_min": {
                    "type": "object",
                    "properties": {
                        "name": "low_min",
                        "type": "float",
                        "description": "Minimum low price."
                    }
                },
                "low_max": {
                    "type": "object",
                    "properties": {
                        "name": "low_max",
                        "type": "float",
                        "description": "Maximum low price."
                    }
                },
                "oi": {
                    "type": "object",
                    "properties": {
                        "name": "oi",
                        "type": "float",
                        "description": "Open Interest."
                    }
                },
                "oi_min": {
                    "type": "object",
                    "properties": {
                        "name": "oi_min",
                        "type": "float",
                        "description": "Minimum Open Interest."
                    }
                },
                "oi_max": {
                    "type": "object",
                    "properties": {
                        "name": "oi_max",
                        "type": "float",
                        "description": "Maximum Open Interest."
                    }
                },
                "vol": {
                    "type": "object",
                    "properties": {
                        "name": "vol",
                        "type": "float",
                        "description": "Volume."
                    }
                },
                "vol_min": {
                    "type": "object",
                    "properties": {
                        "name": "vol_min",
                        "type": "float",
                        "description": "Minimum Volume."
                    }
                },
                "vol_max": {
                    "type": "object",
                    "properties": {
                        "name": "vol_max",
                        "type": "float",
                        "description": "Maximum Volume."
                    }
                },
                "delta": {
                    "type": "object",
                    "properties": {
                        "name": "delta",
                        "type": "float",
                        "description": "Delta."
                    }
                },
                "delta_min": {
                    "type": "object",
                    "properties": {
                        "name": "delta_min",
                        "type": "float",
                        "description": "Minimum Delta."
                    }
                },
                "delta_max": {
                    "type": "object",
                    "properties": {
                        "name": "delta_max",
                        "type": "float",
                        "description": "Maximum Delta."
                    }
                },
                "vega": {
                    "type": "object",
                    "properties": {
                        "name": "vega",
                        "type": "float",
                        "description": "Vega."
                    }
                },
                "vega_min": {
                    "type": "object",
                    "properties": {
                        "name": "vega_min",
                        "type": "float",
                        "description": "Minimum Vega."
                    }
                },
                "vega_max": {
                    "type": "object",
                    "properties": {
                        "name": "vega_max",
                        "type": "float",
                        "description": "Maximum Vega."
                    }
                },
                "iv": {
                    "type": "object",
                    "properties": {
                        "name": "iv",
                        "type": "float",
                        "description": "Implied Volatility."
                    }
                },
                "iv_min": {
                    "type": "object",
                    "properties": {
                        "name": "iv_min",
                        "type": "float",
                        "description": "Minimum Implied Volatility."
                    }
                },
                "iv_max": {
                    "type": "object",
                    "properties": {
                        "name": "iv_max",
                        "type": "float",
                        "description": "Maximum Implied Volatility."
                    }
                },
                "dte": {
                    "type": "object",
                    "properties": {
                        "name": "dte",
                        "type": "string",
                        "description": "Days to Expiry."
                    }
                },
                "dte_min": {
                    "type": "object",
                    "properties": {
                        "name": "dte_min",
                        "type": "string",
                        "description": "Minimum Days to Expiry."
                    }
                },
                "dte_max": {
                    "type": "object",
                    "properties": {
                        "name": "dte_max",
                        "type": "string",
                        "description": "Maximum Days to Expiry."
                    }
                },
                "gamma": {
                    "type": "object",
                    "properties": {
                        "name": "gamma",
                        "type": "float",
                        "description": "Gamma."
                    }
                },
                "gamma_min": {
                    "type": "object",
                    "properties": {
                        "name": "gamma_min",
                        "type": "float",
                        "description": "Minimum Gamma."
                    }
                },
                "gamma_max": {
                    "type": "object",
                    "properties": {
                        "name": "gamma_max",
                        "type": "float",
                        "description": "Maximum Gamma."
                    }
                },
                "theta": {
                    "type": "object",
                    "properties": {
                        "name": "theta",
                        "type": "float",
                        "description": "Theta."
                    }
                },
                "theta_min": {
                    "type": "object",
                    "properties": {
                        "name": "theta_min",
                        "type": "float",
                        "description": "Minimum Theta."
                    }
                },
                "theta_max": {
                    "type": "object",
                    "properties": {
                        "name": "theta_max",
                        "type": "float",
                        "description": "Maximum Theta."
                    }
                },
                "sensitivity": {
                    "type": "object",
                    "properties": {
                        "name": "sensitivity",
                        "type": "float",
                        "description": "Sensitivity."
                    }
                },
                "sensitivity_min": {
                    "type": "object",
                    "properties": {
                        "name": "sensitivity_min",
                        "type": "float",
                        "description": "Minimum Sensitivity."
                    }
                },
                "sensitivity_max": {
                    "type": "object",
                    "properties": {
                        "name": "sensitivity_max",
                        "type": "float",
                        "description": "Maximum Sensitivity."
                    }
                },
                "bid": {
                    "type": "object",
                    "properties": {
                        "name": "bid",
                        "type": "float",
                        "description": "Bid price."
                    }
                },
                "bid_min": {
                    "type": "object",
                    "properties": {
                        "name": "bid_min",
                        "type": "float",
                        "description": "Minimum Bid price."
                    }
                },
                "bid_max": {
                    "type": "object",
                    "properties": {
                        "name": "bid_max",
                        "type": "float",
                        "description": "Maximum Bid price."
                    }
                },
                "ask": {
                    "type": "object",
                    "properties": {
                        "name": "ask",
                        "type": "float",
                        "description": "Ask price."
                    }
                },
                "ask_min": {
                    "type": "object",
                    "properties": {
                        "name": "ask_min",
                        "type": "float",
                        "description": "Minimum Ask price."
                    }
                },
                "ask_max": {
                    "type": "object",
                    "properties": {
                        "name": "ask_max",
                        "type": "float",
                        "description": "Maximum Ask price."
                    }
                },
                "close": {
                    "type": "object",
                    "properties": {
                        "name": "close",
                        "type": "float",
                        "description": "Close price."
                    }
                },
                "close_min": {
                    "type": "object",
                    "properties": {
                        "name": "close_min",
                        "type": "float",
                        "description": "Minimum Close price."
                    }
                },
                "close_max": {
                    "type": "object",
                    "properties": {
                        "name": "close_max",
                        "type": "float",
                        "description": "Maximum Close price."
                    }
                },
                "cp": {
                    "type": "object",
                    "properties": {
                        "name": "cp",
                        "type": "string",
                        "description": "Call or Put."
                    }
                },
                "time_value": {
                    "type": "object",
                    "properties": {
                        "name": "time_value",
                        "type": "float",
                        "description": "Time Value."
                    }
                },
                "time_value_min": {
                    "type": "object",
                    "properties": {
                        "name": "time_value_min",
                        "type": "float",
                        "description": "Minimum Time Value."
                    }
                },
                "time_value_max": {
                    "type": "object",
                    "properties": {
                        "name": "time_value_max",
                        "type": "float",
                        "description": "Maximum Time Value."
                    }
                },
                "moneyness": {
                    "type": "object",
                    "properties": {
                        "name": "moneyness",
                        "type": "string",
                        "description": "Moneyness."
                    }
                },
                "exercise_style": {
                    "type": "object",
                    "properties": {
                        "name": "exercise_style",
                        "type": "string",
                        "description": "Exercise Style."
                    }
                },
                "option_symbol": {
                    "type": "object",
                    "properties": {
                        "name": "option_symbol",
                        "type": "string",
                        "description": "Option Symbol."
                    }
                },
                "theta_decay_rate": {
                    "type": "object",
                    "properties": {
                        "name": "theta_decay_rate",
                        "type": "float",
                        "description": "Theta Decay Rate."
                    }
                },
                "theta_decay_rate_min": {
                    "type": "object",
                    "properties": {
                        "name": "theta_decay_rate_min",
                        "type": "float",
                        "description": "Minimum Theta Decay Rate."
                    }
                },
                "theta_decay_rate_max": {
                    "type": "object",
                    "properties": {
                        "name": "theta_decay_rate_max",
                        "type": "float",
                        "description": "Maximum Theta Decay Rate."
                    }
                },
                "delta_theta_ratio": {
                    "type": "object",
                    "properties": {
                        "name": "delta_theta_ratio",
                        "type": "float",
                        "description": "Delta Theta Ratio."
                    }
                },
                "delta_theta_ratio_min": {
                    "type": "object",
                    "properties": {
                        "name": "delta_theta_ratio_min",
                        "type": "float",
                        "description": "Minimum Delta Theta Ratio."
                    }
                },
                "delta_theta_ratio_max": {
                    "type": "object",
                    "properties": {
                        "name": "delta_theta_ratio_max",
                        "type": "float",
                        "description": "Maximum Delta Theta Ratio."
                    }
                },

            "required": ["ticker"]
            }
            
        }
        }
    }

]


fudstop_tools = [
    {
        "type": "function",
        "function": {
            "name": "all_poly_options",
            "description": "Retrieves options data from Polygon for a given ticker.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "contract_type": {"type": "string", "description": "The type of options contracts to retrieve.", "default": None},
                    "strike_price_gte": {"type": "number", "description": "Filter for options with strike price greater than or equal to this value.", "default": None},
                    "strike_price_lte": {"type": "number", "description": "Filter for options with strike price less than or equal to this value.", "default": None},
                    "expiry_date_gte": {"type": "string", "description": "Filter for options with expiry date greater than or equal to this value.", "default": None},
                    "expiry_date_lte": {"type": "string", "description": "Filter for options with expiry date less than or equal to this value.", "default": None},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "all_webull_options",
            "description": "Retrieves options data from Webull for a given ticker.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "occ_options",
            "description": "Retrieves options data from the Options Clearing Corporation for a given ticker.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_analysts",
            "description": "Retrieves analysts' ratings and price targets for a given ticker from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_capital_flow",
            "description": "Retrieves capital flow data for a given ticker from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_etf_holdings",
            "description": "Retrieves ETF holdings data for a given ticker from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the ETF."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
   {
        "type": "function",
        "function": {
            "name": "webull_financials",
            "description": "Retrieves financial data for a given ticker from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "type": {"type": "string", "description": "The type of financial data to retrieve, such as 'balancesheet'.", "default": "balancesheet"},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_highs_lows",
            "description": "Retrieves data on new highs and lows for stocks from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "description": "The type of highs or lows data to retrieve, such as 'newHigh'.", "default": "newHigh"},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 20},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_institutions",
            "description": "Retrieves institutional holdings data for a given ticker from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_news",
            "description": "Retrieves the latest news for a given ticker from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of news articles to return.", "default": 5},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_short_interest",
            "description": "Retrieves short interest data for a given ticker from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_top_active",
            "description": "Retrieves the list of top active stocks from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "description": "The type of top active stocks to retrieve, such as 'rvol10'.", "default": "rvol10"},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 20},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": []
            }
        }
    },
    # Additional entries for other functions would continue in the same format...
    {
        "type": "function",
        "function": {
            "name": "webull_top_gainers",
            "description": "Retrieves the list of top gainers from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "description": "The type of top gainers to retrieve, such as 'preMarket'.", "default": "preMarket"},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 20},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_top_losers",
            "description": "Retrieves the list of top losers from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "description": "The type of top losers to retrieve, such as 'preMarket'.", "default": "preMarket"},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 20},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_top_options",
            "description": "Retrieves the list of top options from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "description": "The type of top options to retrieve, such as 'volume'.", "default": "volume"},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 20},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "webull_vol_anal",
            "description": "Retrieves volume analysis data for a given ticker from Webull.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "The ticker symbol for the stock."},
                    "limit": {"type": "integer", "description": "The maximum number of results to return.", "default": 50},
                    "insert": {"type": "boolean", "description": "Flag to determine if the data should be inserted into a database.", "default": False}
                },
                "required": ["ticker"]
            }
        }
    }
]