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
        "type": "get_stock_quote",
        "function": {
            "name": "get_stock_quote",
            "description": "Gets stock quote for a given symbol.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "The stock symbol to query."},
                },
                "required": ["symbol"]
            }
        }
    },
    {
        "type": "analyst_ratings",
        "function": {
            "name": "get_analyst_ratings",
            "description": "Fetches analyst ratings for a given stock symbol.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "The stock symbol to query."},
                },
                "required": ["symbol"]
            }
        }
    },
    {
        "type": "short_interest",
        "function": {
            "name": "get_short_interest",
            "description": "Retrieves short interest data for a given stock symbol.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "The stock symbol to query."},
                },
                "required": ["symbol"]
            }
        }
    },
    {
        "type": "institutional_holding",
        "function": {
            "name": "institutional_holding",
            "description": "Gets institutional holding information for a given stock symbol.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "The stock symbol to query."},
                },
                "required": ["symbol"]
            }
        }
    },
    {
        "type": "volume_analysis",
        "function": {
            "name": "volume_analysis",
            "description": "Performs volume analysis for a given stock symbol.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "The stock symbol to query."},
                },
                "required": ["symbol"]
            }
        }
    },
    {
        "type": "cost_distribution",
        "function": {
            "name": "cost_distribution",
            "description": "Analyzes cost distribution for a given stock symbol within a specified date range.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "The stock symbol to query."},
                    "start_date": {"type": "string", "description": "The start date for the query."},
                    "end_date": {"type": "string", "description": "The end date for the query."},
                },
                "required": ["symbol"]
            }
        }
    },
        {
        "type": "get_skew",
        "function": {
            "name": "get_skew",
            "description": "Gets the implied volatility skew for a ticker.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "The stock symbol to query."},
                },
                "required": ["symbol"]
            }
        }
    },

        {
        "type": "multi_quote",
        "function": {
            "name": "multi_quote",
            "description": "Get multiple stock quotes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbols": {"type": "list", "description": "The stock symbols to query."},
                },
                "required": ["symbols"]
            }
        }
    },

{
    "type": "balance_sheet",
    "function": {
        "name": "balance_sheet",
        "description": "Retrieves balance sheet data for a given stock symbol.",
        "parameters": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "The stock symbol to query."},
            },
            "required": ["symbol"]
        }
    }
},
{
    "type": "income_statement",
    "function": {
        "name": "income_statement",
        "description": "Fetches income statement information for a given stock symbol.",
        "parameters": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "The stock symbol to query."},
            },
            "required": ["symbol"]
        }
    }
},
{
    "type": "cash_flow",
    "function": {
        "name": "cash_flow",
        "description": "Obtains cash flow data for a given stock symbol.",
        "parameters": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "The stock symbol to query."},
            },
            "required": ["symbol"]
        }
    }
},
{
    "type": "company_brief",
    "function": {
        "name": "company_brief",
        "description": "Provides a brief overview of a company associated with a given stock symbol.",
        "parameters": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "The stock symbol to query."},
            },
            "required": ["symbol"]
        }
    }
},
{
    "type": "etf_holdings",
    "function": {
        "name": "etf_holdings",
        "description": "Retrieves ETF holdings for a given stock symbol.",
        "parameters": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "The stock symbol to query."},
            },
            "required": ["symbol"]
        }
    }
},
    # Add more functions here following the same structure
]