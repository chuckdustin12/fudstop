import sys
from pathlib import Path

# Add the project fetch to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

import pandas as pd
from .webull_helpers import flatten_list_of_dicts, flatten_dict


class WebullOptionsData:
    def __init__(self, data):
        self.as_dataframe = None
      # Initialize the remaining attributes
        self.expireDate = [i.get('expireDate') for i in data] if isinstance(data, list) else data.get('expireDate')
        self.tickerId = [i.get('tickerId') for i in data] if isinstance(data, list) else data.get('tickerId')
        self.belongTickerId = [i.get('belongTickerId') for i in data] if isinstance(data, list) else data.get('belongTickerId')
        self.openIntChange = [i.get('openIntChange') for i in data] if isinstance(data, list) else data.get('openIntChange')
        self.activeLevel = [i.get('activeLevel') for i in data] if isinstance(data, list) else data.get('activeLevel')
        self.direction = [i.get('direction') for i in data] if isinstance(data, list) else data.get('direction')
        self.symbol = [i.get('symbol') for i in data] if isinstance(data, list) else data.get('symbol')
        self.unSymbol = [i.get('unSymbol') for i in data] if isinstance(data, list) else data.get('unSymbol')
        self.askList = [i.get('askList', None) for i in data] if isinstance(data, list) else data.get('askList', None)
        self.bidList = [i.get('bidList', None) for i in data] if isinstance(data, list) else data.get('bidList', None)
        self.strikePrice = [float(i.get('strikePrice', None)) for i in data] if isinstance(data, list) else float(data.get('strikePrice', None))
        self.open = [i.get('open') for i in data]
        self.high = [i.get('high') for i in data]
        self.low = [i.get('low') for i in data]
        self.close =  [i.get('close') for i in data]
        self.preClose = [float(item.get('preClose')) if isinstance(item.get('preClose'), (int, float)) else None for item in data] if isinstance(data, list) else float(data.get('preClose')) if isinstance(data.get('preClose'), (int, float)) else None

        self.change = [float(item.get('change')) if isinstance(item.get('change'), (int, float)) else None for item in data] if isinstance(data, list) else float(data.get('change')) if isinstance(data.get('change'), (int, float)) else None

        self.changeRatio = [round(float(item.get('changeRatio')) * 100, 2) if isinstance(item.get('changeRatio'), (int, float)) else None for item in data] if isinstance(data, list) else round(float(data.get('changeRatio')) * 100, 2) if isinstance(data.get('changeRatio'), (int, float)) else None

        self.volume = [i.get('volume') for i in data]


        self.latestPriceVol = [float(item.get('latestPriceVol')) if isinstance(item.get('latestPriceVol'), (int, float)) else None for item in data] if isinstance(data, list) else float(data.get('latestPriceVol')) if isinstance(data.get('latestPriceVol'), (int, float)) else None

        self.openInterest = [float(item.get('openInterest')) if isinstance(item.get('openInterest'), (int, float)) else None for item in data] if isinstance(data, list) else float(data.get('openInterest')) if isinstance(data.get('openInterest'), (int, float)) else None

        self.openIntChange = [float(item.get('openIntChange')) if isinstance(item.get('openIntChange'), (int, float)) else None for item in data] if isinstance(data, list) else float(data.get('openIntChange')) if isinstance(data.get('openIntChange'), (int, float)) else None

                # For the following, it seems they are expected to be strings, so we'll leave them as is, just checking for lists.
        self.impVol = [float(item.get('impVol')) if item.get('impVol', '--') != '--' else None for item in data] if isinstance(data, list) else (float(data.get('impVol')) if data.get('impVol', '--') != '--' else None)

        self.delta = [float(item.get('delta')) if item.get('delta', '--') != '--' else None for item in data] if isinstance(data, list) else (float(data.get('delta')) if data.get('delta', '--') != '--' else None)

        self.gamma = [float(item.get('gamma')) if item.get('gamma', '--') != '--' else None for item in data] if isinstance(data, list) else (float(data.get('gamma')) if data.get('gamma', '--') != '--' else None)

        self.theta = [float(item.get('theta')) if item.get('theta', '--') != '--' else None for item in data] if isinstance(data, list) else (float(data.get('theta')) if data.get('theta', '--') != '--' else None)

        self.rho = [float(item.get('rho')) if item.get('rho', '--') != '--' else None for item in data] if isinstance(data, list) else (float(data.get('rho')) if data.get('rho', '--') != '--' else None)

        self.vega = [float(item.get('vega')) if item.get('vega', '--') != '--' else None for item in data] if isinstance(data, list) else (float(data.get('vega')) if data.get('vega', '--') != '--' else None)


        self.askList = [i.get('askList', None) for i in data] if isinstance(data, list) else data.get('askList', None)
        self.bidList = [i.get('bidList', None) for i in data] if isinstance(data, list) else data.get('bidList', None)
        self.bidPrice = [i[0].get('price') for i in self.bidList if i and i[0] is not None]

  
        self.askPrice = [i[0].get('price') for i in self.askList if i and i[0] is not None]
        self.bidVolume = [i[0].get('volume') for i in self.bidList if i and i[0] is not None]
        self.askVolume = [i[0].get('volume') for i in self.askList if i and i[0] is not None]
        self.bidExchange = [i[0].get('quoteEx') for i in self.bidList if i and i[0] is not None]
        self.askExchange = [i[0].get('quoteEx') for i in self.askList if i and i[0] is not None]

     
        self.data_dict = { 
            'strike_price': self.strikePrice,
            'call_put': self.direction,
            'expiry_date': self.expireDate,
            'option_symbol': self.symbol,
            'option_id': self.tickerId,
            'ticker_id': self.belongTickerId,  # Assuming you meant belongTickerId here for the underlying ticker
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'pre_close': self.preClose,
            'change': self.change,
            'change_ratio': self.changeRatio,
            'volume': self.volume,
            'bid_price': self.bidPrice,
            'bid_volume': self.bidVolume,
            'bid_exchange': self.bidExchange,
            'ask_price': self.askPrice,
            'ask_volume': self.askVolume,
            'ask_exchange': self.askExchange,
            'latest_price_vol': self.latestPriceVol,
            'open_interest': self.openInterest,
            'open_interest_change': self.openIntChange,
            'implied_volatility': self.impVol,
            'delta': self.delta,
            'vega': self.vega,
            'gamma': self.gamma,
            'theta': self.theta,
            'rho': self.rho,

            # ... and any other attributes you wish to include
        }

        try:
            self.as_dataframe = pd.DataFrame(self.data_dict)

        except ValueError:
            print(f'Arrays not same length - skipping!')



class VolumeAnalysis:
    def __init__(self, data):
        self.ticker_id = data.get('belongTickerId', None)
        self.option_id = data.get('tickerId', None)
        self.total_trades = data.get('totalNum', None)
        self.total_volume = data.get('totalVolume', None)
        self.avg_price = data.get('avgPrice', None)
        self.buy_volume = data.get('buyVolume', None)
        self.sell_volume = data.get('sellVolume', None)
        self.neutral_volume = data.get('neutralVolume', None)

        
        trades = flatten_list_of_dicts(data.get('datas', None))
        dates = flatten_list_of_dicts(data.get('dates', None))


        if trades is not None and dates is not None:
            flattened_data = []
            for trade, date in zip(trades, dates):
                # Flatten the date dictionary
                date_value = date[''] if '' in date else None
                
                # Combine the trade data with the date
                self.flattened_trade = {
                    'date': date_value,
                    **trade  # Unpack the trade dictionary
                }
                
                # Add the flattened trade to the list
                flattened_data.append(self.flattened_trade)

                # Create a DataFrame
                df = pd.DataFrame(flattened_data)

                df['option_id'] = self.option_id
                df.set_index('option_id', inplace=True)

            print(flattened_data)
        self.trades_and_dates_dict = { 

            'trades': trades,
            'dates': dates
        }

        self.data_dict = { 

            'ticker_id': self.ticker_id,
            'option_id': self.option_id,
            'total_trades': self.total_trades,
            'total_volume': self.total_volume,
            'avg_price': self.avg_price,
            'buy_volume': self.buy_volume,
            'sell_volume': self.sell_volume,
            'neutral_volume': self.neutral_volume,

        }


        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])

        print(self.as_dataframe)