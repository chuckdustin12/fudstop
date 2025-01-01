import pandas as pd


class Orders:
    def __init__(self, order):
        # Determine if the order is a single dictionary or a list of dictionaries
        if isinstance(order, list):
            # If it's a list, initialize attributes as lists with values from each order dictionary
            self.id = [o.get('id') for o in order]
            self.type = [o.get('type') for o in order]
            self.symbol = [o.get('option_symbol') for o in order]
            self.side = [o.get('side') for o in order]
            self.quantity = [o.get('quantity') for o in order]
            self.status = [o.get('status') for o in order]
            self.duration = [o.get('duration') for o in order]
            self.price = [o.get('price') for o in order]
            self.avg_fill_price = [o.get('avg_fill_price') for o in order]
            self.exec_quantity = [o.get('exec_quantity') for o in order]
            self.last_fill_price = [o.get('last_fill_price') for o in order]
            self.last_fill_quantity = [o.get('last_fill_quantity') for o in order]
            self.remaining_quantity = [o.get('remaining_quantity') for o in order]
            self.create_date = [o.get('create_date') for o in order]
            self.transaction_date = [o.get('transaction_date') for o in order]
            self._class = [o.get('class') for o in order]

        else:
            # If it's a dictionary, initialize attributes directly as scalars
            self.id = order.get('id')
            self.type = order.get('type')
            self.symbol = order.get('option_symbol')
            self.side = order.get('side')
            self.quantity = order.get('quantity')
            self.status = order.get('status')
            self.duration = order.get('duration')
            self.price = order.get('price')
            self.avg_fill_price = order.get('avg_fill_price')
            self.exec_quantity = order.get('exec_quantity')
            self.last_fill_price = order.get('last_fill_price')
            self.last_fill_quantity = order.get('last_fill_quantity')
            self.remaining_quantity = order.get('remaining_quantity')
            self.create_date = order.get('create_date')
            self.transaction_date = order.get('transaction_date')
            self._class = order.get('class')


        # Initialize dictionaries for DataFrame conversions, using the existing attributes
        self.dict = {
            'id': self.id,
            'type': self.type,
            'ticker': self.symbol,
            'side': self.side,
            'quantity': self.quantity,
            'status': self.status,
            'duration': self.duration,
            'price': self.price,
            'avg_fill_price': self.avg_fill_price,
            'last_fill_price': self.last_fill_price,
            'last_fill_quantity': self.last_fill_quantity,
            'remaining_quantity': self.remaining_quantity,
            'create_date': self.create_date,
            'transaction_date': self.transaction_date,
            'class': self._class,

        }

        self.discord_dict = {
            'ticker': self.symbol,
            'type': self.type,
            'side': self.side,
            'quantity': self.quantity,
            'price': self.price,
            'timestamp': self.transaction_date
        }

        # Convert to DataFrames based on whether data is scalar or list
        if isinstance(self.id, list):
            # If attributes are lists, create multi-row DataFrames
            self.as_dataframe = pd.DataFrame(self.dict)
            self.as_discord_dataframe = pd.DataFrame(self.discord_dict)
        else:
            # If attributes are scalars, create single-row DataFrames
            self.as_dataframe = pd.DataFrame(self.dict, index=[0])
            self.as_discord_dataframe = pd.DataFrame(self.discord_dict, index=[0])
class Balances:
    def __init__(self, balances):
        self.option_short_value = balances.get('option_short_value')
        self.total_equity = balances.get('total_equity')
        self.account_number = balances.get('account_number')
        self.account_type = balances.get('account_type')
        self.close_pl = balances.get('close_pl')
        self.current_requirement = balances.get('current_requirement')
        self.equity = balances.get('equity')
        self.long_market_value = balances.get('long_market_value')
        self.market_value = balances.get('market_value')
        self.open_pl = balances.get('open_pl')
        self.option_long_value = balances.get('option_long_value')
        self.option_requirement = balances.get('option_requirement')
        self.pending_orders_count = balances.get('pending_orders_count')
        self.short_market_value = balances.get('short_market_value')
        self.stock_long_value = balances.get('stock_long_value')
        self.total_cash = balances.get('total_cash')
        self.uncleared_funds = balances.get('uncleared_funds')
        self.pending_cash = balances.get('pending_cash')
        margin = balances.get('margin')
        self.option_bp = margin.get('option_buying_power')
        self.dict = {
            'option_short_value': self.option_short_value,
            'total_equity': self.total_equity,
            'account_number': self.account_number,
            'account_type': self.account_type,
            'close_pl': self.close_pl,
            'current_requirement': self.current_requirement,
            'equity': self.equity,
            'long_market_value': self.long_market_value,
            'market_value': self.market_value,
            'open_pl': self.open_pl,
            'option_long_value': self.option_long_value,
            'option_requirement': self.option_requirement,
            'pending_orders_count': self.pending_orders_count,
            'short_market_value': self.short_market_value,
            'stock_long_value': self.stock_long_value,
            'total_cash': self.total_cash,
            'uncleared_funds': self.uncleared_funds,
            'pending_cash': self.pending_cash,
            'option_bp': self.option_bp
        }
        
        self.as_dataframe = pd.DataFrame(self.dict, index=[0])




class Positions:
    def __init__(self, positions):
        if isinstance(positions, list):
            self.cost_basis = [i.get("cost_basis") for i in positions]
            self.date = [i.get("date_acquired") for i in positions]
            self.id = [i.get("id") for i in positions]
            self.quantity = [i.get("quantity") for i in positions]
            self.symbol = [i.get("symbol") for i in positions]

            self.data_dict = { 
                'cost_basis': self.cost_basis,
                'date': self.date,
                'id': self.id,
                'quantity': self.quantity,
                'symbol': self.symbol
            }
        else:

            self.cost_basis = positions.get('cost_basis')
            self.date = positions.get('date_acquired')
            self.id = positions.get('id')
            self.quantity = positions.get('quantity')
            self.symbol = positions.get('symbol')


        # Convert to DataFrames based on whether data is scalar or list
        if isinstance(self.id, list):
            # If attributes are lists, create multi-row DataFrames
            self.as_dataframe = pd.DataFrame(self.data_dict)

        else:
            # If attributes are scalars, create single-row DataFrames
            self.as_dataframe = pd.DataFrame(self.data_dictkk, index=[0])


class GainLoss:
    def __init__(self, gain_loss):


        self.close_date = [i.get("close_date") for i in gain_loss]
        self.cost = [i.get("cost") for i in gain_loss]
        self.gainloss = [i.get("gain_loss") for i in gain_loss]
        self.change_percent=[i.get("gain_loss_percent") for i in gain_loss]
        self.open_date=[i.get("open_date") for i in gain_loss]
        self.proceeds=[i.get("proceeds") for i in gain_loss]
        self.quantity=[i.get("quantity") for i in gain_loss]
        self.symbol=[i.get("symbol") for i in gain_loss]
        self.term=[i.get("term") for i in gain_loss]


        self.data_dict = { 
            'close_date': self.close_date,
            'cost': self.cost,
            'gainloss': self.gainloss,
            'change_percent': self.change_percent,
            'open_date': self.open_date,
            'proceeds': self.proceeds,
            'quantity': self.quantity,
            'symbol': self.symbol,
            'term': self.term
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)