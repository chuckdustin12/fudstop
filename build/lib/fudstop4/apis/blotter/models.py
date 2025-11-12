import pandas as pd


class OptionsData:
    def __init__(self, data):

        self.stock_price = data.get('stock_price')
        self.calls = data.get('calls')
        self.puts = data.get('puts')
        self.all_expirations = data.get('all_expirations')

        self


    def filter_by_expirations(self, options_type):
        """
        Filter options (calls/puts) by matching expiration dates with 'all_expirations'.
        
        Args:
            options_type (str): Either 'calls' or 'puts'.

        Returns:
            pd.DataFrame: Flattened DataFrame with options filtered by valid expiration dates.
        """
        options = self.calls if options_type == "calls" else self.puts
        
        if not options:
            raise ValueError(f"No data available for '{options_type}'.")

        records = []

        # Iterate through options and match keys with all_expirations
        for date, contracts in options.items():
            if date in self.all_expirations:  # Check if the date is valid
                for contract in contracts:
                    record = {
                        "expiry": date,
                        "option_symbol": contract.get("contract"),
                        "break_even_price": contract.get("break_even_price"),
                        "change_percent": contract.get("change_percent"),
                        "iv": contract.get("implied_volatility"),
                        "oi": contract.get("open_interest"),
                        "price": contract.get("price"),
                        "stock": contract.get("stock"),
                        "strike": contract.get("strike"),
                        "total_volume": contract.get("total_volume"),
                        "type": contract.get("type"),
                    }
                    records.append(record)

        return pd.DataFrame(records)
    

    def get_filtered_calls(self):
        """
        Get call options filtered by valid expiration dates.
        Returns:
            pd.DataFrame: DataFrame containing filtered call options.
        """
        return self.filter_by_expirations("calls")

    def get_filtered_puts(self):
        """
        Get put options filtered by valid expiration dates.
        Returns:
            pd.DataFrame: DataFrame containing filtered put options.
        """
        return self.filter_by_expirations("puts")
    
    def get_combined_options(self):
        """
        Combine calls and puts into a single DataFrame with an option type identifier.
        
        Returns:
            pd.DataFrame: Combined DataFrame of calls and puts.
        """
        # Filter and add option type
        calls_df = self.filter_by_expirations("calls")
        calls_df["option_type"] = "Call"

        puts_df = self.filter_by_expirations("puts")
        puts_df["option_type"] = "Put"

        # Combine calls and puts into one DataFrame
        combined_df = pd.concat([calls_df, puts_df], ignore_index=True)

        return combined_df
    


class Profeed:
    def __init__(self, data):

        self.accuracy = [i.get('accuracy') for i in data]
        self.avatar = [i.get('avatar') for i in data]
        self.avg_holding_period = [i.get('avg_holding_period') for i in data]
        self.avggain = [i.get('avggain') for i in data]
        self.change = [i.get('change') for i in data]
        changes = [i.get('changes') for i in data]
        changes = [item for sublist in changes for item in sublist]
        self.position_trim = [i.get('trim') for i in changes]
        self.position_price = [i.get('price') for i in changes]
        self.change_time = [i.get('time') for i in changes]
        self.buy_sell = [i.get('buy_sell') for i in changes]
        self.comments = [i.get('comments') for i in data]
        self.completed = [i.get('completed') for i in data]
        self.current_investment = [i.get('current_investment') for i in data]
        self.downvotes = [i.get('downvotes') for i in data]
        self.entry = [i.get('entry') for i in data]
        self.exit_time = [i.get('exit_time') for i in data]
        self.exit_time_raw = [i.get('exit_time_raw') for i in data]
        self.id = [i.get('id') for i in data]
        self.instrument = [i.get('instrument') for i in data]
        self.is_followed = [i.get('is_followed') for i in data]
        self.level = [i.get('level') for i in data]
        self.logo = [i.get('logo') for i in data]
        self.market_alpha = [i.get('market_alpha') for i in data]
        metadata = [i.get('metadata') for i in data]
        self.contract = [i.get('contract') for i in metadata]
        self.cp = [i.get('cp') for i in metadata]
        self.expiration = [i.get('expiration') for i in metadata]
        self.strike = [i.get('strike') for i in metadata]

        self.name = [i.get('name') for i in data]
        self.num_comments = [i.get('num_comments') for i in data]
        self.owned = [i.get('owned') for i in data]
        self.price_now = [i.get('price_now') for i in data]
        self.price_unlock = [i.get('price_unlock') for i in data]
        self.profit = [i.get('profit') for i in data]
        self.quantity = [i.get('quantity') for i in data]
        self.sentiment = [i.get('sentiment') for i in data]
        self.single_trade_at_top = [i.get('single_trade_at_top') for i in data]
        self.submission_time = [i.get('submission_time') for i in data]
        self.submission_time_est = [i.get('submission_time_est') for i in data]
        self.symbol = [i.get('symbol') for i in data]
        self.symbol_name = [i.get('symbol_name') for i in data]
        self.time_horizon = [i.get('time_horizon') for i in data]
        self.time_since_trade = [i.get('time_since_trade') for i in data]
        self.total_profit = [i.get('total_profit') for i in data]
        self.trade_tier = [i.get('trade_tier') for i in data]
        self.trades = [i.get('trades') for i in data]
        self.type = [i.get('type') for i in data]
        self.unique = [i.get('unique') for i in data]
        self.upvotes = [i.get('upvotes') for i in data]
        self.userid = [i.get('userid') for i in data]
        self.username = [i.get('username') for i in data]


        self.data_dict = {
            "buy_sell": self.buy_sell,
            "contract": self.contract * len(data),
            'cp': self.cp * len(data),
            'expiration': self.expiration * len(data),
            'strike': self.strike * len(data),
            "accuracy": [i.get('accuracy') for i in data],
            "avatar": [i.get('avatar') for i in data],
            "avg_holding_period": [i.get('avg_holding_period') for i in data],
            "avggain": [i.get('avggain') for i in data],
            "change": [i.get('change') for i in data],
            "position_trim": self.position_trim * len(data),
            "position_price": self.position_price * len(data),
            "changed_time": self.change_time * len(data),
            "comments": [i.get('comments') for i in data],
            "completed": [i.get('completed') for i in data],
            "current_investment": [i.get('current_investment') for i in data],
            "downvotes": [i.get('downvotes') for i in data],
            "entry": [i.get('entry') for i in data],
            "exit_time": [i.get('exit_time') for i in data],
            "exit_time_raw": [i.get('exit_time_raw') for i in data],
            "id": [i.get('id') for i in data],
            "instrument": [i.get('instrument') for i in data],
            "is_followed": [i.get('is_followed') for i in data],
            "level": [i.get('level') for i in data],
            "logo": [i.get('logo') for i in data],
            "market_alpha": [i.get('market_alpha') for i in data],
            "metadata": [i.get('metadata') for i in data],
            "name": [i.get('name') for i in data],
            "num_comments": [i.get('num_comments') for i in data],
            "owned": [i.get('owned') for i in data],
            "price_now": [i.get('price_now') for i in data],
            "price_unlock": [i.get('price_unlock') for i in data],
            "profit": [i.get('profit') for i in data],
            "quantity": [i.get('quantity') for i in data],
            "sentiment": [i.get('sentiment') for i in data],
            "single_trade_at_top": [i.get('single_trade_at_top') for i in data],
            "submission_time": [i.get('submission_time') for i in data],
            "submission_time_est": [i.get('submission_time_est') for i in data],
            "symbol": [i.get('symbol') for i in data],
            "symbol_name": [i.get('symbol_name') for i in data],
            "time_horizon": [i.get('time_horizon') for i in data],
            "time_since_trade": [i.get('time_since_trade') for i in data],
            "total_profit": [i.get('total_profit') for i in data],
            "trade_tier": [i.get('trade_tier') for i in data],
            "trades": [i.get('trades') for i in data],
            "type": [i.get('type') for i in data],
            "unique": [i.get('unique') for i in data],
            "upvotes": [i.get('upvotes') for i in data],
            "userid": [i.get('userid') for i in data],
            "username": [i.get('username') for i in data]
        }

        # Ensure all lists are the same length
        self._ensure_equal_lengths()
        self.as_dataframe = pd.DataFrame(self.data_dict)
    def _ensure_equal_lengths(self):
        """
        Ensures all lists in self.data_dict are of equal length.
        """
        max_length = max(len(v) for v in self.data_dict.values())
        for key, value in self.data_dict.items():
            if len(value) < max_length:
                self.data_dict[key].extend([None] * (max_length - len(value)))
            elif len(value) > max_length:
                self.data_dict[key] = value[:max_length]
