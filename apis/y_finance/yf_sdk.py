


import yfinance as yf
from _markets.list_sets.ticker_lists import all_tickers
from datetime import datetime
import pandas as pd
from ..helpers import format_large_numbers_in_dataframe
import numpy as np
# Set options to display all rows and columns
pd.set_option('display.max_rows', None)  # None means show all rows


class yfSDK:
    def __init__(self):
        self.tickers = all_tickers
  
    def batch_insert_dataframe(self, df, table_name, unique_columns, batch_size=250):
        with self.lock:
            if not self.table_exists(table_name):
                self.create_table(df, table_name, unique_columns)

            df = df.copy()
            df.dropna(inplace=True)
            df['insertion_timestamp'] = [datetime.now() for _ in range(len(df))]

            records = df.to_records(index=False)
            data = list(records)

            with self.pool.acquire() as connection:
                column_types = connection.fetch(
                    f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
                )
                type_mapping = {col: next((item['data_type'] for item in column_types if item['column_name'] == col), None) for col in df.columns}

                with connection.transaction():
                    insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(f'"{col}"' for col in df.columns)}) 
                    VALUES ({', '.join('%s' for _ in df.columns)})
                    ON CONFLICT ({unique_columns})
                    DO UPDATE SET {', '.join(f'"{col}" = excluded."{col}"' for col in df.columns)}
                    """

                    batch_data = []
                    for record in data:
                        new_record = []
                        for col, val in zip(df.columns, record):
                            pg_type = type_mapping[col]

                            if val is None:
                                new_record.append(None)
                            elif pg_type == 'timestamp' and isinstance(val, np.datetime64):
                                new_record.append(pd.Timestamp(val).to_pydatetime().replace(tzinfo=None))
                            elif isinstance(val, datetime):
                                new_record.append(pd.Timestamp(val).to_pydatetime())
                            elif pg_type in ['double precision', 'real'] and not isinstance(val, str):
                                new_record.append(float(val))
                            elif isinstance(val, np.int64):
                                new_record.append(int(val))
                            elif pg_type == 'integer' and not isinstance(val, int):
                                new_record.append(int(val))
                            else:
                                new_record.append(val)

                        batch_data.append(new_record)

                        if len(batch_data) == batch_size:
                            try:
                                connection.executemany(insert_query, batch_data)
                                batch_data.clear()
                            except Exception as e:
                                print(f"An error occurred while inserting the record: {e}")
                                connection.execute('ROLLBACK')
                                raise

    def save_to_history(self, df, main_table_name, history_table_name):
        # Assume the DataFrame `df` contains the records to be archived
        if not self.table_exists(history_table_name):
            self.create_table(df, history_table_name, None)

        df['archived_at'] = datetime.now()  # Add an 'archived_at' timestamp
        self.batch_insert_dataframe(df, history_table_name, None)

    def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');"

        with self.pool.acquire() as conn:
            with conn.transaction():
                exists = conn.fetchval(query)
        return exists
    def balance_sheet(self, ticker:str, frequency:str='quarterly', pretty:bool=None, as_dict:bool=False):
        """
        Gets balance sheet information for a ticker.

        Arguments:

        >>> Frequency: The frequency. quarterly / annual (default quarterly)

        >>> As Dict: bool - return as a dictionary (optional - default FALSE)

        >>> Pretty: (optional - pretty prent)

        """

        

        data = yf.Ticker(ticker)
        if pretty == None:
            pretty = False

        balance_sheet = data.get_balance_sheet(freq=frequency,pretty=pretty, as_dict=as_dict)
        formatted_data = format_large_numbers_in_dataframe(balance_sheet)
        return formatted_data 

    

    def get_cash_flow(self, ticker:str, frequency:str='quarterly', pretty:bool=False, as_dict:bool=False):
        """
        Gets cash flow information for a ticker.

        Arguments:

        >>> Frequency: The frequency. quarterly / annual (default quarterly)

        >>> As Dict: bool - return as a dictionary (optional - default FALSE)

        >>> Pretty: (optional - pretty prent)
        """
        data = yf.Ticker(ticker).get_cash_flow(freq=frequency,pretty=pretty, as_dict=as_dict)


        formatted_data = format_large_numbers_in_dataframe(data)
        return formatted_data





    def get_all_candles(self, tickers:str):
        """
        Gets OHLC, adj.Close and Volume data for ALL DATES

        Arguments:


        >>> Tickers: a list of comma separated tickers. (default ALL TICKERS)

        >>> Period: the period to gather data for. OPTIONS = 
        
            >>> 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo 
            
            
            Intraday data cannot extend last 60 days
        
        """


        try:
            chart_data = yf.download(tickers)
        except Exception as e:
            print(f'Error processing data. - {e}')



            chart_data = pd.DataFrame(chart_data).reset_index(None)


            return chart_data
        
    

    def dividends(self, ticker:str):
        """
        Returns historic dividends for a ticker
        
        """
        try:
            data = yf.Ticker(ticker).get_dividends()

            return data
        except Exception as e:
            return(f"No dividends found for {ticker}. {e}")
        

    def fast_info(self, ticker:str):
        """
        Arguments:

        >>> Limit: the number of results to return (optional - default 15)
        """
        data = yf.Ticker(ticker=ticker).get_fast_info().items()


        


        df = pd.DataFrame(data)
        df.reset_index(drop=True, inplace=True)
        formatted_data = format_large_numbers_in_dataframe(df)
        return formatted_data 



    def financials(self, ticker:str, frequency:str='quarterly', as_dict:bool=False, pretty:bool=False):

        """
        Gets all financials for a ticker.


        Arguments:

        >>> Frequency: The frequency. quarterly / annual (default quarterly)

        >>> As Dict: bool - return as a dictionary (optional - default FALSE)

        >>> Pretty: (optional - pretty prent)
        """
        data = yf.Ticker(ticker=ticker).get_financials(freq=frequency,as_dict=as_dict, pretty=pretty)

        formatted_data = format_large_numbers_in_dataframe(data)
        return formatted_data 
    

    def income_statement(self, ticker:str, frequency:str='quarterly', as_dict:bool=False, pretty:bool=False):
        """
        Gets the income statement for a ticker.

        Arguments:

        >>> Frequency: The frequency. quarterly / annual (default quarterly)

        >>> As Dict: bool - return as a dictionary (optional - default FALSE)

        >>> Pretty: (optional - pretty prent)

        """
        data = yf.Ticker(ticker=ticker).get_income_stmt(freq=frequency,as_dict=as_dict,pretty=pretty)


        formatted_data = format_large_numbers_in_dataframe(data)
        return formatted_data     
    

    def get_info(self, ticker:str):
        """
        Returns a large dictionary of information for a ticker.

        Arguments:

        None
        
        """
        data  = yf.Ticker(ticker).get_info().items()

        df = pd.DataFrame(data)
        formatted_data = format_large_numbers_in_dataframe(df)
        return formatted_data     

    


    def institutional_holdings(self, ticker:str):
        """
        Gets institutional holdoings.

        Arguments:


        
        """

        data =yf.Ticker(ticker).get_institutional_holders()

 

        formatted_data = format_large_numbers_in_dataframe(data)

        # Convert the '% Out' column to float (if it's not already)
        formatted_data['% Out'] = formatted_data['% Out'].astype(float)

        # Round the '% Out' column to 3 decimal places
        formatted_data['% Out'] = formatted_data['% Out'].round(3)
        formatted_data.set_index('Date Reported', inplace=True)

        return formatted_data


    def mutual_fund_holders(self, ticker:str):
        """
        Gets mutual fund holders


        Arguments:

        >>> 
        
        """

        data = yf.Ticker(ticker=ticker).get_mutualfund_holders()

        formatted_data = format_large_numbers_in_dataframe(data)
        # Convert the '% Out' column to float (if it's not already)
        formatted_data['% Out'] = formatted_data['% Out'].astype(float)

        # Round the '% Out' column to 3 decimal places
        formatted_data['% Out'] = formatted_data['% Out'].round(3)
        formatted_data.set_index('Date Reported', inplace=True)

        return formatted_data     
    

    def news(self, ticker:str):
        """
        Gets ticker news.

        Arguments:

        None
        """

        data = yf.Ticker(ticker=ticker).get_news()

        df=  pd.DataFrame(data)

      # Drop the 'uuid' column
        df = df.drop('uuid', axis=1)

        return df
    

    def atm_calls(self, ticker:str):
        """
        Gets at the money calls for a ticker.
 
        
        """

        calls = yf.Ticker(ticker)._download_options()

        call_options = calls['calls']


        df = pd.DataFrame(call_options)


        



        return df

    def atm_puts(self, ticker:str):
        """
        Gets At The Money puts for a ticker.
        
        """

        puts = yf.Ticker(ticker)._download_options()

        put_options = puts['puts']


        df = pd.DataFrame(put_options)


        



        return df

