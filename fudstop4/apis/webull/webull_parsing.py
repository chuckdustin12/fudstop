import os
from dotenv import load_dotenv
load_dotenv()


class WebullParser:
    async def async_parse_most_active(ticker_entry):

        all_parsed_data = []
        # Parsing 'ticker' attributes
        datas = ticker_entry.get('data', {})
        for data in datas:
            parsed_data = {}
            ticker_info = data.get('ticker', {})
            parsed_data['tickerId'] = ticker_info.get('tickerId')
            parsed_data['exchangeId'] = ticker_info.get('exchangeId')
            parsed_data['regionId'] = ticker_info.get('regionId')
            parsed_data['currencyId'] = ticker_info.get('currencyId')
            parsed_data['currencyCode'] = ticker_info.get('currencyCode')
            parsed_data['name'] = ticker_info.get('name')
            parsed_data['symbol'] = ticker_info.get('symbol')
            parsed_data['disSymbol'] = ticker_info.get('disSymbol')
            parsed_data['disExchangeCode'] = ticker_info.get('disExchangeCode')
            parsed_data['status'] = ticker_info.get('status')
            parsed_data['close'] = ticker_info.get('close')
            parsed_data['change'] = ticker_info.get('change')
            parsed_data['changeRatio'] = ticker_info.get('changeRatio')
            parsed_data['marketValue'] = ticker_info.get('marketValue')
            parsed_data['volume'] = ticker_info.get('volume')
            parsed_data['turnoverRate'] = ticker_info.get('turnoverRate')
            parsed_data['regionName'] = ticker_info.get('regionName')
            parsed_data['peTtm'] = ticker_info.get('peTtm')
            parsed_data['timeZone'] = ticker_info.get('timeZone')
            parsed_data['preClose'] = ticker_info.get('preClose')
            parsed_data['fiftyTwoWkHigh'] = ticker_info.get('fiftyTwoWkHigh')
            parsed_data['fiftyTwoWkLow'] = ticker_info.get('fiftyTwoWkLow')
            parsed_data['open'] = ticker_info.get('open')
            parsed_data['high'] = ticker_info.get('high')
            parsed_data['low'] = ticker_info.get('low')
            parsed_data['vibrateRatio'] = ticker_info.get('vibrateRatio')
            

            all_parsed_data.append(parsed_data)
        return all_parsed_data



    # Creating a function to parse each attribute of the data_entry and return it as a dictionary
    async def async_parse_total_top_options(self, data_entry):
        all_parsed_data = []
        
        for data in data_entry:
            parsed_data = {}
            ticker_info = data.get('ticker', {})
            for key, value in ticker_info.items():
                if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                    parsed_data[f'{key}'] = value
        
            # Parsing 'values' attributes
            values_info = data.get('values', {})
            for key, value in values_info.items():

                if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                    parsed_data[f'{key}'] = value
            
            all_parsed_data.append(parsed_data)
        if 't_sectype' in all_parsed_data:
            all_parsed_data.remove('t_sectype')

        return all_parsed_data



    async def async_parse_contract_top_options(self, data_entry):
        all_parsed_data = []
        for data in data_entry:
            parsed_data = {}
            # Parsing 'belongTicker' attributes
            belong_ticker_info = data.get('belongTicker', {})
            
            for key, value in belong_ticker_info.items():
                if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                    parsed_data[f'{key}'] = value

        
            
            # Parsing 'derivative' attributes
            derivative_info = data.get('derivative', {})
            for key, value in derivative_info.items():
                if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                    parsed_data[f'{key}'] = value
            
            # Parsing 'values' attributes
            values_info = data.get('values', {})
            for key, value in values_info.items():
                if type(key) != list and key != 'exchangeTrade' and key != 'derivativeSupport':
                    parsed_data[f'{key}'] = value 

            all_parsed_data.append(parsed_data)
        if 'bt_secType' in all_parsed_data:
            all_parsed_data = all_parsed_data.remove('bt_secType')
        return all_parsed_data



    # Creating a function to parse each attribute of the data_entry and return it as a dictionary
    async def async_parse_ticker_values(self, data_entry):
        all_parsed_data = []
        data_entry = data_entry.get('data', {})
        for data in data_entry:
            parsed_data = {}
            ticker_info = data.get('ticker', {})
            for key, value in ticker_info.items():
                if type(key) != list and key != 'secType' and key != 'derivativeSupport':
                    parsed_data[f'{key}'] = value
        
            # Parsing 'values' attributes
            values_info = data.get('values', {})
            for key, value in values_info.items():
                if type(key) != list and key != 'secType' and key != 'derivativeSupport':
                    parsed_data[f'{key}'] = value
            
            all_parsed_data.append(parsed_data)
        return all_parsed_data


    async def async_parse_forex(ticker_list):
        parsed_data_list = []
        
        for ticker_entry in ticker_list:
            parsed_data = {}
            
            parsed_data['tickerId'] = ticker_entry.get('tickerId')
            parsed_data['exchangeId'] = ticker_entry.get('exchangeId')
            
            parsed_data['name'] = ticker_entry.get('name')
            parsed_data['symbol'] = ticker_entry.get('symbol')
            parsed_data['disSymbol'] = ticker_entry.get('disSymbol')
            parsed_data['status'] = ticker_entry.get('status')
            parsed_data['close'] = ticker_entry.get('close')
            parsed_data['change'] = ticker_entry.get('change')
            parsed_data['changeRatio'] = ticker_entry.get('changeRatio')
            parsed_data['marketValue'] = ticker_entry.get('marketValue')
            
            parsed_data_list.append(parsed_data)
        
        return parsed_data_list


    async def async_parse_etfs(response):
        flattened_data = []
        
        for tab in response.get('tabs', []):
            tab_info = {
                'id': tab.get('id'),
                'name': tab.get('name'),
                'comment': tab.get('comment'),
                'queryId': tab.get('queryId'),
                'upNum': tab.get('upNum'),
                'dowoNum': tab.get('dowoNum'),
                'flatNum': tab.get('flatNum'),
            }
            
            for ticker in tab.get('tickerTupleList', []):
                # Merge the 'tab' info and the 'ticker' info into a single dictionary
                merged_info = {**tab_info, **ticker}
                flattened_data.append(merged_info)

        return flattened_data

    # Define a function to parse the given data object with specific attributes under the parent key "item"
    async def async_parse_ipo_data(data):
        """
        Parses an IPO data object and returns a dictionary with relevant fields.

        Args:
        - item (dict): The IPO data item to parse.

        Returns:
        - dict: A dictionary containing parsed IPO data.
        """
        items = data['items']
        all_parsed_data=[]
        for item in items:
            parsed_data = {
                'ticker_id': item.get('tickerId', None),
                'list_date': item.get('listDate', None),
                'issue_up_limit': item.get('issueUpLimit', None),
                'issue_price': item.get('issuePrice', None),
                'currency_id': item.get('currencyId', None),
                'exchange_code': item.get('disExchangeCode', None),
                'symbol': item.get('disSymbol', None),
                'ipo_status': item.get('ipoStatus', None),
                'issue_currency_id': item.get('issueCurrencyId', None),
                'issue_down_limit': item.get('issueDownLimit', None),
                'issue_price_str': item.get('issuePriceStr', None),
                'name': item.get('name', None),
                'offering_type': item.get('offeringType', None),
                'prospectus': item.get('prospectus', None),
                'prospectus_publish_date': item.get('prospectusPublishDate', None),
                'purchase_end_date': item.get('purchaseEndDate', None),
                'purchase_start_date': item.get('purchaseStartDate', None),
                'close_days': item.get('closeDays', 0)  # Assuming 0 if not present
            }
            all_parsed_data.append(parsed_data)
        return all_parsed_data

