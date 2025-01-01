
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import requests

import re
from io import StringIO

import pandas as pd
from .models import AuctionResult, FXSwaps, TimeSeries, AsOfDates, TimeSeriesData, SecuredReferenceRates, RepoOperations, SecuritiesLending
session = requests.session()
from datetime import datetime, timedelta
today_str = datetime.now().strftime('%Y-%m-%d')
thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
class FedNewyork:
    def __init__(self):
        self.base_url = "https://markets.newyorkfed.org/api/"
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.today = datetime.now().strftime('%Y-%m-%d')
    def agency_mbs_search(self, start_date=thirty_days_ago, end_date=today_str):
        """Search for AMBS operations out of the Federal Reserve of New York"""

        r = session.get(self.base_url + f"ambs/all/results/summary/search.json?startDate={start_date}&endDate={today_str}").json()
        ambs = r['ambs']
        auctions = ambs['auctions']
        if auctions is not None:
            data = AuctionResult(auctions)
            return data
        else:
            return None
    

    def agency_mbs_count(self, number=10):
        """Return AMBS transactions by count."""
        r = session.get(self.base_url + f"ambs/all/results/details/last/{number}.json").json()
        ambs = r['ambs']
        auctions = ambs['auctions']
        if auctions is not None:
            data = AuctionResult(auctions)
            return data
        else:
            return None
        

    def soma_holdings(self):
        url = f"https://markets.newyorkfed.org/api/soma/summary.json"
        r = requests.get(url).json()
        soma = r['soma']
        summary = soma['summary'] if 'summary' in soma else None
        if summary is not None:
            return summary
    def liquidity_swaps_latest(self):
        """Get the latest central bank liquidity swap data."""
        r = session.get(self.base_url + f"fxs/all/latest.json").json()
        fxSwaps = r['fxSwaps']
        operations = fxSwaps['operations']
        if operations is not None:
            data = FXSwaps(operations)
            return data
        else:
            return "No recent data found."
        
    def liquidity_swaps_count(self, number=50):
        """Get the latest central bank liquidity swap data."""
        r = session.get(self.base_url + f"fxs/usdollar/last/{number}.json").json()
        fxSwaps = r['fxSwaps']
        operations = fxSwaps['operations']
        if operations is not None:
            data = FXSwaps(operations)
            return data
        else:
            return "No recent data found."
        

    def liquidity_swaps_search(self, start_date = "2023-01-01", end_date = today_str, type="trade", counterparties='japan,europe'):
        """Search for liquidity swaps between a custom date range.
        
        Arguments:
          >>> start_date: a date in YYYY-MM-DD format to start from
          >>> end_date: a date in YYYY-MM-DD to end on. defaults to today.
          >>> type: type of information to return. trade or maturity.
          >>> counterparties: list of counterparties. default: europe, japan
        """

        

        r = session.get(self.base_url + f"fxs/all/search.json?startDate={start_date}&endDate={end_date}&dateType={type}&counterparties={counterparties}").json()
        fxSwaps = r['fxSwaps']
        operations = fxSwaps['operations']
        if operations is not None:
            data = FXSwaps(operations)
            return data
        else:
            return None
        

    def get_fed_counterparties(self):
        """Returns the current counterparties to the Federal Reserve."""
        r = session.get(self.base_url + "fxs/list/counterparties.json").json()
        fxSwaps = r['fxSwaps']
        counterparties = fxSwaps['counterparties']
        if counterparties is not None:
            return counterparties


    def get_as_of_dates(self):
        """Returns a list of dates to query the FED API with."""
        r = session.get("https://markets.newyorkfed.org/api/pd/list/asof.json").json()
        pdd = r['pd']
        as_of_dates = pdd['asofdates']
        if as_of_dates is not None:
            data = AsOfDates(as_of_dates)
            return data
        else:
            return None
        
    def get_timeseries(self):
        """Returns the timeseries data to query the FED API"""

        r = requests.get("https://markets.newyorkfed.org/api/pd/list/timeseries.json").json()
        pdd = r['pd']
        timeseries = pdd['timeseries']
        if timeseries is not None:
            data = TimeSeries(timeseries)

            
            return data
        else:
            return None


    def get_timeseries_data(self, timeseries):
        """Use timeseries codes to query the FED API."""
        
        timeseries_data = requests.get(f"https://markets.newyorkfed.org/api/pd/get/{timeseries}.json").json()
        pdd = timeseries_data['pd']
        timeseries = pdd['timeseries']
        if timeseries is not None:
            data = TimeSeriesData(timeseries)
            return data
        else:
            return None
    
    def reference_rates(self, type):
        """Returns all unsecured central bank rates globally.
        
        Arguments:
        >>> rate_type: secured or unsecured
        """
        r = session.get(f"https://markets.newyorkfed.org/api/rates/{type}/all/latest.json").json()
        refrates = r['refRates']

        if refrates is not None:
            data = SecuredReferenceRates(refrates)
            return data

        

    def rates_search(self, start_date:str=None, end_date:str=None):
        """Search reference rates between a given time range."""
        if start_date == None:
            start_date = self.thirty_days_ago
        if end_date == None:
            end_date = self.today
        r = session.get(self.base_url + f"rates/all/search.json?startDate={start_date}&endDate={end_date}").json()
        refrates = r['refRates']
        if refrates is not None:
            data = SecuredReferenceRates(refrates)
            return data
   
    def repo_operations_search(self, start_date=None, end_date=today_str):
        """Search by date for repo operations out of the FED."""
        if start_date == None:
            start_date = self.thirty_days_ago
        r = session.get(f"https://markets.newyorkfed.org/api/rp/results/search.json?startDate={start_date}&endDate={end_date}&securityType=mb").json()
        repo = r['repo']
        operations = repo['operations']
        if operations is not None:
            data = RepoOperations(operations)
            return data

        

    def repo_latest(self):
        """Get the latest repo operations from the FED's discount window."""

        r = session.get("https://markets.newyorkfed.org/api/rp/all/all/results/latest.json").json()

        repo = r['repo']
        operations = repo['operations']
        if operations is not None:
            data = RepoOperations(operations)

            return data
  

    def repo_propositions(self):
        """Check all repo & reverse repo operations out of the FED."""
        propositions = session.get("https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json").json()

        repo = propositions['repo']
        operations = repo['operations']
        data = []
        for operation in operations:
            operation_id = operation['operationId']
            operation_date = operation['operationDate']
            operation_type = operation['operationType']
            note = operation['note']
            total_amt_accepted = operation['totalAmtAccepted']
            
            data.append({
                'Operation ID': operation_id,
                'Operation Date': operation_date,
                'Operation Type': operation_type,
                'Note': note,
                'Total Amount Accepted': total_amt_accepted
            })

        df = pd.DataFrame(data)
        return df


    def securities_lending_search(self, start_date=None, end_date=today_str):
        """Search securities lending operations out of the FED."""
        if start_date == None:
            start_date = self.thirty_days_ago
        sec_lending = session.get(f"https://markets.newyorkfed.org/api/seclending/all/results/summary/search.json?startDate={start_date}&endDate={end_date}").json()

        seclending = sec_lending.get('seclending')
        operations = seclending['operations']
        if operations is not None:
            data = SecuritiesLending(operations)
            return data





    def all_agency_mortgage_backed_securities(self):
        """Returns Agency Mortgage Backed Securities from the New York Fed API
        
        PARAMS:

        >>> operation:

            'all'
            'purchases'
            'sales'
            'roll'
            'swap'
        
        >>> status:


            'announcements'
            'results'
        
            
        >>> include:

            'summary'
            'details'

        >>> format:

            'json'
            'csv'
            'xml'
            'xlsx'
        
        """
        url = f"https://markets.newyorkfed.org/beta/api/ambs/all/results/details/search.json?startDate={self.thirty_days_ago}&endDate={today_str}"
        print(url)
        r = requests.get(url).json()
        ambs = r['ambs'] if 'ambs' in r else None

        all_data_dicts = []
        
        if ambs is None:
            return all_data_dicts
        
        auctions = ambs.get('auctions', [])
        
        for i in auctions:
            details = i.get('details', [])
            
            for detail in details:
                data_dict = {
                    'auction_status': i.get("auctionStatus"),
                    'operation_id': i.get('operationId'),
                    'operation_date': i.get('operationDate'),
                    'operation_type': i.get('operationType'),
                    'operation_direction': i.get('operationDirection'),
                    'method': i.get('method'),
                    'release_time': i.get('releaseTime'),
                    'close_time': i.get('closeTime'),
                    'class_type': i.get('classType'),
                    'total_submitted_orig_face': i.get('totalSubmittedOrigFace'),
                    'total_accepted_orig_face': i.get('totalAcceptedOrigFace'),
                    'total_submitted_curr_face': i.get('totalSubmittedCurrFace'),
                    'total_accepted_curr_face': i.get('totalAcceptedCurrFace'),
                    'total_submitted_par': i.get('totalAmtSubmittedPar'),
                    'total_accepted_par': i.get('totalAmtAcceptedPar'),
                    'settlement_date': i.get('settlementDate'),
                    'last_updated': i.get('lastUpdated'),
                    'note': i.get('note'),
                    'inclusion_flag': detail.get('inclusionExclusionFlag'),
                    'security_description': detail.get('securityDescription'),
                    'amt_accepted_par': detail.get('amtAcceptedPar')
                }
                all_data_dicts.append(data_dict)
        df = pd.DataFrame(all_data_dicts)
        return df



    def securities_lending_operations(self):
        url="https://markets.newyorkfed.org/api/seclending/all/results/summary/lastTwoWeeks.json"
        r = requests.get(url).json()

        seclending = r['seclending'] if 'seclending' in r else None
        if seclending is not None:
            operations = seclending['operations'] if 'operations' in seclending else None
            if operations is not None:
                df = pd.DataFrame(operations)

                return df

    def treasury_holdings(self):
        url = f"https://markets.newyorkfed.org/api/tsy/all/results/summary/last/10.json"
        r = requests.get(url).json()
        treasury = r['treasury']
        auctions = treasury['auctions'] if 'auctions' in treasury else None
        df = pd.DataFrame(auctions)
        return df
    def data_act_compliance(self):
        base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
        url=base_url+f"/v2/debt/tror/data_act_compliance?filter=record_date:gte:{self.thirty_days_ago},record_date:lte:{today_str}&sort=-record_date,agency_nm,agency_bureau_indicator,bureau_nm"
        r = requests.get(url).json()
        data = r['data']
        df = pd.DataFrame(data)

        return df


    def soma_holdings(self):
        url = f"https://markets.newyorkfed.org/api/soma/summary.json"
        r = requests.get(url).json()
        soma = r['soma']
        summary = soma['summary'] if 'summary' in soma else None

        df = pd.DataFrame(summary)
        return df        

    def market_share(self):
        url="https://markets.newyorkfed.org/api/marketshare/qtrly/latest.json"
        r = requests.get(url).text


        securityType = re.compile(r'"securityType": "(.*?)"')
        security  = re.compile(r'"security": "(.*?)"')
        security_matches = security.findall(r)
        securityType_matches = securityType.findall(r)

        percentFirstQuintRange = re.compile(r'"percentFirstQuintRange": "(.*?)"')
        percentFirstQuintRange_matches = percentFirstQuintRange.findall(r)
        percentFirstQuintMktShare = re.compile(r'"percentFirstQuintMktShare": "(\d+\.\d+)"')
        percentFirstQuintMktShare_matches = percentFirstQuintMktShare.findall(r)


        percentSecondQuintRange = re.compile(r'"percentSecondQuintRange": "(.*?)"')
        percentSecondQuintRange_matches = percentSecondQuintRange.findall(r)
        percentSecondQuintMktShare= re.compile(r'"percentSecondQuintMktShare": "(\d+\.\d+)"')
        percentSecondQuintMktShare_matches = percentSecondQuintMktShare.findall(r)

        percentThirdQuintRange = re.compile(r'"percentThirdQuintRange": "(.*?)"')
        percentThirdQuintRange_matches = percentThirdQuintRange.findall(r)
        percentThirdQuintMktShare = re.compile(r'"percentThirdQuintMktShare": "(\d+\.\d+)"')
        percentThirdQuintMktShare_matches = percentThirdQuintMktShare.findall(r)


        percentFourthQuintRange  = re.compile(r'"percentFourthQuintRange": "(.*?)"')
        percentFourthQuintRange_matches = percentFourthQuintRange.findall(r)
        percentFourthQuintMktShare= re.compile(r'"percentFourthQuintMktShare": "(\d+\.\d+)"')
        percentFourthQuintMktShare_matches = percentFourthQuintMktShare.findall(r)

        percentFifthQuintRange =  re.compile(r'"percentFifthQuintRange": "(.*?)"')
        percentFifthQuintRange_matches = percentFifthQuintRange.findall(r)
        percentFifthQuintMktShare = re.compile(r'"percentFifthQuintMktShare": "(\d+\.\d+)"')
        percentFifthQuintMktShare_matches = percentFifthQuintMktShare.findall(r)



        dailyAvgVolInMillions = re.compile(r'"dailyAvgVolInMillions": (\d+\.\d+)')
        dailyAvgVolInMillions_matches = dailyAvgVolInMillions.findall(r)

        print(percentFirstQuintRange_matches)
        print(percentFirstQuintMktShare_matches)

        print(percentSecondQuintRange_matches)
        print(percentSecondQuintMktShare_matches)

        print(percentThirdQuintRange_matches)
        print(percentThirdQuintMktShare_matches)

        print(percentFourthQuintRange_matches)
        print(percentFourthQuintMktShare_matches)

        print(percentFifthQuintRange_matches)
        print(percentFifthQuintMktShare_matches)


        print(dailyAvgVolInMillions_matches)

        print(security_matches)
        print(securityType_matches)

        # Assuming all lists are of the same length

        # Create a list of dictionaries
        data_dicts = []
        print("Length of percentFirstQuintRange_matches:", len(percentFirstQuintRange_matches))
        print("Length of percentFirstQuintMktShare_matches:", len(percentFirstQuintMktShare_matches))
        print("Length of percentSecondQuintRange_matches:", len(percentSecondQuintRange_matches))
        print("Length of percentSecondQuintMktShare_matches:", len(percentSecondQuintMktShare_matches))
        print("Length of percentThirdQuintRange_matches:", len(percentThirdQuintRange_matches))
        print("Length of percentThirdQuintMktShare_matches:", len(percentThirdQuintMktShare_matches))
        # Assuming all lists are of the same length
        min_length = min(
            len(percentFirstQuintRange_matches),
            len(percentFirstQuintMktShare_matches),
            len(percentSecondQuintRange_matches),
            len(percentSecondQuintMktShare_matches),
            len(percentThirdQuintRange_matches),
            len(percentThirdQuintMktShare_matches),
            len(percentFourthQuintRange_matches),
            len(percentFourthQuintMktShare_matches),
            len(percentFifthQuintRange_matches),
            len(percentFifthQuintMktShare_matches),
            len(security_matches),
            len(securityType_matches))
        for i in range(min_length):
            data_dict = {
                'percentFirstQuintRange': percentFirstQuintRange_matches[i],
                'percentFirstQuintMktShare': percentFirstQuintMktShare_matches[i],
                'percentSecondQuintRange': percentSecondQuintRange_matches[i],
                'percentSecondQuintMktShare': percentSecondQuintMktShare_matches[i],
                'percentThirdQuintRange': percentThirdQuintRange_matches[i],
                'percentThirdQuintMktShare': percentThirdQuintMktShare_matches[i],
                'percentFourthQuintRange': percentFourthQuintRange_matches[i],
                'percentFourthQuintMktShare': percentFourthQuintMktShare_matches[i],
                'percentFifthQuintRange': percentFifthQuintRange_matches[i],  # Changed this line
                'percentFifthQuintMktShare': percentFifthQuintMktShare_matches[i],
                'dailyAvgVolInMillions': dailyAvgVolInMillions_matches[i],
                'security': security_matches[i],  # Changed this line
                'securityType': securityType_matches[i]  # Changed this line
            }
            data_dicts.append(data_dict)

        df = pd.DataFrame(data_dicts)
        return df


    def central_bank_liquidity_swaps(self):
        """Returns operations out of the fed for central bank liquidity swaps
        
        ARGS:

        >>> count:
                    the last n records
        
        default: 10
        
        """

        url = f"https://markets.newyorkfed.org/api/fxs/usdollar/last/100.json"

        r = requests.get(url).json()
        fxswaps = r["fxSwaps"]
        ops = fxswaps["operations"] if "operations" in fxswaps else None
        if ops is not None:
            return pd.DataFrame(ops)
    

    def primary_dealer_timeseries():


        metadata_url = "https://markets.newyorkfed.org/api/pd/list/timeseries.csv"
        metadata_response = requests.get(metadata_url)
        metadata_csv = StringIO(metadata_response.text)
        metadata_df = pd.read_csv(metadata_csv)

        # Download timeseries data in CSV format and load it into a DataFrame
        timeseries_url = "https://markets.newyorkfed.org/api/pd/get/all/timeseries.csv"
        timeseries_response = requests.get(timeseries_url)
        timeseries_csv = StringIO(timeseries_response.text)
        timeseries_df = pd.read_csv(timeseries_csv)

        # Merge the two DataFrames on the common column (assuming it's called 'keyid' in both DataFrames)
        #final_df = pd.merge(timeseries_df, metadata_df, on='keyid', how='left')


        # Merge the two DataFrames on the common columns
        final_df = pd.merge(timeseries_df, metadata_df, left_on='Time Series', right_on='Key Id', how='left')

        # Display the first few rows of the merged DataFrame
        print(final_df.head())

        return final_df



    def reverse_repo(self):
        url = f"https://markets.newyorkfed.org/api/rp/all/all/results/last/10.json"
        r = requests.get(url)
        r.raise_for_status()  # This will raise an exception if the request failed
        repo_data = r.json().get('repo', {})
        operations = repo_data.get('operations', [])

        all_operations = []
        for operation in operations:
            # Common operation data
            op_data_common = {
                'operationId': operation.get('operationId'),
                'auctionStatus': operation.get('auctionStatus'),
                'operationDate': operation.get('operationDate'),
                'settlementDate': operation.get('settlementDate'),
                'maturityDate': operation.get('maturityDate'),
                'operationType': operation.get('operationType'),
                'operationMethod': operation.get('operationMethod'),
                'settlementType': operation.get('settlementType'),
                'termCalenderDays': operation.get('termCalenderDays'),
                'term': operation.get('term'),
                'releaseTime': operation.get('releaseTime'),
                'closeTime': operation.get('closeTime'),
                'note': operation.get('note'),
                'lastUpdated': operation.get('lastUpdated'),
                'participatingCpty': operation.get('participatingCpty'),
                'acceptedCpty': operation.get('acceptedCpty'),
                'totalAmtSubmitted': operation.get('totalAmtSubmitted'),
                'totalAmtAccepted': operation.get('totalAmtAccepted'),
            }

            details = operation.get('details', [])
            for detail in details:
                # Combine common data with detail data for each entry in details
                op_data = {**op_data_common, **{
                    'securityType': detail.get('securityType'),
                    'amtSubmitted': detail.get('amtSubmitted'),
                    'amtAccepted': detail.get('amtAccepted'),
                    'minimumBidRate': detail.get('minimumBidRate'),
                    'percentHighRate': detail.get('percentHighRate'),
                    'percentLowRate': detail.get('percentLowRate'),
                    'percentStopOutRate': detail.get('percentStopOutRate'),
                    'percentWeightedAverageRate': detail.get('percentWeightedAverageRate')
                }}

                all_operations.append(op_data)
        df = pd.DataFrame(all_operations)
        return df, RepoOperations(all_operations)