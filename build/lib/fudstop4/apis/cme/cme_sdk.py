import requests
import pandas as pd
import datetime


class CmeSDK:
    def __init__(self):
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "dnt": "1",
            "priority": "u=1, i",
            "referer": "https://www.cmegroup.com/solutions/risk-management/margin-services/product-margins.html",
            "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        }
    def to_cme_timestamp(dt: datetime.datetime) -> int:
        """
        Converts a Python datetime object to the CME-style integer timestamp 
        (milliseconds since Unix epoch).
        
        Parameters:
            dt (datetime.datetime): The datetime to convert.
        
        Returns:
            int: The number of milliseconds since the Unix epoch.
        """
        # Ensure dt is in UTC so the timestamp matches CME expectations.
        utc_dt = dt.astimezone(datetime.timezone.utc)
        return int(utc_dt.timestamp() * 1000)

    def marginRates(self):

        r = requests.get("https://www.cmegroup.com/CmeWS/mvc/Margins/OUTRIGHT?1=1&sortField=&sortAsc=true&exchange=CME&pageSize=12&pageNumber=1&isProtected&_t=1737506081811", headers=self.headers).json()
        total = r['total']
        props = r['props']
        marginRates = r['marginRates']


        id = [i.get('id') for i in marginRates]
        exchange = [i.get('exchange') for i in marginRates]
        sector = [i.get('sector') for i in marginRates]
        name = [i.get('name') for i in marginRates]
        filePath = [i.get('filePath') for i in marginRates]
        clearingCode = [i.get('clearingCode') for i in marginRates]
        clearingOrg = [i.get('clearingOrg') for i in marginRates]
        productFamily = [i.get('productFamily') for i in marginRates]
        startPeriod = [i.get('startPeriod') for i in marginRates]
        endPeriod = [i.get('endPeriod') for i in marginRates]
        maintenanceRate = [i.get('maintenanceRate') for i in marginRates]
        volScanMaintenanceRate = [i.get('volScanMaintenanceRate') for i in marginRates]
        volScanMaintenanceRatePercent = [i.get('volScanMaintenanceRatePercent') for i in marginRates]
        currency = [i.get('currency') for i in marginRates]

        data_dict = {
            "id": id,
            "exchange": exchange,
            "sector": sector,
            "name": name,
            "filePath": filePath,
            "clearingCode": clearingCode,
            "clearingOrg": clearingOrg,
            "productFamily": productFamily,
            "startPeriod": startPeriod,
            "endPeriod": endPeriod,
            "maintenanceRate": maintenanceRate,
            "volScanMaintenanceRate": volScanMaintenanceRate,
            "volScanMaintenanceRatePercent": volScanMaintenanceRatePercent,
            "currency": currency
        }

        import pandas as pd
        df = pd.DataFrame(data_dict)


        return df



    def rates(self, date):
   
        date_object = datetime.datetime.strptime(date, "%Y-%m-%d")
        timestamp = int(date_object.timestamp() * 1000)

   

        r = requests.get(f"https://www.cmegroup.com/services/sofr-strip-rates/?isProtected&_t={timestamp}", headers=self.headers).json()


        resultsStrip = r['resultsStrip']
        date = [i.get('date') for i in resultsStrip]
        rates = [i.get('rates') for i in resultsStrip]
        average30day = [i.get('average30day') for i in resultsStrip]
        average90day = [i.get('average90day') for i in resultsStrip]
        average180day = [i.get('average180day') for i in resultsStrip]
        index = [i.get('index') for i in resultsStrip]
        overnight = [i.get('overnight') for i in resultsStrip]

        resultsStripDict = {  
            'date': date,
            'avg_30day': average30day,
            'avg_90day': average90day,
            'avg_180day': average180day,
            'index': index,
            'overnight': overnight
        }

        rates_df = pd.DataFrame(resultsStripDict)

        resultsCurve = r['resultsCurve']
        date = [i.get('date') for i in resultsCurve]
        rates = [i.get('rates') for i in resultsCurve]

        sofrFedFundRates = [i.get('sofrFedFundRates') for i in rates]
        sofrFedFundRates = [item for sublist in sofrFedFundRates for item in sublist]
        ff_term = [i.get('term') for i in sofrFedFundRates]
        ff_price = [i.get('price') for i in sofrFedFundRates]

        sofrRates = [i.get('sofrRates') for i in rates]
        sofrRates = [item for sublist in sofrRates for item in sublist]
        sofr_term = [i.get('term') for i in sofrFedFundRates]
        sofr_price = [i.get('price') for i in sofrFedFundRates]


        dict = { 
            'fedfunds_term': ff_term,
            'fedfunds_price': ff_price,
            'sofr_term': sofr_term,
            'sofr_price': sofr_price
        }
        
        df = pd.DataFrame(dict)

        return df
