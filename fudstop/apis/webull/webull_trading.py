import time
from dotenv import load_dotenv
import ta.trend
load_dotenv()
import ta.momentum
import ta.volatility
import ta.volume
from webull import webull as wb
import aiohttp
import ta
import httpx
import pandas as pd
from pytz import timezone
from .webull_helpers import calculate_countdown, calculate_setup
from .trade_models.stock_quote import MultiQuote
from .trade_models.capital_flow import CapitalFlow, CapitalFlowHistory
from .trade_models.deals import Deals
from .trade_models.cost_distribution import CostDistribution, NewCostDist
from .trade_models.etf_holdings import ETFHoldings
from .trade_models.institutional_holdings import InstitutionHolding, InstitutionStat
from .trade_models.financials import BalanceSheet, FinancialStatement, CashFlow
from .trade_models.news import NewsItem
from .trade_models.forecast_evaluator import ForecastEvaluator
from .trade_models.short_interest import ShortInterest
from fudstop.apis.webull.webull_option_screener import WebullOptionScreener
from .trade_models.volume_analysis import WebullVolAnalysis
from .trade_models.ticker_query import WebullStockData
from .trade_models.analyst_ratings import Analysis
from .trade_models.price_streamer import PriceStreamer
from .trade_models.company_brief import CompanyBrief, Executives, Sectors
from .trade_models.order_flow import OrderFlow
import asyncio
import ta as _ta
from datetime import datetime, timedelta, timezone

screen = WebullOptionScreener()
webull = wb()
class WebullTrading:
    def __init__(self, etf_list=None):
        self.most_active_tickers= ['SNOW', 'IBM', 'DKNG', 'SLV', 'NWL', 'SPXS', 'DIA', 'QCOM', 'CMG', 'WYNN', 'PENN', 'HLF', 'CCJ', 'WW', 'NEM', 'MOS', 'SRPT', 'MS', 'DPST', 'AG', 'PAA', 'PANW', 'XPEV', 'BHC', 'KSS', 'XLP', 'LLY', 'MDB', 'AZN', 'NVO', 'BOIL', 'ZM', 'HUT', 'VIX', 'PDD', 'SLB', 'PCG', 'DIS', 'TFC', 'SIRI', 'TDOC', 'CRSP', 'BSX', 'BITF', 'AAL', 'EOSE', 'RIVN', 'X', 'CCL', 'SOXS', 'NOVA', 'TMUS', 'HES', 'LI', 'NVAX', 'TSM', 'CNC', 'IAU', 'GDDY', 'CVX', 'TGT', 'MCD', 'GDXJ', 'AAPL', 'NKLA', 'EDR', 'NOK', 'SPWR', 'NKE', 'HYG', 'FSLR', 'SGEN', 'DNN', 'BAX', 'CRWD', 'OSTK', 'XLC', 'RIG', 'SEDG', 'SNDL', 'RSP', 'M', 'CD', 'UNG', 'LQD', 'TTD', 'AMGN', 'EQT', 'YINN', 'MULN', 'FTNT', 'WBD', 'MRNA', 'PTON', 'SCHW', 'ABNB', 'EW', 'PM', 'UCO', 'TXN', 'DLR', 'KHC', 'MMAT', 'QQQ', 'GOOGL', 'AEM', 'RTX', 'AVGO', 'RBLX', 'PAAS', 'UUP', 'OXY', 'SQ', 'PLUG', 'CLF', 'GOEV', 'BKLN', 'ALB', 'BALL', 'SMH', 'CVE', 'F', 'KRE', 'TWLO', 'ARCC', 'ARM', 'U', 'SOFI', 'SBUX', 'FXI', 'BMY', 'HSBC', 'EFA', 'SVXY', 'VALE', 'GOLD', 'MSFT', 'OIH', 'ARKK', 'AMD', 'AA', 'DXCM', 'ABT', 'WOLF', 'FDX', 'SOXL', 'MA', 'KWEB', 'BP', 'SNAP', 'NLY', 'KGC', 'URA', 'UVIX', 'KMI', 'ACB', 'NET', 'W', 'GRAB', 'LMT', 'EPD', 'FCX', 'STNE', 'NIO', 'SU', 'ET', 'CVS', 'ADBE', 'MXL', 'HOOD', 'FUBO', 'RIOT', 'CRM', 'TNA', 'DISH', 'XBI', 'VFS', 'GPS', 'NVDA', 'MGM', 'MRK', 'ABBV', 'LABU', 'BEKE', 'VRT', 'LVS', 'CPNG', 'BA', 'MTCH', 'PEP', 'EBAY', 'GDX', 'XLV', 'UBER', 'GOOG', 'COF', 'XLU', 'BILI', 'XLK', 'VXX', 'DVN', 'MSOS', 'KOLD', 'XOM', 'BKNG', 'SPY', 'RUT', 'CMCSA', 'STLA', 'NCLH', 'GRPN', 'ZION', 'UAL', 'GM', 'NDX', 'TQQQ', 'COIN', 'WBA', 'CLSK', 'NFLX', 'FREY', 'AFRM', 'NAT', 'EEM', 'IYR', 'KEY', 'OPEN', 'DM', 'TSLA', 'BXMT', 'T', 'TZA', 'BAC', 'MARA', 'UVXY', 'LOW', 'COST', 'HL', 'CHTR', 'TMF', 'ROKU', 'DOCU', 'PSEC', 'XHB', 'VMW', 'SABR', 'USB', 'DDOG', 'DB', 'V', 'NOW', 'XRT', 'SMCI', 'PFE', 'NYCB', 'BIDU', 'C', 'SPX', 'ETSY', 'EMB', 'SQQQ', 'CHPT', 'DASH', 'VZ', 'DNA', 'CL', 'ANET', 'WMT', 'MRO', 'WFC', 'MO', 'USO', 'ENVX', 'INTC', 'GEO', 'VFC', 'WE', 'MET', 'CHWY', 'PBR', 'KO', 'TH', 'QS', 'BTU', 'GLD', 'JD', 'XLY', 'KR', 'ASTS', 'WDC', 'HTZ', 'XLF', 'COP', 'PATH', 'SHEL', 'MXEF', 'SE', 'SPCE', 'UPS', 'RUN', 'DOW', 'ASHR', 'ONON', 'DAL', 'SPXL', 'SAVE', 'LUV', 'HD', 'JNJ', 'LYFT', 'UNH','NEE', 'STNG', 'SPXU', 'MMM', 'VNQ', 'IMGN', 'MSTR', 'AXP', 'TMO', 'XPO', 'FEZ', 'ENPH', 'AX', 'NVCR', 'GS', 'MRVL', 'ADM', 'GILD', 'IBB', 'PARA', 'PINS', 'JBLU', 'SNY', 'BITO', 'PYPL', 'FAS', 'GME', 'LAZR', 'URNM', 'BX', 'MPW', 'UPRO', 'HPQ', 'AMZN', 'SAVA', 'TLT', 'ON', 'CAT', 'VLO', 'AR', 'IDXX', 'SWN', 'META', 'BABA', 'ZS', 'EWZ', 'ORCL', 'XOP', 'TJX', 'XP', 'EL', 'HAL', 'IEF', 'XLI', 'UPST', 'Z', 'TELL', 'LRCX', 'DLTR', 'BYND', 'PACW', 'CVNA', 'GSAT', 'CSCO', 'NU', 'KVUE', 'JPM', 'LCID', 'TLRY', 'AGNC', 'CGC', 'XLE', 'VOD', 'TEVA', 'JETS', 'UEC',  'ZIM', 'ABR', 'IQ', 'AMC', 'ALLY', 'HE', 'OKTA', 'ACN', 'MU', 'FLEX', 'SHOP', 'PLTR', 'CLX', 'LUMN', 'WHR', 'PAGP', 'IWM', 'WPM', 'TTWO', 'AI', 'ALGN', 'SPOT', 'BTG', 'IONQ', 'GE', 'DG', 'AMAT', 'XSP', 'PG', 'LULU', 'DE', 'MDT', 'RCL']
        self.scalar_tickers = ['SPX', 'VIX', 'OSTK', 'XSP', 'NDX', 'MXEF']
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.semaphore = asyncio.Semaphore(10)
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.timeframes = ['m1','m5', 'm10', 'm15', 'm20', 'm30', 'm60', 'm120', 'm240', 'd1']
        self.now_timestamp_int = int(datetime.now(timezone.utc).timestamp())
        self.day = int(86400)
        self.ticker_df = pd.read_csv('files/ticker_csv.csv')
        self.id = 15765933
        self.etf_list = etf_list
        self.ticker_to_id_map = dict(zip(self.ticker_df['ticker'], self.ticker_df['id']))
        self.client = httpx.AsyncClient()
        #miscellaenous
                #sessions
        # Define the dictionary with all the rules
# Define the dictionary with all the rules
        self.rules_dict = {
            "boll": {
                "bullbear": [
                    "wlas.screener.value.bullbear.bullish",
                    "wlas.screener.value.bullbear.bear"
                ],
                "term": [
                    "wlas.screener.value.term.long",
                    "wlas.screener.value.term.inter",
                    "wlas.screener.value.term.short"
                ]
            },
            "fsto": {
                "bullbear": [
                    "wlas.screener.value.bullbear.bullish",
                    "wlas.screener.value.bullbear.bear"
                ],
                "term": [
                    "wlas.screener.value.term.long",
                    "wlas.screener.value.term.inter"
                ]
            },
            "macd": {
                "bullbear": [
                    "wlas.screener.value.bullbear.bullish"
                ]
            },
            "rsitech": {
                "bullbear": [
                    "wlas.screener.value.bullbear.bullish"
                ],
                "term": [
                    "wlas.screener.value.term.long"
                ]
            },
            "william": {
                "bullbear": [
                    "wlas.screener.value.bullbear.bullish",
                    "wlas.screener.value.bullbear.bear"
                ],
                "term": [
                    "wlas.screener.value.term.long",
                    "wlas.screener.value.term.short",
                    "wlas.screener.value.term.inter"
                ]
            },
            "cci": {
                "term": [
                    "wlas.screener.value.term.inter",
                    "wlas.screener.value.term.short",
                    "wlas.screener.value.term.long"
                ],
                "bullbear": [
                    "wlas.screener.value.bullbear.bear",
                    "wlas.screener.value.bullbear.bullish"
                ]
            },
            "kst": {
                "bullbear": [
                    "wlas.screener.value.bullbear.bullish",
                    "wlas.screener.value.bullbear.bear"
                ],
                "term": [
                    "wlas.screener.value.term.long",
                    "wlas.screener.value.term.short",
                    "wlas.screener.value.term.inter"
                ]
            },
            "mom": {
                "bullbear": [
                    "wlas.screener.value.bullbear.bullish",
                    "wlas.screener.value.bullbear.bear"
                ],
                "term": [
                    "wlas.screener.value.term.long",
                    "wlas.screener.value.term.inter",
                    "wlas.screener.value.term.short"
                ]
            },
            "slowstach": {
                "bullbear": [
                    "wlas.screener.value.bullbear.bullish",
                    "wlas.screener.value.bullbear.bear"
                ],
                "term": [
                    "wlas.screener.value.term.long",
                    "wlas.screener.value.term.short",
                    "wlas.screener.value.term.inter"
                ]
            }
        }
        self.candle_patterns = ['gravestone', 'insidebar', 'outsidebar', 'gud', 'tbr', 'ibt', 'hhm', 'eb']
        self.indicators = ['mom', 'slowstach', 'kst', 'cci', 'william', 'rsi', 'macd', 'boll', 'fsto']
        self.ticker_df = pd.read_csv('files/ticker_csv.csv')
        self.ticker_to_id_map = dict(zip(self.ticker_df['ticker'], self.ticker_df['id']))
    def is_etf(self, symbol):
        """Check if a symbol is an ETF."""
        return symbol in self.etf_list['Symbol'].values
    async def fetch_endpoint(self, headers, endpoint):
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(endpoint) as resp:
                return await resp.json()


    async def get_webull_id(self, symbol):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        ticker_id = self.ticker_to_id_map.get(symbol)
        return ticker_id

    async def multi_quote(self, tickers=['AAPL', 'SPY']):
        """Query multiple tickers using the Webull API"""


        tasks = [self.ticker_to_id_map.get(str(ticker)) for ticker in tickers]
        
        ticker_ids = await asyncio.gather(*tasks)


        ticker_ids = ','.join(str(ticker_id) for ticker_id in ticker_ids)
        print(ticker_ids)
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={ticker_ids}&includeSecu=1&delay=0&more=1"

        async with httpx.AsyncClient() as client:
            response = await client.get(endpoint)
            data = response.json()


            multi_quote = MultiQuote(data)

            return multi_quote
        
    async def candle_pattern_screener(self, headers, candle_pattern):
        """
        
        Candle types:
        
        
        >>> ihss - inverted hammer
        >>> gravestone
        >>> insidebar
        >>> outsidebar
        >>> gud - gap up or down
        >>> tbr - two bar reversal
        >>> ibt - island bottom/top
        >>> hhm - hammer / hanging man
        >>> eb - exhaustion bar
        """
        url = "https://quotes-gw.webullfintech.com/api/wlas/screener/ng/query"

        payload = {"fetch":200,"rules":{"wlas.screener.rule.region":"securities.region.name.6","wlas.screener.group.technical.signals":None,f"wlas.screener.rule.{candle_pattern}":"{\"wlas.screener.value.bullbear\":[\"wlas.screener.value.bullbear.bullish\",\"wlas.screener.value.bullbear.bear\"],\"wlas.screener.value.term\":[\"wlas.screener.value.term.long\",\"wlas.screener.value.term.inter\",\"wlas.screener.value.term.short\"]}"},"sort":{"rule":"wlas.screener.rule.volume","desc":True},"attach":{"hkexPrivilege":False}}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                response_data = response.json()

                items = response_data.get('items')

                symbols = [i.get('symbol') for i in items]

                return symbols
            

    async def technical_screener(self, headers, technical_indicator, sentiment, time_horizon):
        """indicator types:
        >>> mom - momentum
        >>> slowstach
        >>> kst
        >>> cci
        >>> william
        >>> rsi
        >>> macd
        >>> fsto
        >>> boll

        sentiment:
        >>> bullish
        >>> bear

        time horizons:
        >>> long
        >>> short
        >>> inter (medium)

        """
        url = "https://quotes-gw.webullfintech.com/api/wlas/screener/ng/query"
        payload = {
            "fetch": 200,
            "rules": {
                "wlas.screener.rule.region": "securities.region.name.6",
                "wlas.screener.group.technical.signals": None,
                f"wlas.screener.rule.{technical_indicator}": f"""{{
                    "wlas.screener.value.bullbear": ["wlas.screener.value.bullbear.{sentiment}"],
                    "wlas.screener.value.term": ["wlas.screener.value.term.{time_horizon}"]
                }}"""
            },
            "sort": {
                "rule": "wlas.screener.rule.price",
                "desc": True
            },
            "attach": {
                "hkexPrivilege": False
            }
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload, headers=headers) as response:
                    response_data = response.json()


                    items = response_data.get('items')
                    ticker = [i.get('ticker') for i in items]
                    symbols = [i.get('symbol') for i in ticker]
                    volume = [float(i.get('volume')) for i in ticker]


                    dict = { 
                        'sym': symbols,
                    }

                    df = pd.DataFrame(dict)
                    df['indicator'] = technical_indicator
                    df['sentiment'] = sentiment
                    df['timeframe'] = time_horizon
                    return df
            except Exception as e:
                return f"No results for {technical_indicator} on the {time_horizon} {sentiment}"

            

    async def run_screeners_for_signal(self, headers, technical_signal):
        results = []
        rule_values = self.rules_dict[technical_signal]
        bullbear_values = rule_values.get("bullbear", ["wlas.screener.value.bullbear.bullish", "wlas.screener.value.bullbear.bear"])
        term_values = rule_values.get("term", ["wlas.screener.value.term.long", "wlas.screener.value.term.inter", "wlas.screener.value.term.short"])

        for bullbear in bullbear_values:
            for term in term_values:
                result = await self.fetch_screened_data(headers, technical_signal, bullbear, term)
                results.append(result)
        
        # Concat all results into a final DataFrame
        final_df = pd.DataFrame(results)
        return final_df
  
    # Detecting unfilled gaps in stock price data
    async def find_unfilled_gaps(self, ticker:str, interval:str):
        ticker = ticker.upper()
        unfilled_gaps=[]
        async for df in self.get_bars(ticker=ticker, interval=interval):

            df.sort_values(by='Timestamp', ascending=True, inplace=True)
            # Assuming the DataFrame is sorted in ascending order by 'Timestamp'
            for i in range(1, len(df)):
                previous_row = df.iloc[i - 1]
                current_row = df.iloc[i]
                
                # Checking for gap up
                if current_row['Low'] > previous_row['High']:
                    gap = {
                        'gap_date': current_row['Timestamp'],
                        'gap_range': (previous_row['High'], current_row['Low'])
                    }
                    # Check in the following days if the gap has been filled
                    filled = df[i+1:].apply(
                        lambda row: row['Low'] <= gap['gap_range'][1] and row['High'] >= gap['gap_range'][0], axis=1
                    ).any()
                    if not filled:
                        unfilled_gaps.append(gap)

                # Checking for gap down
                elif current_row['High'] < previous_row['Low']:
                    gap = {
                        'gap_date': current_row['Timestamp'],
                        'gap_range': (current_row['High'], previous_row['Low'])
                    }
                    # Check in the following days if the gap has been filled
                    filled = df[i+1:].apply(
                        lambda row: row['Low'] <= gap['gap_range'][1] and row['High'] >= gap['gap_range'][0], axis=1
                    ).any()
                    if not filled:
                        unfilled_gaps.append(gap)

            return unfilled_gaps
        


    async def deals(self, symbol: str, headers):
        try:
            tickerId = self.ticker_to_id_map.get(symbol)
            if not tickerId:
                raise ValueError(f"Ticker {symbol} not found in ticker_to_id_map.")

            endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/deals?count=50000&tickerId={tickerId}"

            async with httpx.AsyncClient(headers=headers) as response:
                data  = await response.get(endpoint)
                data = response.json()
                deal_data = data.get('data', [])
                return Deals(deal_data)

        except Exception as e:
            print(f"Error in deals: {e}")
            return None



    async def get_analyst_ratings(self, symbol:str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint=f"https://quotes-gw.webullfintech.com/api/information/securities/analysis?tickerId={ticker_id}"
            async with httpx.AsyncClient() as response:
                data = await response.get(endpoint)
                if data.status_code == 200:
                    datas = data.json()
                    data = Analysis(datas)
                    return data
        except Exception as e:
            print(e)
    

    async def get_short_interest(self, symbol: str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found in ticker_to_id_map.")
            
            endpoint = f"https://quotes-gw.webullfintech.com/api/information/brief/shortInterest?tickerId={ticker_id}"
            async with httpx.AsyncClient() as response:  # Use the client without closing it
                data = await response.get(endpoint)
                datas = data.json()
                data = ShortInterest(datas)
                return data

        except Exception as e:
            print(f"Error: {e}")
    
    async def institutional_holding(self, symbol:str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/information/stock/getInstitutionalHolding?tickerId={ticker_id}"
            async with httpx.AsyncClient() as response:
                data = await response.get(endpoint)
                if data.status_code == 200:
                    datas = data.json()
                    data = InstitutionStat(datas)
                    return data
        except Exception as e:
            print(e)
    

    async def volume_analysis(self, symbol:str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?count=10&tickerId={ticker_id}&type=0"
            async with httpx.AsyncClient() as response:
                data = await response.get(endpoint)
                datas = data.json()
                datas = WebullVolAnalysis(datas, symbol)
                return datas
        except Exception as e:
            print(e)
    

    async def new_cost_dist(self, symbol:str, start_date:str, end_date:str):
        """Returns list"""
        tickerId = self.ticker_to_id_map.get(symbol)
        try:
            endpoint = f"https://quotes-gw.webullfintech.com/api/quotes/chip/query?tickerId={tickerId}&startDate={start_date}&endDate={end_date}"
      
            async with httpx.AsyncClient() as response:
                data = await response.get(endpoint)
                if data.status_code == 200:
                    data = data.json()
                    data = data['data']
                    return NewCostDist(data,symbol)
        except Exception as e:
            print(e)


    async def cost_distribution(self, symbol:str, start_date:str=None, end_date:str=None):
        try:

            if start_date == None:
                start_date = self.thirty_days_ago
                

            if end_date == None:
                end_date = self.today

            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/quotes/chip/query?tickerId={ticker_id}&startDate={start_date}&endDate={end_date}"
            print(endpoint)
            datas = await self.fetch_endpoint(endpoint)
            data = CostDistribution(datas, symbol)
            return data
        except Exception as e:
            print(e)
        

    async def stock_quote(self, symbol:str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={ticker_id}&includeSecu=1&delay=0&more=1"
            datas = await self.fetch_endpoint(endpoint)
            data = WebullStockData(datas)
            return data
        except Exception as e:
            print(e)

    async def financials(self, symbol:str, financials_type:str='balancesheet'):
        """Argument
        
        Symbol: the symbol to query
        """
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/{financials_type}?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit=4"
        
            async with httpx.AsyncClient() as response:
                data = await response.get(endpoint)
                datas = response.json()
                data = datas['data'] if 'data' in datas else None
                if data is not None and financials_type == 'incomestatement':
                    data = FinancialStatement(datas).df.to_dict('records')
                    return data
                if data is not None and financials_type == 'balancesheet':
                    data = BalanceSheet(datas).df.to_dict('records')
                    return data
                if data is not None and financials_type == 'cashflow':
                    data = CashFlow(datas).df.to_dict('records')
                    return data
        except Exception as e:
            print(e)
    


    async def quote(self, ticker):
        try:
            tickerid = self.ticker_to_id_map.get(ticker)
            if not tickerid:
                raise ValueError(f"Ticker {ticker} not found in ticker_to_id_map.")

            url = f"https://quotes-gw.webullfintech.com/api/stock/tickerRealTime/getQuote?tickerId={tickerid}&includeSecu=1&includeQuote=1&more=1"
            async with httpx.AsyncClient() as response:
                data = await response.get(url)
                data = data.json()
                result = {}

                if not self.is_etf(ticker):
                    result.update({
                        'forward_pe': float(data.get('forwardPe', 0)),
                        'indicated_pe': float(data.get('indicatedPe', 0)),
                        'pe_ttm': float(data.get('peTtm', 0)),
                        'eps': float(data.get('eps', 0)),
                        'eps_ttm': float(data.get('epsTtm', 0)),
                        'price_to_book': float(data.get('pb', 0)),
                    })

                result.update({
                    'underlying_open': float(data.get('open', 0)),
                    'underlying_close': float(data.get('close', 0)),
                    'underlying_high': float(data.get('high', 0)),
                    'underlying_low': float(data.get('low', 0)),
                    'underlying_change_pct': round(float(data.get('changeRatio', 0))),
                    'underlying_volume': float(data.get('volume', 0)),
                    'vibrate_ratio': float(data.get('vibrateRatio', 0)),
                    'avg_vol_10d': float(data.get('avgVol10D', 0)),
                    'avg_vol_3m': float(data.get('avgVol3M', 0)),
                })

                return result

        except Exception as e:
            print(f"Error in quote: {e}")
            return None




    async def news(self, symbol: str, pageSize: str = '100', headers=None):
        try:
            if headers is None:
                raise ValueError("Headers are required but not provided.")

            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found in ticker_to_id_map.")

            endpoint = f"https://nacomm.webullfintech.com/api/information/news/tickerNews?tickerId={ticker_id}&currentNewsId=0&pageSize={pageSize}"
            async with httpx.AsyncClient(headers=headers) as response:
                response = await response.get(endpoint)
                if response.status_code == 200:
                    datas = response.json()
                    return NewsItem(datas)
                else:
                    raise Exception(f"Failed to fetch news data: {response.status_code}")
        except Exception as e:
            print(f"Error in news: {e}")
            return None

    

    async def company_brief(self, symbol:str, as_dataframe:bool=False):
        """
        RETURNS THREE THINGS

        >>> companyBrief_df
        >>> executives_df
        >>> sectors_df
        """
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint=f"https://quotes-gw.webullfintech.com/api/information/stock/brief?tickerId={ticker_id}"    
            async with httpx.AsyncClient() as response:
                data = await response.get(endpoint)
                data = response.json()
                datas = data


                companyBrief = CompanyBrief(datas['companyBrief'])
                sectors = Sectors(datas['sectors'])
                executives = Executives(datas['executives'])

                # Convert to DataFrames
                companyBrief_df = companyBrief.as_dataframe
                sectors_df = sectors.as_dataframe
                executives_df = executives.as_dataframe

                
                return companyBrief, sectors, executives
        except Exception as e:
            print(e)

    async def balance_sheet(self, symbol:str, limit:str='11'):
        ticker_id = self.ticker_to_id_map.get(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/balancesheet?tickerId={ticker_id}&type=101&fiscalPeriod=0&limit={limit}"
        async with httpx.AsyncClient() as response:
            data = await response.get(endpoint)
            if data.status_code == 200:
                datas = data.json()
                data = BalanceSheet(datas)
                return data
    
    async def cash_flow(self, symbol:str, limit:str='12'):
        ticker_id = self.ticker_to_id_map.get(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/cashflow?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit={limit}"
        async with httpx.AsyncClient() as response:
            data = await response.get(endpoint)
            if data.status_code == 200:
                datas = data.json()
                data = CashFlow(datas)
                return data
    
    async def income_statement(self, symbol:str, limit:str='12'):
        ticker_id = self.ticker_to_id_map.get(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/incomestatement?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit={limit}"
        async with httpx.AsyncClient() as response:
            data = await response.get(endpoint)
            if data.status_code == 200:
                datas = data.json()
                data = FinancialStatement(datas)
                return data
    

    async def order_flow(self, headers, symbol: str, type: str = '0', count: str = '1'):
        """
        Gets order flow for tickers
        """
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found in ticker_to_id_map.")

            endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?count={count}&tickerId={ticker_id}&type={type}"

            async with httpx.AsyncClient(headers=headers) as client:
                response = await client.get(endpoint)
                if response.status_code == 200:
                    data = response.json()
                    return OrderFlow(data)
                else:
                    raise Exception(f"Failed to fetch order flow data. HTTP Status: {response.status_code}")
        except Exception as e:
            print(f"Error in order_flow: {e}")
            return None


    async def price_streamer(self, symbol:str, type:str='0'):
        """
        Type:
        >>> 0 = today
        >>> 1 = yesterday
        """
        ticker_id = self.ticker_to_id_map.get(symbol)
        url=f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?count=50000&tickerId={ticker_id}&type={type}"
        async with httpx.AsyncClient() as client:
            data = await client.get(url)
            data = data.json()

            return PriceStreamer(data)


    async def capital_flow(self, symbol: str):
        """
        Fetches and returns the latest and historical capital flow data for a given symbol.

        Args:
            symbol (str): Stock ticker symbol.

        Returns:
            Tuple[CapitalFlow, CapitalFlowHistory]: A tuple containing the latest and historical capital flow data.
        """
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found in ticker_to_id_map.")

            endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/ticker?tickerId={ticker_id}&showHis=true"
            
            async with httpx.AsyncClient() as response:
                response.raise_for_status()  # Raise exception for non-200 status codes
                data = await response.get(endpoint)
                datas = data.json()

                # Extract latest and historical data from the response
                latest = datas.get('latest', {})
                historical = datas.get('historical', [])

                # Extract necessary attributes for initialization
                dates = [i.get('date') for i in historical]
                historical_items = [i.get('item') for i in historical]
                latest_item = latest.get('item', {})

                # Initialize CapitalFlow and CapitalFlowHistory instances
                data = CapitalFlow(latest_item, ticker=symbol)
                history = CapitalFlowHistory(historical_items, dates)

                return data, history
        except httpx.RequestError as req_err:
            print(f"An error occurred while requesting data for {symbol}: {req_err}")
        except httpx.HTTPStatusError as http_err:
            print(f"HTTP error occurred for {symbol}: {http_err.response.status_code}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        return None, None

            

    async def etf_holdings(self, symbol:str, pageSize:str='200'):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/information/company/queryEtfList?tickerId={ticker_id}&pageIndex=1&pageSize={pageSize}"
            async with httpx.AsyncClient() as response:
                data = await response.get(endpoint)
                if data.status_code == 200:
                    datas = data.json()

                    data = ETFHoldings(datas)
                    return data
        except Exception as e:
            print(e)
        

    
    async def get_quote(self, ticker):
        try:
            ticker_id = self.ticker_to_id_map.get(ticker)
            endpoint = f"https://quotes-gw.webullfintech.com/api/stock/tickerRealTime/getQuote?tickerId={ticker_id}&includeSecu=1&includeQuote=1&more=1"
            async with httpx.AsyncClient() as response:
                data = await response.get(endpoint)
                data = data.json()
                # Updated data_dict


                # Extracting data from the input dictionary
                name = data.get('name', None)
                symbol = data.get('symbol', None)
                derivative_support = data.get('derivativeSupport', 0)
                close = float(data.get('close', 0))
                change = float(data.get('change', 0))
                change_ratio = round(float(data.get('changeRatio', 0)) * 100, 2)
                market_value = float(data.get('marketValue', 0))
                volume = float(data.get('volume', 0))
                turnover_rate = round(float(data.get('turnoverRate', 0)) * 100, 2)
                open = float(data.get('open', 0))
                high = float(data.get('high', 0))
                low = float(data.get('low', 0))
                vibrate_ratio = float(data.get('vibrateRatio', 0))
                avg_vol_10d = float(data.get('avgVol10D', 0))
                avg_vol_3m = float(data.get('avgVol3M', 0))
                neg_market_value = float(data.get('negMarketValue', 0))
                pe = float(data.get('pe', 0))
                forward_pe = float(data.get('forwardPe', 0))
                indicated_pe = float(data.get('indicatedPe', 0))
                pe_ttm = float(data.get('peTtm', 0))
                eps = float(data.get('eps', 0))
                eps_ttm = float(data.get('epsTtm', 0))
                pb = float(data.get('pb', 0))
                total_shares = float(data.get('totalShares', 0))
                outstanding_shares = float(data.get('outstandingShares', 0))
                fifty_two_wk_high = float(data.get('fiftyTwoWkHigh', 0))
                fifty_two_wk_low = float(data.get('fiftyTwoWkLow', 0))
                dividend = float(data.get('dividend', 0))
                yield_ = float(data.get('yield', 0))
                latest_dividend_date = data.get('latestDividendDate', None)
                latest_split_date = data.get('latestSplitDate', None)
                latest_earnings_date = data.get('latestEarningsDate', None)
                ps = float(data.get('ps', 0))
                bps = float(data.get('bps', 0))
                estimate_earnings_date = data.get('estimateEarningsDate', None)

                # Calculate percentage from 52-week high
                pct_from_52_high = ((fifty_two_wk_high - close) / fifty_two_wk_high) * 100 if fifty_two_wk_high != 0 else None
                pct_from_52_high = round(float(pct_from_52_high),2)
                # Calculate percentage from 52-week low
                pct_from_52_low = ((close - fifty_two_wk_low) / fifty_two_wk_low) * 100 if fifty_two_wk_low != 0 else None
                pct_from_52_low = round(float(pct_from_52_low),2)
                # Calculate volume vs average volume (10 days)
                volume_vs_avg_vol_10d = (volume / avg_vol_10d) if avg_vol_10d != 0 else None
                volume_vs_avg_vol_10d = round(float(volume_vs_avg_vol_10d)* 100, 2)
                # Calculate volume vs average volume (3 months)
                volume_vs_avg_vol_3m = (volume / avg_vol_3m) if avg_vol_3m != 0 else None
                volume_vs_avg_vol_3m = round(float(volume_vs_avg_vol_3m)* 100, 2)

                # Create new earnings metrics
                earnings_to_price = (eps_ttm / close) if close != 0 else None
                earnings_to_price = round(float(earnings_to_price)*100,2)
                forward_earnings_to_price = (forward_pe / close) if close != 0 else None
                forward_earnings_to_price = round(float(forward_earnings_to_price) * 100, 2)

                data_dict = {
                    'name': name,
                    'ticker': symbol,
                    'derivative_support': derivative_support,
                    'close': close,
                    'change': change,
                    'change_ratio': change_ratio,
                    'market_value': market_value,
                    'volume': volume,
                    'turnover_rate': turnover_rate,
                    'open': open,
                    'high': high,
                    'low': low,
                    'vibrate_ratio': vibrate_ratio,
                    'avg_vol_10d': avg_vol_10d,
                    'avg_vol_3m': avg_vol_3m,
                    'neg_market_value': neg_market_value,
                    'pe': pe,
                    'forward_pe': forward_pe,
                    'indicated_pe': indicated_pe,
                    'pe_ttm': pe_ttm,
                    'eps': eps,
                    'eps_ttm': eps_ttm,
                    'pb': pb,
                    'total_shares': total_shares,
                    'outstanding_shares': outstanding_shares,
                    'fifty_two_wk_high': fifty_two_wk_high,
                    'fifty_two_wk_low': fifty_two_wk_low,
                    'dividend': dividend,
                    'yield': yield_,
                    'latest_dividend_date': latest_dividend_date,
                    'latest_split_date': latest_split_date,
                    'latest_earnings_date': latest_earnings_date,
                    'ps': ps,
                    'bps': bps,
                    'estimate_earnings_date': estimate_earnings_date,
                    'pct_from_52_high': pct_from_52_high,
                    'pct_from_52_low': pct_from_52_low,
                    'volume_vs_avg_vol_10d': volume_vs_avg_vol_10d,
                    'volume_vs_avg_vol_3m': volume_vs_avg_vol_3m,
                    'earnings_to_price': earnings_to_price,
                    'forward_earnings_to_price': forward_earnings_to_price,

                }

                # Print to verify the updated dictionary
            
                df = pd.DataFrame(data_dict, index=[0])

                return df
        except Exception as e:
            print(e)
    async def async_get_td9(self, ticker, interval, headers, count: str = '13'):
        try:
            timeStamp = None
            if ticker == 'I:SPX':
                ticker = 'SPXW'
            elif ticker == 'I:NDX':
                ticker = 'NDX'
            elif ticker == 'I:VIX':
                ticker = 'VIX'
            elif ticker == 'I:RUT':
                ticker = 'RUT'
            elif ticker == 'I:XSP':
                ticker = 'XSP'

            tickerid = self.ticker_to_id_map.get(ticker)

            if not tickerid:
                raise ValueError(f"Ticker {ticker} not found in ticker_to_id_map.")

            if timeStamp is None:
                timeStamp = int(time.time())  # Default to current time if not provided

            base_fintech_gw_url = f"https://quotes-gw.webullfintech.com/api/quote/charts/query?tickerIds={tickerid}&type={interval}&timestamp={timeStamp}&count={count}&extendTrading=1"

            interval_mapping = {
                'm5': '5 min',
                'm30': '30 min',
                'm60': '1 hour',
                'm120': '2 hour',
                'm240': '4 hour',
                'd': 'day',
                'w': 'week',
                'm': 'month'
            }
            timespan = interval_mapping.get(interval, 'minute')

            async with httpx.AsyncClient(headers=headers) as response:
                data = await response.get(base_fintech_gw_url)
                r = data.json()
                if r and isinstance(r, list) and 'data' in r[0]:
                    data = r[0]['data']
                    if data is not None:
                        parsed_data = []
                        for entry in data:
                            values = entry.split(',')
                            if values[-1] == 'NULL':
                                values = values[:-1]
                            parsed_data.append([float(value) if value != 'null' else 0.0 for value in values])

                        sorted_data = sorted(parsed_data, key=lambda x: x[0], reverse=True)

                        columns = ['Timestamp', 'Open', 'Close', 'High', 'Low', 'N', 'Volume', 'Vwap'][:len(sorted_data[0])]

                        df = pd.DataFrame(sorted_data, columns=columns)
                        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s', utc=True)
                        df['Timestamp'] = df['Timestamp'].dt.tz_convert('US/Eastern').dt.tz_localize(None)
                        df['Ticker'] = ticker
                        df['timespan'] = timespan

                        return df
                else:
                    raise ValueError("Unexpected data structure or empty response.")
   
        except Exception as e:
            print(f"Error in async_get_td9: {e}")
            return None

    async def get_bars(self, ticker, interval:str='m1', timeStamp=None, headers=None):
        if ticker == 'I:SPX':
            ticker = 'SPXW'
        elif ticker =='I:NDX':
            ticker = 'NDX'
        elif ticker =='I:VIX':
            ticker = 'VIX'
        
        tickerid = await self.get_webull_id(ticker)




        if timeStamp is None:
            # if not set, default to current time
            timeStamp = int(time.time()) - 25000

        base_fintech_gw_url = f'https://quotes-gw.webullfintech.com/api/quote/charts/kdata/latest?tickerIds={tickerid}&type={interval}&count=800&timestamp={timeStamp}&restorationType=1&direction=1&extendTrading=1'
        print(base_fintech_gw_url)

  
        async with aiohttp.ClientSession(headers=headers) as session:
            if interval == 'm1':
                timespan = 'minute'
            elif interval == 'm60':
                timespan = '1 hour'
            elif interval == 'm5':
                timespan = '5 minute'
            elif interval == 'm15':
                timespan = '15 minute'
            elif interval == 'm30':
                timespan = '30 minute'
            elif interval == 'm60':
                timespan = '1 hour'
            elif interval == 'm120':
                timespan = '2 hour'
            elif interval == 'm240':
                timespan = '4 hour'
            elif interval == 'd1':
                timespan = 'day'
            elif interval == 'w':
                timespan = 'weekly'
            elif interval == 'm':
                timespan = 'monthly'
            async with session.get(base_fintech_gw_url, headers=headers) as resp:

                r = await resp.json()

                try:
                    data = r[0]['data']
                    if data is not None:
                        try:
                            parsed_data = []
                            for entry in data:
                                values = entry.split(',')
                                if values[-1] == 'NULL':
                                    values = values[:-1]
                                elif values[-1] == 'NULL':
                                    values = values[:-1]  # remove the last element if it's 'NULL'
                                parsed_data.append([float(value) if value != 'null' else 0.0 for value in values])
                            
                            sorted_data = sorted(parsed_data, key=lambda x: x[0], reverse=True)
                            
                            # Dynamically assign columns based on the length of the first entry
                            columns = ['Timestamp', 'Open', 'Close', 'High', 'Low', 'N', 'Volume', 'Vwap'][:len(sorted_data[0])]
                            
                            df = pd.DataFrame(sorted_data, columns=columns)
                            # Convert the Unix timestamps to datetime objects in UTC first
                            df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s', utc=True)

                            # Convert UTC to Eastern Time (ET)
                            df['Timestamp'] = df['Timestamp'].dt.tz_convert('US/Eastern')
                            df['Timestamp'] = df['Timestamp'].dt.tz_localize(None)
                            df['Ticker'] = ticker
                            df['timespan'] = interval

                            return df
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)


    async def get_stock_quote(self, ticker):
        ticker = ticker.split(',')
        tickerid = [self.get_webull_id(i) for i in ticker]
        tickerid = await asyncio.gather(*tickerid)
        tickerid = ','.join(map(str, tickerid))  # Convert to comma-separated string
        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={tickerid}&includeSecu=1&delay=0&more=1")
            data = data.json()

            data = MultiQuote(data)
            return data

    async def price_target(self, ticker):
        """Get price target"""
        try:
            tickerid = self.ticker_to_id_map.get(ticker)

            if not tickerid:
                raise ValueError(f"Ticker {ticker} not found in ticker_to_id_map.")

            url = f"https://quotes-gw.webullfintech.com/api/information/securities/analysis?tickerId={tickerid}"

            async with httpx.AsyncClient() as response:
                    data = await response.get(url)
                    data = data.json()

                    targetPrice = data.get('targetPrice', {})
                    if not targetPrice:
                        raise ValueError("Target price data is missing or invalid.")

                    data_dict = {
                        'target_price': targetPrice.get('current'),
                        'current_price': targetPrice.get('current'),
                        'start_date': targetPrice.get('effectiveStartDate'),
                        'high_target': targetPrice.get('high'),
                        'low_target': targetPrice.get('low'),
                        'avg_target': targetPrice.get('mean')
                    }

                    dataframe = pd.DataFrame([data_dict])  # Create DataFrame from a single dictionary
                    return dataframe

        except Exception as e:
            print(f"Error in price_target: {e}")
            return None
        

    