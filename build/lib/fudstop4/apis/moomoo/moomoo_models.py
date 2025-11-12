import pandas as pd




class EarningsData:
    def __init__(self, cards):
    
        self.stockId = [i.get('stockId') for i in cards]
        self.stockId = [item for sublist in self.stockId for item in sublist]
        print(self.stockId)
        self.total = [i.get('total') for i in cards]
        self.total = [item for sublist in self.total for item in sublist]
        self.category = [i.get('category') for i in self.total]
        self.content = [i.get('content') for i in self.total]
        self.labels = [','.join(i.get('labels')) for i in cards]
        self.pageId = [i.get('pageId') for i in cards]
        self.qMark = [i.get('qMark') for i in cards]
        self.earningsQuarter = [i.get('earningsQuarter') for i in cards]
        self.reportTime = [i.get('reportTime') for i in cards]
        self.pubType = [i.get('pubType') for i in cards]
        self.entUid = [i.get('entUid') for i in cards]
        self.entName = [i.get('entName') for i in cards]
        self.entLogo = [i.get('entLogo') for i in cards]
        self.jumpUrl = [i.get('jumpUrl') for i in cards]
        self.mainStockId = [i.get('mainStockId') for i in cards]
        self.stockReportSummary = [i.get('stockReportSummary') for i in cards]



        self.data_dict = { 
            'labels': self.labels,
            'page_id': self.pageId,
            'q_mark': self.qMark,
            'earnings_qtr': self.earningsQuarter,
            'report_time': self.reportTime,
            'publish_type': self.pubType,
            'event_name': self.entName,
            'event_logo': self.entLogo,
            'url': self.jumpUrl,
            'main_stock_id': self.mainStockId,
            'content': self.content,
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)