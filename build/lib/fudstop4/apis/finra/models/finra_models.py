import pandas as pd



class TickerATS:
    def __init__(self, data):
        self.total_weekly_share_quantity = [i.get('totalWeeklyShareQuantity', None) for i in data]
        self.issue_symbol_identifier = [i.get('issueSymbolIdentifier', None) for i in data]
        self.issue_name = [i.get('issueName', None) for i in data]
        self.last_update_date = [i.get('lastUpdateDate', None) for i in data]
        self.last_reported_date = [i.get('lastReportedDate', None) for i in data]
        self.tier_description = [i.get('tierDescription', None) for i in data]
        self.initial_published_date = [i.get('initialPublishedDate', None) for i in data]
        self.tier_identifier = [i.get('tierIdentifier', None) for i in data]
        self.summary_start_date = [i.get('summaryStartDate', None) for i in data]
        self.total_notional_sum = [i.get('totalNotionalSum', None) for i in data]
        self.total_weekly_trade_count = [i.get('totalWeeklyTradeCount', None) for i in data]
        self.week_start_date = [i.get('weekStartDate', None) for i in data]
        self.mpid = [i.get('MPID', None) for i in data]
        self.firm_crd_number = [i.get('firmCRDNumber', None) for i in data]
        self.product_type_code = [i.get('productTypeCode', None) for i in data]
        self.market_participant_name = [i.get('marketParticipantName', None) for i in data]
        self.summary_type_code = [i.get('summaryTypeCode', None) for i in data]

        self.data_dict = {
            "total_weekly_share_quantity": self.total_weekly_share_quantity,
            "issue_symbol_identifier": self.issue_symbol_identifier,
            "issue_name": self.issue_name,
            "last_update_date": self.last_update_date,
            "last_reported_date": self.last_reported_date,
            "tier_description": self.tier_description,
            "initial_published_date": self.initial_published_date,
            "tier_identifier": self.tier_identifier,
            "summary_start_date": self.summary_start_date,
            "total_notional_sum": self.total_notional_sum,
            "total_weekly_trade_count": self.total_weekly_trade_count,
            "week_start_date": self.week_start_date,
            "mpid": self.mpid,
            "firm_crd_number": self.firm_crd_number,
            "product_type_code": self.product_type_code,
            "market_participant_name": self.market_participant_name,
            "summary_type_code": self.summary_type_code
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)


class NonTickerATS:
    def __init__(self, data):
        pass