import pandas as pd
class AvgInterestRates:
    def __init__(self, datas):
        self.record_date = [i.get('record_date') for i in datas]
        self.security_type_desc = [i.get('security_type_desc') for i in datas]
        self.security_desc = [i.get('security_desc') for i in datas]
        self.avg_interest_rate_amt = [i.get('avg_interest_rate_amt') for i in datas]
        self.src_line_nbr = [i.get('src_line_nbr') for i in datas]
        self.record_fiscal_year = [i.get('record_fiscal_year') for i in datas]
        self.record_fiscal_quarter = [i.get('record_fiscal_quarter') for i in datas]
        self.record_calendar_year = [i.get('record_calendar_year') for i in datas]
        self.record_calendar_quarter = [i.get('record_calendar_quarter') for i in datas]
        self.record_calendar_month = [i.get('record_calendar_month') for i in datas]
        self.record_calendar_day = [i.get('record_calendar_day') for i in datas]


        self.data_dict = { 

            'date': self.record_date,
            'security_type_description': self.security_type_desc,
            'security_description': self.security_desc,
            'avg_rate_amount': self.avg_interest_rate_amt,
            'source_line_number': self.src_line_nbr,
            'record_fiscal_year': self.record_fiscal_year,
            'record_fiscal_qtr': self.record_calendar_quarter,
            'record_calendar_year': self.record_calendar_year,
            'record_calendar_qtr': self.record_calendar_quarter,
            'record_calendar_month': self.record_calendar_month,
            'record_calendar_day': self.record_calendar_day
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class RecordSettingAuctions:
    def __init__(self, data):
        self.record_date = [i.get('record_date') for i in data]
        self.security_type = [i.get('security_type') for i in data]
        self.security_term = [i.get('security_term') for i in data]
        self.first_auc_date_single_price = [i.get('first_auc_date_single_price') for i in data]
        self.low_rate_pct = [i.get('low_rate_pct') for i in data]
        self.first_auc_date_low_rate = [i.get('first_auc_date_low_rate') for i in data]
        self.high_rate_pct = [i.get('high_rate_pct') for i in data]
        self.first_auc_date_high_rate = [i.get('first_auc_date_high_rate') for i in data]
        self.high_offer_amt = [i.get('high_offer_amt') for i in data]
        self.first_auc_date_high_offer = [i.get('first_auc_date_high_offer') for i in data]
        self.high_bid_cover_ratio = [i.get('high_bid_cover_ratio') for i in data]
        self.first_auc_date_high_bid_cover = [i.get('first_auc_date_high_bid_cover') for i in data]
        self.src_line_nbr = [i.get('src_line_nbr') for i in data]
        self.record_fiscal_year = [i.get('record_fiscal_year') for i in data]
        self.record_fiscal_quarter = [i.get('record_fiscal_quarter') for i in data]
        self.record_calendar_year = [i.get('record_calendar_year') for i in data]
        self.record_calendar_quarter = [i.get('record_calendar_quarter') for i in data]
        self.record_calendar_month = [i.get('record_calendar_month') for i in data]
        self.record_calendar_day = [i.get('record_calendar_day') for i in data]


        self.data_dict = {
            "record_date": self.record_date,
            "security_type": self.security_type,
            "security_term": self.security_term,
            "first_auc_date_single_price": self.first_auc_date_single_price,
            "low_rate_pct": self.low_rate_pct,
            "first_auc_date_low_rate": self.first_auc_date_low_rate,
            "high_rate_pct": self.high_rate_pct,
            "first_auc_date_high_rate": self.first_auc_date_high_rate,
            "high_offer_amt": self.high_offer_amt,
            "first_auc_date_high_offer": self.first_auc_date_high_offer,
            "high_bid_cover_ratio": self.high_bid_cover_ratio,
            "first_auc_date_high_bid_cover": self.first_auc_date_high_bid_cover,
            "src_line_nbr": self.src_line_nbr,
            "record_fiscal_year": self.record_fiscal_year,
            "record_fiscal_quarter": self.record_fiscal_quarter,
            "record_calendar_year": self.record_calendar_year,
            "record_calendar_quarter": self.record_calendar_quarter,
            "record_calendar_month": self.record_calendar_month,
            "record_calendar_day": self.record_calendar_day
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class DebtToPenny:
    def __init__(self, data):
        self.record_date = [i.get('record_date') for i in data]
        self.debt_held_public_amt = [i.get('debt_held_public_amt') for i in data]
        self.intragov_hold_amt = [i.get('intragov_hold_amt') for i in data]
        self.tot_pub_debt_out_amt = [i.get('tot_pub_debt_out_amt') for i in data]
        self.src_line_nbr = [i.get('src_line_nbr') for i in data]
        self.record_fiscal_year = [i.get('record_fiscal_year') for i in data]
        self.record_fiscal_quarter = [i.get('record_fiscal_quarter') for i in data]
        self.record_calendar_year = [i.get('record_calendar_year') for i in data]
        self.record_calendar_quarter = [i.get('record_calendar_quarter') for i in data]
        self.record_calendar_month = [i.get('record_calendar_month') for i in data]
        self.record_calendar_day = [i.get('record_calendar_day') for i in data]
        self.data_dict = {
            "record_date": self.record_date,
            "debt_held_public_amt": self.debt_held_public_amt,
            "intragov_hold_amt": self.intragov_hold_amt,
            "tot_pub_debt_out_amt": self.tot_pub_debt_out_amt,
            "src_line_nbr": self.src_line_nbr,
            "record_fiscal_year": self.record_fiscal_year,
            "record_fiscal_quarter": self.record_fiscal_quarter,
            "record_calendar_year": self.record_calendar_year,
            "record_calendar_quarter": self.record_calendar_quarter,
            "record_calendar_month": self.record_calendar_month,
            "record_calendar_day": self.record_calendar_day
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)



class FRN:
    def __init__(self, data):

        self.record_date = [i.get('record_date') for i in data]
        self.frn = [i.get('frn') for i in data]
        self.cusip = [i.get('cusip') for i in data]
        self.original_dated_date = [i.get('original_dated_date') for i in data]
        self.original_issue_date = [i.get('original_issue_date') for i in data]
        self.maturity_date = [i.get('maturity_date') for i in data]
        self.spread = [i.get('spread') for i in data]
        self.start_of_accrual_period = [i.get('start_of_accrual_period') for i in data]
        self.end_of_accrual_period = [i.get('end_of_accrual_period') for i in data]
        self.daily_index = [i.get('daily_index') for i in data]
        self.daily_int_accrual_rate = [i.get('daily_int_accrual_rate') for i in data]
        self.daily_accrued_int_per100 = [i.get('daily_accrued_int_per100') for i in data]
        self.accr_int_per100_pmt_period = [i.get('accr_int_per100_pmt_period') for i in data]

        self.data_dict = {
            "record_date": self.record_date,
            "frn": self.frn,
            "cusip": self.cusip,
            "original_dated_date": self.original_dated_date,
            "original_issue_date": self.original_issue_date,
            "maturity_date": self.maturity_date,
            "spread": self.spread,
            "start_of_accrual_period": self.start_of_accrual_period,
            "end_of_accrual_period": self.end_of_accrual_period,
            "daily_index": self.daily_index,
            "daily_int_accrual_rate": self.daily_int_accrual_rate,
            "daily_accrued_int_per100": self.daily_accrued_int_per100,
            "accr_int_per100_pmt_period": self.accr_int_per100_pmt_period
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)



class UpcomingAuctions:
    def __init__(self, data):
        self.record_date = [i.get('record_date') for i in data]
        self.security_type = [i.get('security_type') for i in data]
        self.security_term = [i.get('security_term') for i in data]
        self.reopening = [i.get('reopening') for i in data]
        self.cusip = [i.get('cusip') for i in data]
        self.offering_amt = [i.get('offering_amt') for i in data]
        self.announcemt_date = [i.get('announcemt_date') for i in data]
        self.auction_date = [i.get('auction_date') for i in data]
        self.issue_date = [i.get('issue_date') for i in data]


        self.data_dict = {
            "record_date": self.record_date,
            "security_type": self.security_type,
            "security_term": self.security_term,
            "reopening": self.reopening,
            "cusip": self.cusip,
            "offering_amt": self.offering_amt,
            "announcemt_date": self.announcemt_date,
            "auction_date": self.auction_date,
            "issue_date": self.issue_date
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)


class TreasuryGold:
    def __init__(self, data):
        self.record_date = [i.get('record_date') for i in data]
        self.facility_desc = [i.get('facility_desc') for i in data]
        self.form_desc = [i.get('form_desc') for i in data]
        self.location_desc = [i.get('location_desc') for i in data]
        self.fine_troy_ounce_qty = [i.get('fine_troy_ounce_qty') for i in data]
        self.book_value_amt = [i.get('book_value_amt') for i in data]
        self.src_line_nbr = [i.get('src_line_nbr') for i in data]
        self.record_fiscal_year = [i.get('record_fiscal_year') for i in data]
        self.record_fiscal_quarter = [i.get('record_fiscal_quarter') for i in data]
        self.record_calendar_year = [i.get('record_calendar_year') for i in data]
        self.record_calendar_quarter = [i.get('record_calendar_quarter') for i in data]
        self.record_calendar_month = [i.get('record_calendar_month') for i in data]
        self.record_calendar_day = [i.get('record_calendar_day') for i in data]

        self.data_dict = {
            "record_date": self.record_date,
            "facility_desc": self.facility_desc,
            "form_desc": self.form_desc,
            "location_desc": self.location_desc,
            "fine_troy_ounce_qty": self.fine_troy_ounce_qty,
            "book_value_amt": self.book_value_amt,
            "src_line_nbr": self.src_line_nbr,
            "record_fiscal_year": self.record_fiscal_year,
            "record_fiscal_quarter": self.record_fiscal_quarter,
            "record_calendar_year": self.record_calendar_year,
            "record_calendar_quarter": self.record_calendar_quarter,
            "record_calendar_month": self.record_calendar_month,
            "record_calendar_day": self.record_calendar_day
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)