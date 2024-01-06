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