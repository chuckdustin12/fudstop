from dataclasses import dataclass, asdict
from typing import Optional
import pandas as pd

class CompanyResults:
    def __init__(self, results):

        self.ticker = results.get('ticker')
        self.name = results.get('name')
        self.market = results.get('market')
        self.locale = results.get('locale')
        self.primary_exchange = results.get('primary_exchange')
        self.type = results.get('type')
        self.active = results.get('active')
        self.currency_name = results.get('currency_name')
        self.cik = results.get('cik')
        self.composite_figi = results.get('composite_figi')
        self.share_class_figi = results.get('share_class_figi')
        self.market_cap = results.get('market_cap')
        self.phone_number = results.get('phone_number')
        address = results.get('address')
        self.address = address.get('address1')
        self.city = address.get('city')
        self.zip = address.get('postal_code')
        self.state = address.get('state')
        self.description = results.get('description')
        self.sic_code = results.get('sic_code')
        self.sic_description = results.get('sic_description')
        self.ticker_root = results.get('ticker_root')
        self.homepage_url = results.get('homepage_url')
        self.total_employees = results.get('total_employees')
        self.list_date = results.get('list_date')
        branding = results.get('branding')
        self.icon_url = branding.get('icon_url')
        self.logo_url = branding.get('logo_url')
        self.share_class_shares_outstanding = results.get('share_class_shares_outstanding')
        self.weighted_shares_outstanding = results.get('weighted_shares_outstanding')
        self.round_lot = results.get('round_lot')


        self.data_dict = { 
            'ticker': self.ticker,
            'name': self.name,
            'market': self.market,
            'locale': self.locale,
            'primary_exchange': self.primary_exchange,
            'type': self.type,
            'active': self.active,
            'currency': self.currency_name,
            'cik': self.cik,
            'composite_figi': self.composite_figi,
            'share_class_figi': self.share_class_figi,
            'market_cap': self.market_cap,
            'phone': self.phone_number,
            'address': self.address,
            'city': self.city,
            'state': self.state,
            'zip': self.zip,
            'description': self.description,
            'sic_code': self.sic_code,
            'sic_description': self.sic_description,
            'ticker_root': self.ticker_root,
            'homepage_url': self.homepage_url,
            'total_employees': self.total_employees,
            'list_date': self.list_date,
            'icon_url': self.icon_url,
            'logo_url': self.logo_url,
            'shares_outstanding': self.share_class_shares_outstanding,
            'weighted_shares_outstanding': self.weighted_shares_outstanding,
            'round_lot': self.round_lot
        }


        self.as_dataframe = pd.DataFrame(self.data_dict, index=[0])