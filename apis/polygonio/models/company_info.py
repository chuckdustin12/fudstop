from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class CombinedCompanyResults:
    ticker: Optional[str] = None
    name: Optional[str] = None
    market: Optional[str] = None
    locale: Optional[str] = None
    primary_exchange: Optional[str] = None
    type: Optional[str] = None
    active: Optional[bool] = None
    currency_name: Optional[str] = None
    cik: Optional[str] = None
    composite_figi: Optional[str] = None
    share_class_figi: Optional[str] = None
    market_cap: Optional[int] = None
    phone_number: Optional[str] = None
    description: Optional[str] = None
    sic_code: Optional[str] = None
    sic_description: Optional[str] = None
    ticker_root: Optional[str] = None
    homepage_url: Optional[str] = None
    total_employees: Optional[int] = None
    list_date: Optional[str] = None
    share_class_shares_outstanding: Optional[int] = None
    weighted_shares_outstanding: Optional[int] = None
    round_lot: Optional[int] = None
    
    # Address attributes
    address1: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    
    # Branding attributes
    logo_url: Optional[str] = None
    icon_url: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)