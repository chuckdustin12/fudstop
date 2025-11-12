import requests
import pandas as pd
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions()
import asyncio
from bs4 import BeautifulSoup



# Split the list into chunks of 55
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]
async def screener(headers=None,
    market_value_gte=None, market_value_lte=None,
    roe_gte=None, roe_lte=None,
    change_ratio_gte=None, change_ratio_lte=None,
    last_price_gte=None, last_price_lte=None,
    volume_gte=None, volume_lte=None,
    pettm_gte=None, pettm_lte=None,
    eps_gte=None, eps_lte=None
):
    # Base payload structure
    payload = {
        "fetch": 200,
        "rules": {
            "wlas.screener.rule.region": "securities.region.name.6",
        },
        "sort": {"rule": "wlas.screener.rule.price", "desc": True},
        "attach": {"hkexPrivilege": False}
    }

    # Helper function to construct rule values
    def build_rule(gte, lte):
        parts = []
        if gte is not None:
            parts.append(f"gte={gte}")
        if lte is not None:
            parts.append(f"lte={lte}")
        return "&".join(parts)

    # Add rules dynamically
    if market_value_gte is not None or market_value_lte is not None:
        payload["rules"]["wlas.screener.rule.marketvalue"] = build_rule(market_value_gte, market_value_lte)
    if volume_gte is not None or volume_lte is not None:
        payload["rules"]["wlas.screener.rule.volume"] = build_rule(volume_gte, volume_lte)
    if last_price_gte is not None or last_price_lte is not None:
        payload["rules"]["wlas.screener.rule.lastPrice"] = build_rule(last_price_gte, last_price_lte)
    if change_ratio_gte is not None or change_ratio_lte is not None:
        payload["rules"]["wlas.screener.rule.changeRatio"] = build_rule(change_ratio_gte, change_ratio_lte)
    if roe_gte is not None or roe_lte is not None:
        payload["rules"]["wlas.screener.rule.roe"] = build_rule(roe_gte, roe_lte)
    if eps_gte is not None or eps_lte is not None:
        payload["rules"]["wlas.screener.rule.eps"] = build_rule(eps_gte, eps_lte)
    if pettm_gte is not None or pettm_lte is not None:
        payload["rules"]["wlas.screener.rule.peTTM"] = build_rule(pettm_gte, pettm_lte)

    # Remove empty rules
    payload["rules"] = {k: v for k, v in payload["rules"].items() if v}

    # Send request and parse response
    response = requests.post("https://quotes-gw.webullfintech.com/api/wlas/screener/ng/query", json=payload, headers=headers)
    r = response.json()

    # Get items from response
    items = r.get('items', [])
    ticker = [i.get('ticker') for i in items]
    symbol = [i.get('symbol') for i in ticker if i]

    # Return DataFrame
    return pd.DataFrame({'ticker': symbol})

def build_rule(gte=None, lte=None):
    """Helper function to dynamically build a rule."""
    if gte is not None and lte is not None:
        return f"gte={gte}&lte={lte}"
    elif gte is not None:
        return f"gte={gte}"
    elif lte is not None:
        return f"lte={lte}"
    return None

async def options_screener(
    headers=None,
    expire_date_gte=None, expire_date_lte=None,
    volume_gte=None, volume_lte=None,
    open_interest_gte=None, open_interest_lte=None,
    delta_gte=None, delta_lte=None,
    ticker_impl_vol_gte=None, ticker_impl_vol_lte=None,
    total_volume_gte=None, total_volume_lte=None,
    total_open_interest_gte=None, total_open_interest_lte=None,
    avg30_open_interest_gte=None, avg30_open_interest_lte=None,
    pulse_index_gte=None, pulse_index_lte=None,
    his_volatility_gte=None, his_volatility_lte=None,
    iv_percent_gte=None, iv_percent_lte=None,
    avg30_volume_gte=None, avg30_volume_lte=None,
    bid_gte=None, bid_lte=None,
    ask_gte=None, ask_lte=None,
    close_gte=None, close_lte=None,
    change_ratio_gte=None, change_ratio_lte=None,
    vega_gte=None, vega_lte=None,
    rho_gte=None, rho_lte=None,
    impl_vol_gte=None, impl_vol_lte=None,
    theta_gte=None, theta_lte=None,
    gamma_gte=None, gamma_lte=None,
    prob_itm_gte=None, prob_itm_lte=None,
    leverage_ratio_gte=None, leverage_ratio_lte=None
):
    # Construct the payload
    payload = {
        "filter": {},
        "page": {"fetchSize": 200}
    }

    # Add rules dynamically
    if expire_date_gte is not None or expire_date_lte is not None:
        rule = build_rule(expire_date_gte, expire_date_lte)
        if rule:
            payload["filter"]["options.screener.rule.expireDate"] = rule

    if volume_gte is not None or volume_lte is not None:
        rule = build_rule(volume_gte, volume_lte)
        if rule:
            payload["filter"]["options.screener.rule.volume"] = rule

    if open_interest_gte is not None or open_interest_lte is not None:
        rule = build_rule(open_interest_gte, open_interest_lte)
        if rule:
            payload["filter"]["options.screener.rule.openInterest"] = rule

    if delta_gte is not None or delta_lte is not None:
        rule = build_rule(delta_gte, delta_lte)
        if rule:
            payload["filter"]["options.screener.rule.delta"] = rule

    if ticker_impl_vol_gte is not None or ticker_impl_vol_lte is not None:
        rule = build_rule(ticker_impl_vol_gte, ticker_impl_vol_lte)
        if rule:
            payload["filter"]["options.screener.rule.tickerImplVol"] = rule

    if total_volume_gte is not None or total_volume_lte is not None:
        rule = build_rule(total_volume_gte, total_volume_lte)
        if rule:
            payload["filter"]["options.screener.rule.totalVolume"] = rule

    if total_open_interest_gte is not None or total_open_interest_lte is not None:
        rule = build_rule(total_open_interest_gte, total_open_interest_lte)
        if rule:
            payload["filter"]["options.screener.rule.totalOpenInterest"] = rule

    if avg30_open_interest_gte is not None or avg30_open_interest_lte is not None:
        rule = build_rule(avg30_open_interest_gte, avg30_open_interest_lte)
        if rule:
            payload["filter"]["options.screener.rule.avg30OpenInterest"] = rule

    if pulse_index_gte is not None or pulse_index_lte is not None:
        rule = build_rule(pulse_index_gte, pulse_index_lte)
        if rule:
            payload["filter"]["options.screener.rule.pulseIndex"] = rule

    if his_volatility_gte is not None or his_volatility_lte is not None:
        rule = build_rule(his_volatility_gte, his_volatility_lte)
        if rule:
            payload["filter"]["options.screener.rule.hisVolatility"] = rule

    if iv_percent_gte is not None or iv_percent_lte is not None:
        rule = build_rule(iv_percent_gte, iv_percent_lte)
        if rule:
            payload["filter"]["options.screener.rule.ivPercent"] = rule

    if avg30_volume_gte is not None or avg30_volume_lte is not None:
        rule = build_rule(avg30_volume_gte, avg30_volume_lte)
        if rule:
            payload["filter"]["options.screener.rule.avg30Volume"] = rule

    if bid_gte is not None or bid_lte is not None:
        rule = build_rule(bid_gte, bid_lte)
        if rule:
            payload["filter"]["options.screener.rule.bid"] = rule

    if ask_gte is not None or ask_lte is not None:
        rule = build_rule(ask_gte, ask_lte)
        if rule:
            payload["filter"]["options.screener.rule.ask"] = rule

    if close_gte is not None or close_lte is not None:
        rule = build_rule(close_gte, close_lte)
        if rule:
            payload["filter"]["options.screener.rule.close"] = rule

    if change_ratio_gte is not None or change_ratio_lte is not None:
        rule = build_rule(change_ratio_gte, change_ratio_lte)
        if rule:
            payload["filter"]["options.screener.rule.changeRatio"] = rule

    if vega_gte is not None or vega_lte is not None:
        rule = build_rule(vega_gte, vega_lte)
        if rule:
            payload["filter"]["options.screener.rule.vega"] = rule

    if rho_gte is not None or rho_lte is not None:
        rule = build_rule(rho_gte, rho_lte)
        if rule:
            payload["filter"]["options.screener.rule.rho"] = rule

    if impl_vol_gte is not None or impl_vol_lte is not None:
        rule = build_rule(impl_vol_gte, impl_vol_lte)
        if rule:
            payload["filter"]["options.screener.rule.implVol"] = rule

    if theta_gte is not None or theta_lte is not None:
        rule = build_rule(theta_gte, theta_lte)
        if rule:
            payload["filter"]["options.screener.rule.theta"] = rule

    if gamma_gte is not None or gamma_lte is not None:
        rule = build_rule(gamma_gte, gamma_lte)
        if rule:
            payload["filter"]["options.screener.rule.gamma"] = rule

    if prob_itm_gte is not None or prob_itm_lte is not None:
        rule = build_rule(prob_itm_gte, prob_itm_lte)
        if rule:
            payload["filter"]["options.screener.rule.probITM"] = rule

    if leverage_ratio_gte is not None or leverage_ratio_lte is not None:
        rule = build_rule(leverage_ratio_gte, leverage_ratio_lte)
        if rule:
            payload["filter"]["options.screener.rule.leverageRatio"] = rule

    # Remove empty rules
    payload["filter"] = {k: v for k, v in payload["filter"].items() if v}

    # Debug: Print the constructed payload
    print("Constructed Payload:", payload)

    # Send the request and handle errors
    try:
        response = requests.post("https://quotes-gw.webullfintech.com/api/wlas/option/screener/query", json=payload, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        r = response.json()

        datas = r['datas']
        derivative = [i.get('derivative') for i in datas]
        return pd.DataFrame(derivative)

    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return pd.DataFrame()

# Example usage
df = asyncio.run(options_screener(impl_vol_gte=0.45))
ticker_ids = df['tickerId'].to_list()
chunks = list(chunk_list(ticker_ids, 55))

# Fetch data for each chunk
responses = []
for chunk in chunks:
    # Convert integers in the chunk to strings
    chunk_str = ','.join(map(str, chunk))
    print(chunk_str)
    response = requests.get(f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={chunk_str}").json()
    responses.append(response)

    print(response)