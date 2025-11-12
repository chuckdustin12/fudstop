#!/usr/bin/env python3
from rss_feeds.rss_models import NasdaqRSS
import urllib.request
import xml.etree.ElementTree as ET

def _strip_ns(tag: str) -> str:
    # Turn "{uri}local" -> "local"
    return tag.split("}", 1)[1] if "}" in tag else tag

def _flatten_xml(elem: ET.Element, path: str = "", out: dict | None = None, counts: dict | None = None) -> dict:
    """
    Recursively flatten an XML element into a dict with dotted paths.
    - Attributes are stored as <path>@attr
    - Text content stored as <path>#text
    - Repeated children are indexed: e.g., item.category[0] ... item.category[1]
    """
    if out is None:
        out = {}
    if counts is None:
        counts = {}

    name = _strip_ns(elem.tag)
    cur = f"{path}.{name}" if path else name

    # attributes
    for k, v in elem.attrib.items():
        out[f"{cur}@{k}"] = v

    # text
    text = (elem.text or "").strip()
    if text:
        out[f"{cur}#text"] = text

    # children (index repeated names)
    for child in list(elem):
        child_name = _strip_ns(child.tag)
        base = f"{cur}.{child_name}"
        idx = counts.get(base, 0)
        counts[base] = idx + 1
        child_path = f"{base}[{idx}]"
        _flatten_xml(child, path=cur, out=out, counts=counts)  # keep hierarchical counting
        # NOTE: We also want the indexed path for clarity; re-run flatten for the child at the indexed path
        _flatten_xml(child, path=f"{cur}.{child_name}[{idx}]", out=out, counts=counts)

    return out

def extract_items_dynamic(xml_text: str):
    root = ET.fromstring(xml_text)
    items = root.findall(".//item") or root.findall(".//{*}entry")
    return [_flatten_xml(it) for it in items]






TOPIC_URL_MAP = {"earnings": "https://www.nasdaq.com/feed/rssoutbound?category=Earnings",
           "nasdaq_original": "https://www.nasdaq.com/feed/nasdaq-original/rss.xml",
           "dividends": "https://www.nasdaq.com/feed/rssoutbound?category=Dividends",
           "crypto": "https://www.nasdaq.com/feed/rssoutbound?category=Cryptocurrencies",
           "commodities": "https://www.nasdaq.com/feed/rssoutbound?category=Commodities",
           "efs": "https://www.nasdaq.com/feed/rssoutbound?category=ETFs",
           "ipos": "https://www.nasdaq.com/feed/rssoutbound?category=IPOs",
           "markets": "https://www.nasdaq.com/feed/rssoutbound?category=Markets",
           "options": "https://www.nasdaq.com/feed/rssoutbound?category=Options",
           "stocks": "https://www.nasdaq.com/feed/rssoutbound?category=Stocks",
           "ai": "https://www.nasdaq.com/feed/rssoutbound?category=Artificial+Intelligence",
           "blockchain": "https://www.nasdaq.com/feed/rssoutbound?category=Blockchain",
           "corporate_governance": "https://www.nasdaq.com/feed/rssoutbound?category=Corporate+Governance",
           "financial_advisors": "https://www.nasdaq.com/feed/rssoutbound?category=Financial+Advisors",
           "fintech": "https://www.nasdaq.com/feed/rssoutbound?category=FinTech",
           "innovation": "https://www.nasdaq.com/feed/rssoutbound?category=Innovation",
           "nasdaq": "https://www.nasdaq.com/feed/rssoutbound?category=Nasdaq",
           "technology": "https://www.nasdaq.com/feed/rssoutbound?category=Technology",
           "investing": "https://www.nasdaq.com/feed/rssoutbound?category=Investing",
           "retirement": "https://www.nasdaq.com/feed/rssoutbound?category=Retirement",
           "saving": "https://www.nasdaq.com/feed/rssoutbound?category=Saving%20Money",
           
           }


TICKER_URL_MAP = { 
    "AAPL": "https://www.nasdaq.com/feed/rssoutbound?symbol=aapl"
}





def fetch_xml(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "rss-to-df-simple/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")
    




async def get_rows(url):
    try:
        x = fetch_xml(url)
        # x = the raw XML you fetched
        root = ET.fromstring(x)

        # Find all <item> elements in the feed
        items = root.findall(".//item")

        rows = [
            { _strip_ns(child.tag): (child.text or "").strip() 
            for child in it }
            for it in items
        ]

        rows = NasdaqRSS(rows)

        return rows
    except Exception as e:
        print(e)
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
import asyncio
opts = PolygonOptions()
async def main():
    await opts.connect()
    try:
        for i in most_active_tickers:
            i = i.lower()
            x = await get_rows(url=f"https://www.nasdaq.com/feed/rssoutbound?symbol={i}")
            x.as_dataframe['ticker'] = i
            
            await opts.batch_upsert_dataframe(x.as_dataframe, table_name='nasdaq_rss', unique_columns=['ticker'])
    except Exception as e:
        print(e)
asyncio.run(main())