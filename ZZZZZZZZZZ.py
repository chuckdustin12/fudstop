#!/usr/bin/env python3
"""
Simple: Parse an RSS feed to a pandas DataFrame.

Usage (default URL is the Nasdaq Earnings feed):
  python rss_to_df_simple.py
  python rss_to_df_simple.py --url https://www.nasdaq.com/feed/rssoutbound?category=Earnings
  python rss_to_df_simple.py --csv earnings.csv
"""

import argparse
import html
import sys
import urllib.request
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

import pandas as pd


DEFAULT_URL = "https://www.nasdaq.com/feed/rssoutbound?category=Earnings"
DEFAULT_URL2="https://www.nasdaq.com/feed/nasdaq-original/rss.xml"


def fetch_xml(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "rss-to-df-simple/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _find_text(node: ET.Element, names) -> str | None:
    # look at direct children first
    for child in node:
        if _strip_ns(child.tag) in names:
            return (child.text or "").strip()
    # then any descendants (namespace-agnostic)
    for name in names:
        el = node.find(f".//{{*}}{name}")
        if el is not None and el.text:
            return el.text.strip()
    return None


def parse_rss(xml_text: str) -> pd.DataFrame:
    root = ET.fromstring(xml_text)
    items = root.findall(".//item") or root.findall(".//{*}entry")

    rows = []

    for it in items:
        row = {}  # every attribute will be added here

        # ----- SIMPLE TAG EXTRACTION -----
        row["title"] = _find_text(it, ["title"]) or ""
        row["link"] = _find_text(it, ["link"]) or ""
        row["guid"] = _find_text(it, ["guid", "id"]) or ""

        desc = _find_text(it, ["description", "summary", "content"]) or ""
        row["summary"] = " ".join(html.unescape(desc).split())

        pub_raw = _find_text(it, ["pubDate", "published", "updated"]) or ""
        try:
            row["published"] = pd.to_datetime(parsedate_to_datetime(pub_raw), utc=True)
        except Exception:
            row["published"] = pd.NaT

        # ----- CATEGORIES -----
        cats = []
        for c in it.findall(".//{*}category"):
            if c.text:
                cats.extend([x.strip() for x in c.text.split(",") if x.strip()])
        row["categories"] = cats

        # ----- TICKERS -----
        tick_el = it.find(".//{*}tickers")
        row["tickers"] = []
        if tick_el is not None and tick_el.text:
            row["tickers"] = [t.strip() for t in tick_el.text.split(",") if t.strip()]

        # ----- NEW PART: CAPTURE ALL XML ATTRIBUTES -----
        # For every element inside <item>, collect attributes
        attr_counter = {}  # handles duplicate tag names

        for child in it.iter():
            tag_name = _strip_ns(child.tag)

            # Track counts (some tags like <partnerlink> appear multiple times)
            attr_counter.setdefault(tag_name, 0)
            attr_count = attr_counter[tag_name]
            attr_counter[tag_name] += 1

            # Save attributes
            for attr_key, attr_val in child.attrib.items():
                # Naming format:
                #   <tagname>_<n>_<attr>
                # Example:
                #   partnerlink_0_url = "https://example.com"
                colname = f"{tag_name}_{attr_count}_{attr_key}"
                row[colname] = attr_val

        rows.append(row)

    df = pd.DataFrame(rows)

    print(df)
    if not df.empty:
        df = df.sort_values("published", ascending=False, na_position="last").reset_index(drop=True)

    return df

def main():
    ap = argparse.ArgumentParser(description="Convert an RSS/Atom feed to a pandas DataFrame.")
    ap.add_argument("--url", default=DEFAULT_URL2, help="Feed URL (defaults to Nasdaq Earnings feed).")
    ap.add_argument("--csv", help="Optional path to write CSV output.")
    args = ap.parse_args()

    try:
        xml_text = fetch_xml(args.url)
    except Exception as e:
        print(f"Failed to fetch feed: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        df = parse_rss(xml_text)
    except Exception as e:
        print(f"Failed to parse feed XML: {e}", file=sys.stderr)
        sys.exit(1)

    # Print a compact view
    with pd.option_context("display.max_colwidth", 120, "display.width", 160):
        cols = [c for c in ["published", "title", "link", "categories", "tickers"] if c in df.columns]
        print(df[cols].to_string(index=False) if not df.empty else "No items found.")

    if args.csv and not df.empty:
        df.to_csv(args.csv, index=False)
        print(f"\nSaved CSV → {args.csv}", file=sys.stderr)


if __name__ == "__main__":
    main()
