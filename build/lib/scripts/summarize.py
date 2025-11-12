import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
import xml.etree.ElementTree as ET

opts = PolygonOptions()

import requests
async def fetch_json(session, url):
    """Fetch JSON data from the Federal Register API."""
    try:
        async with session.get(url, timeout=10) as response:
            return await response.json() if response.status == 200 else None
    except Exception as e:
        return {"error": str(e)}


async def fetch_xml_text(session, xml_url):
    """Fetch XML content and parse it into structured data."""
    try:
        async with session.get(xml_url, timeout=10) as response:
            if response.status == 200:
                xml_content = await response.text()
                return parse_sec_xml(xml_content)
            return {"error": f"Failed to fetch XML: {response.status}"}
    except Exception as e:
        return {"error": str(e)}

# Function to clean up Release No and File Number
def clean_text(text):
    if isinstance(text, str):
        text = text.replace("[", "").replace("]", "").strip()
        return text
    return text

# Function to flatten the Headers list into a single string
def flatten_headers(headers):
    if isinstance(headers, list):
        return ", ".join(headers)  # Convert list to a comma-separated string
    return headers
def parse_sec_xml(xml_string):
    """Parses SEC XML content and extracts relevant details."""
    root = ET.fromstring(xml_string)

    data = {
        "Agency": root.find(".//AGENCY").text if root.find(".//AGENCY") is not None else None,
        "Release No": root.find(".//DEPDOC").text if root.find(".//DEPDOC") is not None else None,
        "Subject": root.find(".//SUBJECT").text if root.find(".//SUBJECT") is not None else None,
        "Date": root.find(".//DATE").text if root.find(".//DATE") is not None else None,
        "Body": "\n".join([p.text for p in root.findall(".//P") if p.text]),  # Extract all paragraphs
        "Comments URL": root.find(".//E").text if root.find(".//E") is not None else None,
        "Secretary": root.find(".//NAME").text if root.find(".//NAME") is not None else None,
        "Title": root.find(".//TITLE").text if root.find(".//TITLE") is not None else None,
        "Billing Code": root.find(".//BILCOD").text if root.find(".//BILCOD") is not None else None,
        "File Number": root.find(".//DEPDOC").text.split(";")[-1].strip() if root.find(".//DEPDOC") is not None else None,
        "Headers": [header.text for header in root.findall(".//HD") if header.text],
    }

    return data

async def scrape_sec_filings():
    """Fetch SEC filings, retrieve XML content, and parse it."""
    await opts.connect()

    # Fetch document numbers from database
    query = "SELECT document_number FROM sec_filings"
    results = await opts.fetch(query)

    df = pd.DataFrame(results, columns=['document_number'])
    document_numbers = df['document_number'].to_list()

    sec_data = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for doc_num in document_numbers:
            url = f"https://www.federalregister.gov/api/v1/documents/{doc_num}.json"
            tasks.append(fetch_json(session, url))

        json_responses = await asyncio.gather(*tasks)

        # Fetch XML content from URLs
        xml_tasks = []
        for response in json_responses:
            full_text_url = response.get("full_text_xml_url")
            if full_text_url:
                xml_tasks.append(fetch_xml_text(session, full_text_url))

        xml_responses = await asyncio.gather(*xml_tasks)

        for xml_data in xml_responses:
            if "error" not in xml_data:
                sec_data.append(xml_data)

    # Convert to DataFrame and display results
    df_sec_parsed = pd.DataFrame(sec_data)
    # Load the dataframe (assuming df_sec_parsed contains the data)
    df_sec_parsed["Release No"] = df_sec_parsed["Release No"].apply(clean_text)
    df_sec_parsed["File Number"] = df_sec_parsed["File Number"].apply(clean_text)
    df_sec_parsed["Headers"] = df_sec_parsed["Headers"].apply(flatten_headers)

    # Clean text fields (remove excessive spaces, newlines)

    df_sec_parsed.columns = df_sec_parsed.columns.str.lower().str.replace(" ", "_")
    # Ensure text is confined to a single column (if multiple columns exist)


    df_sec_parsed.to_csv('test.csv', index=False)
    await opts.batch_upsert_dataframe(df_sec_parsed, table_name='summarized_filings', unique_columns=['release_no'])

asyncio.run(scrape_sec_filings())