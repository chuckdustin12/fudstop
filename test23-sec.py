from fudstop4.apis.federal_register.fed_register_sdk import FedRegisterSDK



sdk = FedRegisterSDK()
import xml.etree.ElementTree as ET
import xmltodict
import pandas as pd
import asyncio
import re

def clean_text_for_db(text):
    """
    Cleans and formats extracted SEC filing text into a single block for database insertion.
    """
    try:
        # Remove excessive newlines and spaces
        cleaned_text = re.sub(r"\s+", " ", text).strip()

        # Fix spacing around punctuation
        cleaned_text = re.sub(r"(\.\s+)", ". ", cleaned_text)  # Ensure correct sentence spacing

        return cleaned_text
    except Exception as e:
        print(e)
def extract_text_from_sec_xml(xml_data):
    """
    Extracts and cleans only the text from parsed SEC XML.
    """
    extracted_text = []
    try:
        # Loop through all sections in the XML
        for section in xml_data:
            if isinstance(section, dict) and "#text" in section:
                raw_text = section["#text"]
                cleaned_text = raw_text.replace("\n", " ").strip()  # Remove newlines & extra spaces
                extracted_text.append(cleaned_text)

        # Join extracted text into a single formatted document
        final_text = "\n\n".join(extracted_text)

        return final_text
    except Exception as e:
        print(e)
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions()
import requests
async def main():

    await opts.connect()
    all = await sdk.all_sec_documents()

    await opts.batch_upsert_dataframe(all.as_dataframe, table_name='fed_register', unique_columns=['document_number'])




async def parse_document():
    headers ={"User-Agent": "Mozilla/5.0"}
    await opts.connect()

    query = f"""SELECT document_number from fed_register"""


    results = await opts.fetch(query) 


    nums = [i.get('document_number') for i in results]


    for num in nums:
        

        try:
            r = requests.get(f"https://www.federalregister.gov/api/v1/documents/{num}.json").json()

            xml_url = r.get('full_text_xml_url')

            # Fetch the XML file
            response = requests.get(xml_url, headers=headers)
            if response.status_code != 200:
                return {"error": f"Failed to download XML. Status: {response.status_code}"}

            # Convert XML to structured JSON using xmltodict
            xml_dict = xmltodict.parse(response.content)


            # Extract <NOTICE> section
            notice = xml_dict.get("NOTICE", {})

            PREAMB = notice.get('PREAMB')
            P = PREAMB.get('P')
            agency = PREAMB.get('AGENCY')
            subject = PREAMB.get('SUBJECT')
            date = PREAMB.get('DATE')


            text = extract_text_from_sec_xml(P)
            
            clean_text = clean_text_for_db(text)

            if clean_text is not None:
                dict = { 
                    'date': date,
                    'subject': subject,
                    'text': clean_text,
                }

                df = pd.DataFrame(dict, index=[0])

                await opts.batch_upsert_dataframe(df, table_name='register_parsed', unique_columns=['date', 'subject'])
        except Exception as e:
            print(e)

asyncio.run(parse_document())