import re
import requests
from lxml import etree

from xml.etree import ElementTree as ET

import pandas as pd

def fix_xml_attributes(xml_content):
    # Find attributes without values and add a default value
    fixed_xml = re.sub(r'(\w+)=""', r'\1="default"', xml_content)
    return fixed_xml


def xml_to_dataframe(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    data = extract_xml_data(root)
    df = pd.DataFrame([data])
    return df


def extract_xml_data(element, parent_name=''):
    data = {}
    for child in element:
        name = child.tag
        if child.text and child.text.strip():
            data[name] = child.text.strip()
        data.update(extract_xml_data(child, name))
    return data
