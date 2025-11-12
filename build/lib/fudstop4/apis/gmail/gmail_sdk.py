import imaplib
import os
import email
import csv
import json
from bs4 import BeautifulSoup

# --- Configuration ---
EMAIL_ADDRESS = "chuckdustin12@gmail.com"
APP_PASSWORD = "qssq abzp bcqx kuhu"
SENDER_EMAIL = "no-reply@efilingmail.tylertech.cloud"
SUBJECT_KEYWORD = "Filing Accepted"

# --- Helper Functions ---

def extract_table_by_header(soup, header_text):
    """
    Finds and returns the first table that has a <th> containing header_text.
    """
    for table in soup.find_all("table"):
        th = table.find("th")
        if th and header_text in th.get_text():
            return table
    return None

def parse_table(table, is_document=False):
    """
    Parses a table and returns a dictionary mapping label to value.
    For document tables, if the label is 'File Stamped Copy', it extracts the link from the <a> tag.
    """
    data = {}
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 2:
            label = cells[0].get_text(separator=" ", strip=True)
            if is_document and "File Stamped Copy" in label:
                # For File Stamped Copy, extract the href from the anchor tag
                a = cells[1].find("a")
                if a and a.has_attr("href"):
                    value = a["href"].strip()
                else:
                    value = cells[1].get_text(separator=" ", strip=True)
            else:
                value = cells[1].get_text(separator=" ", strip=True)
            data[label] = value
    return data

def extract_email_data(msg):
    """
    Extracts clean filing data from an email message.
    It first obtains the HTML body, then uses BeautifulSoup to locate the 'Filing Details'
    and 'Document Details' tables and parses them.
    """
    # Get Envelope Number from subject if available
    envelope = "Unknown"
    subject = msg.get("Subject", "")
    if "Envelope Number:" in subject:
        try:
            envelope = subject.split("Envelope Number:")[1].strip()
        except Exception:
            envelope = "Unknown"
    
    # --- Extract HTML body ---
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disp = str(part.get("Content-Disposition"))
            if content_type == "text/html" and "attachment" not in content_disp:
                body = part.get_payload(decode=True).decode(errors="ignore")
                break  # Use first HTML part found
    else:
        body = msg.get_payload(decode=True).decode(errors="ignore")
    
    soup = BeautifulSoup(body, "html.parser")
    
    # --- Extract tables by header ---
    filing_table = extract_table_by_header(soup, "Filing Details")
    document_table = extract_table_by_header(soup, "Document Details")
    
    filing_data = {}
    document_data = {}
    if filing_table:
        filing_data = parse_table(filing_table, is_document=False)
    if document_table:
        document_data = parse_table(document_table, is_document=True)
    
    # --- Build final output dictionary using known label names ---
    final_data = {
        "Envelope Number": envelope,
        "Case Number": filing_data.get("Case Number", "Unknown"),
        "Case Style": filing_data.get("Case Style", "Unknown"),
        "Date Submitted": filing_data.get("Date/Time Submitted", "Unknown"),
        "Date Accepted": filing_data.get("Date/Time Accepted", "Unknown"),
        "Filing Type": filing_data.get("Filing Type", "Unknown"),
        "Filing Description": filing_data.get("Filing Description", "Unknown"),
        "Activity Requested": filing_data.get("Activity Requested", "Unknown"),
        "Filed By": filing_data.get("Filed By", "Unknown"),
        "Filing Attorney": filing_data.get("Filing Attorney", ""),
        "Lead Document": document_data.get("Lead Document", "Unknown"),
        "Lead Document Page Count": document_data.get("Lead Document Page Count", "Unknown"),
        "File Stamped Copy": document_data.get("File Stamped Copy", "Unknown")
    }
    return final_data

# --- Main Script ---

# Connect to Gmail IMAP server
mail = imaplib.IMAP4_SSL("imap.gmail.com")
mail.login(EMAIL_ADDRESS, APP_PASSWORD)
mail.select("inbox")

# Search for emails matching the sender and subject criteria
status, email_ids = mail.search(None, f'FROM "{SENDER_EMAIL}"', f'SUBJECT "{SUBJECT_KEYWORD}"')
all_data = []

if status == "OK":
    email_ids = email_ids[0].split()
    if not email_ids:
        print(f"No emails found from {SENDER_EMAIL} with subject '{SUBJECT_KEYWORD}'")
    else:
        print(f"Found {len(email_ids)} emails. Extracting clean data...")
        for num in email_ids:
            status, data = mail.fetch(num, "(RFC822)")
            if status != "OK":
                continue
            msg = email.message_from_bytes(data[0][1])
            try:
                email_data = extract_email_data(msg)
                all_data.append(email_data)
                # Optional: Print each email's extracted data
                print("Extracted data for one email:")
                for k, v in email_data.items():
                    print(f"{k}: {v}")
                print("-" * 40)
            except Exception as e:
                print(f"Error processing email {num}: {e}")

        # --- Save results ---
        output_folder = "extracted_filing_data"
        os.makedirs(output_folder, exist_ok=True)
        
        # Write CSV output
        csv_filename = os.path.join(output_folder, "filing_accepted_emails_clean.csv")
        with open(csv_filename, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=all_data[0].keys())
            writer.writeheader()
            writer.writerows(all_data)
        print(f"Clean data saved to CSV: {csv_filename}")
        
        # Write JSON output
        json_filename = os.path.join(output_folder, "filing_accepted_emails_clean.json")
        with open(json_filename, "w", encoding="utf-8") as json_file:
            json.dump(all_data, json_file, indent=4)
        print(f"Clean data saved to JSON: {json_filename}")
else:
    print("Failed to search emails.")

mail.logout()
