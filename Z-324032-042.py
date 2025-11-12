import requests
import pdfkit
import pandas as pd
import os
import aiohttp
import urllib.parse
import re
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions()
# CONFIGURE wkhtmltopdf path
path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
def clean_snippet(snippet: str) -> str:
    """
    Removes <sep /> markers and <hi>...</hi> tags from the given snippet,
    preserving the text inside <hi> tags.
    
    :param snippet: The raw snippet text containing <sep/> and <hi> tags.
    :return: A cleaned-up string suitable for database insertion.
    """
    # 1. Remove <sep /> markers entirely:
    snippet_no_sep = snippet.replace("<sep />", "")
    
    # 2. Remove <hi> tags but keep the text inside them:
    #    We can use a regex that finds <hi> and </hi> pairs and removes them.
    #    The pattern below replaces <hi>...</hi> with the content inside.
    snippet_cleaned = re.sub(r"</?hi>", "", snippet_no_sep)
    
    # Optionally, you might strip extraneous whitespace:
    snippet_cleaned = snippet_cleaned.strip()
    
    return snippet_cleaned

base_url=f"https://app.vlex.com"
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36', 'x-csrf-token': 'fT3s64Svo0V2cseIoT6Rty49TqIhddYuZMtVrDdStcg=', 'x-root-account-email': 'chuckdustin12@gmail.com', 'x-user-email': 'chuckdustin12@gmail.com', 'x-webapp-seed': '8113867', 'traceparent': '00-7ca0d0a1c6cf4c7d92cbde37b0075e2b-4db447070577450b-01', 'cookie': '_ga=GA1.1.2032994151.1742669284; _fbp=fb.1.1742669284512.774563573359695035; hubspotutk=750bec2eb354f317c2c2b7f23a410836; __hssrc=1; keymachine=M483Y8WMLXU9; LT=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NDUxNzYwMjg4ODAsImxvZ2luX21ldGhvZCI6InNpZ251cCIsInZsZXhfdXNlcl9pZCI6MTE2MjYxMzQ3NSwiaXNfYWNjb3VudF9tYW5hZ2VyIjpmYWxzZSwiYWN0aXZlX3Byb2R1Y3RzIjpbIlVOSVZFUlNBTCJdLCJlbWFpbCI6ImNodWNrZHVzdGluMTJAZ21haWwuY29tIiwic2l0ZV9kb21haW4iOiJhcHAudmxleC5jb20iLCJpbmhlcml0ZWRfYWNjb3VudHMiOlsxMTYyNjEzNDc1XX0.JJlxa4NE9mBXcKwEZKsCKzbAidGcKt4-c_tkkW2bcNE; ALT=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VySWQiOjExNjI2MTM0NzV9.TRmIacHkxWrF8pjrBS3wCRPkO4TAoIite-4c4UgLdVs; _hjSessionUser_3307954=eyJpZCI6ImQyMWMxMWM2LWQ1ZDQtNWVlYi04NmFhLWQ1NmEyNWI2MDUyMSIsImNyZWF0ZWQiOjE3NDUxNzYwMzE5NjEsImV4aXN0aW5nIjp0cnVlfQ==; vlex-session=BAh7HEkiD3Nlc3Npb25faWQGOgZFRiIlYjg4OTFhZGI3OTQ1MDNlMzkwOTBhNjNkMWQwMTkyYzJJIhpoYXNfQ0NMQV9pbml0aWFsX2dpZnQGOwBGRkkiD3V0bV9tZWRpdW0GOwBGSSIJc2VycAY7AFRJIg91dG1fc291cmNlBjsARkkiD2dvb2dsZS5jb20GOwBUSSIRbGFuZGluZ19wYWdlBjsARkkiVWh0dHBzOi8vY2FzZS1sYXcudmxleC5jb20vdmlkL2luLXJlLWJsYWtlbmV5LW5vLTg4ODg2NjA1MD91dG1fc291cmNlPWNoYXRncHQuY29tBjsAVEkiDHVzdWFyaW8GOwBGbCsH4xJMRUkiDmlkaW9tYV9pZAY7AEZJIgdFTgY7AFRJIhRtb2JpbGVfdmVyc2lvbj8GOwBGRkkiDHBhaXNfaWQGOwBGSSIHVVMGOwBUSSIUY29udGVudF9wYWlzX2lkBjsARkAVSSIRdXNlcl9wYWlzX2lkBjsARkAVSSIMZG9taW5pbwY7AEYiDXZsZXguY29tSSIKbG9naW4GOwBGSSITcmVtZW1iZXJfbWVfTFQGOwBUSSITbGF0ZXN0X3Nlc3Npb24GOwBGbzoQTG9nU2VzaW9uZXMNOhBAYXR0cmlidXRlc3sWSSIHaWQGOwBUSSIOMzI5MjUzNTMyBjsAVEkiD3VzdWFyaW9faWQGOwBUSSIPMTE2MjYxMzQ3NQY7AFRJIhBrZXlfbWFxdWluYQY7AFRJIhFNNDgzWThXTUxYVTkGOwBUSSIPc3RfZG9taW5pbwY7AFRJIg12bGV4LmNvbQY7AFRJIhR0X2luaWNpb19zZXNpb24GOwBUSSIfMjAyNS0wNC0yMCAxOTowNzoxMC40NjUzMjEGOwBUSSIUdF91bHRpbWFfYWNjaW9uBjsAVEkiHzIwMjUtMDQtMjAgMTk6MDc6MTAuNDY1MzIyBjsAVEkiE2JfZGVzY29uZWN0YWRvBjsAVEkiBmYGOwBUSSIWY3RfcGFnaW5hc192aXN0YXMGOwBUSSIGMAY7AFRJIg9zdF9jb2RhcmVhBjsAVEkiDlVOSVZFUlNBTAY7AFRJIg5pcF9yZW1vdGEGOwBUSSITNTQuMTY0LjE4OS4xMzYGOwBUSSIWdXN1YXJpb19hZ2VudGVfaWQGOwBUMEkiIXVzdWFyaW9zeGFyZWFzcHJlbWl1bV9iX2RlbW8GOwBUSSIGZgY7AFRJIhBrZXlfcGVyc29uYQY7AFRJIhNyZW1lbWJlcl9tZV9MVAY7AFRJIhFjdF9kZXNjYXJnYXMGOwBUSSIGMAY7AFRJIhRyb290X3VzdWFyaW9faWQGOwBUSSIPMTE2MjYxMzQ3NQY7AFRJIg1iX2hpZGRlbgY7AFQwSSINcGxhdGZvcm0GOwBUMDoYQGNoYW5nZWRfYXR0cmlidXRlc3sAOhhAcHJldmlvdXNseV9jaGFuZ2VkewA6FkBhdHRyaWJ1dGVzX2NhY2hlewZJIhR0X3VsdGltYV9hY2Npb24GOwBGVTogQWN0aXZlU3VwcG9ydDo6VGltZVdpdGhab25lWwhJdToJVGltZSMyMDI1LTA0LTIwVDE5OjA3OjEwLjQ2NTMyMjAwMFoGOwBGSSIIVVRDBjsARkBEOhxAbWFya2VkX2Zvcl9kZXN0cnVjdGlvbkY6D0BkZXN0cm95ZWRGOg5AcmVhZG9ubHlGOhBAbmV3X3JlY29yZEZJIiFkYXlzX3NpbmNlX3ByZXZpb3VzX2FjdGl2aXR5BjsARkkiEmZpcnN0X3Nlc3Npb24GOwBUSSIHaWQGOwBGaQScAqATSSIPY2x1c3Rlcl9pZAY7AEZp%2BkkiEWFsZ29yaXRtb19pZAY7AEZpBkkiEXNlc3Npb25fdHlwZQY7AEZAG0kiE3NraXBfYXV0b2xvZ2luBjsARkZJIhJ3ZWJhcHBfbG9jYWxlBjsARkkiB2VuBjsAVEkiEF9jc3JmX3Rva2VuBjsARkkiMWZUM3M2NFN2bzBWMmNzZUlvVDZSdHk0OVRxSWhkZFl1Wk10VnJEZFN0Y2c9BjsARkkiEHVybF9yZWZlcmVyBjsARiIfaHR0cHM6Ly9jYXNlLWxhdy52bGV4LmNvbS8%3D--cc9d9cabe0a1cab83581cd2bfab2939496b4b8ae; _ga_VZEM72P2C6=GS1.1.1745332097.13.1.1745333250.60.0.0; _clck=1544l4t%7C2%7Cfvc%7C0%7C1926; _ga_KB4HEYN24H=GS1.1.1745507615.2.0.1745507617.58.0.0; _ga_25K1QHSCNF=GS1.1.1745507615.2.0.1745507617.0.0.0; _ga_BLFQ3RHFYK=GS1.1.1745507615.2.0.1745507617.0.0.0; _ga_3TZPTEZXBM=GS1.1.1745507615.2.0.1745507617.0.0.0; _ga_XCF0ZL4X3Y=GS1.1.1745558352.18.0.1745558352.60.0.0; __hstc=217543049.750bec2eb354f317c2c2b7f23a410836.1742669285085.1745541145941.1745558352916.26; _hjSession_3307954=eyJpZCI6IjllMzE5YTlmLTVlODgtNGM2ZC1hYTEyLWQyOTRkOTMzZjJhMSIsImMiOjE3NDU2MjIzMjIzMjIsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowfQ==; _gcl_au=1.1.15727863.1742669284.199752660.1745622335.1745622334; AWSALB=Fioge7gjXog44vGjhU+F2s6tp7h8lR9GZZHHOyrKKV0Tbfv54mwnA4u5KJ10UWX0umO2cp8W988AzdNZwmfQFBCuOlyhgYops/jDJMcugIfY7aeMawzKacz5q+1d; AWSALBCORS=Fioge7gjXog44vGjhU+F2s6tp7h8lR9GZZHHOyrKKV0Tbfv54mwnA4u5KJ10UWX0umO2cp8W988AzdNZwmfQFBCuOlyhgYops/jDJMcugIfY7aeMawzKacz5q+1d'}


def sanitize_filename(text):
    return re.sub(r'[\\/*?:"<>|]', "_", text)


async def get_all_documents_for_query(query: str, max_pages: int = 20, count: int = 50, page:int=1) -> pd.DataFrame:
    """
    Paginate through all pages for a given query and compile into a single DataFrame.
    """
    all_docs = []
    
    for page in range(1, max_pages + 1):
        url = f"https://app.vlex.com/search.json?product_id=WW&jurisdiction=US&country_jurisdiction=TX&content_type=712&textolibre={query}&bypass_rabl=true&include=parent%2Cabstract%2Csnippet%2Cproperties_with_ids%2Ccitation_counts&per_page={count}&page={page}&sort=score&type=document&locale=en&hide_ct6=true&t=1744702830"
        print(f"[DEBUG] Fetching page {page}: {url}")
        response = requests.get(url, headers=headers)
        data = response.json()

        docs = data.get("documents", [])
        if not docs:
            break  # No more documents

        all_docs.extend(docs)

    return pd.DataFrame(all_docs)


async def fetch_page(
    session,
    page: int,
    base_url: str,
    encoded_query: str,
    doc_type: str,
    count: int,
    max_retries: int = 3,
    backoff_factor: float = 3.0
):
    """
    Fetches and parses a single page worth of documents from VLex (async), 
    with retry and exponential backoff for transient errors (e.g., HTTP 503).
    
    :param session: The aiohttp ClientSession.
    :param page: The page number to fetch.
    :param base_url: The base URL for constructing final document links.
    :param encoded_query: The already URL-encoded query string.
    :param doc_type: The VLex 'type' param (e.g., 'document').
    :param count: Number of records per page.
    :param max_retries: How many times to retry on certain 5xx errors.
    :param backoff_factor: Base wait time, multiplied by (2^(retry-1)).
    :return: (full_urls, has_results) 
             full_urls: List of constructed PDF URLs for this page.
             has_results: Boolean indicating whether results were found.
    """
    url = (
        f"https://app.vlex.com/search.json?product_id=WW"
        f"&jurisdiction=US&country_jurisdiction=TX&content_type=713"
        f"&textolibre={encoded_query}&bypass_rabl=true"
        f"&include=parent%2Cabstract%2Csnippet%2Cproperties_with_ids%2Ccitation_counts"
        f"&per_page={count}&page={page}&sort=score&type={doc_type}"
        f"&locale=en&hide_ct6=true&t=1744702830"
    )

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()  # Raise if status code isn't 2xx/3xx
                data = await response.json()
                results = data.get('results', [])

                if not results:
                    # If empty, return no results + False
                    return [], False

                # Build final PDF URLs
                ids = [r.get('id') for r in results if r.get('id')]
                full_urls = [f"{base_url}{doc_id}" for doc_id in ids]
                return full_urls, True

        except aiohttp.ClientResponseError as e:
            # For 5xx or certain 4xx, we can attempt a retry
            # If it's specifically 503 or in 5xx range, do backoff
            # If it's a 4xx we might want to break immediately, depending on the service
            is_server_error = 500 <= e.status < 600
            is_too_many_requests = (e.status == 429)
            if (e.status == 503 or is_server_error or is_too_many_requests) and attempt < max_retries:
                sleep_time = backoff_factor * (2 ** (attempt - 1))
                print(f"Got {e.status} (Service Unavailable). Retrying page={page} "
                      f"attempt={attempt}/{max_retries} after {sleep_time:.1f}s...")
                await asyncio.sleep(sleep_time)
            else:
                # If we ran out of retries or error isn't one we should retry, re-raise
                raise

        except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError) as e:
            # Covers network-level or server disconnect issues 
            if attempt < max_retries:
                sleep_time = backoff_factor * (2 ** (attempt - 1))
                print(f"Network/Server error on page={page}, attempt={attempt}/{max_retries}. "
                      f"Retrying after {sleep_time:.1f}s...")
                await asyncio.sleep(sleep_time)
            else:
                raise

    # If we somehow exit the loop without returning, return empty
    return [], False


async def get_documents_concurrently(
    query: str = 'en banc reconsideration void',
    doc_type: str = 'document',
    count: int = 50,
    max_pages: int = 5000,
    chunk_size: int = 10,
    concurrency_limit: int = 5,
    max_retries: int = 3,
    backoff_factor: float = 3.0,
    opts=None
):
    """
    Fetches VLex documents concurrently in batches to speed up retrieval, 
    with a retry/backoff for transient errors (like HTTP 503).
    
    :param query: The search query to run.
    :param doc_type: VLex 'type' (e.g., 'document').
    :param count: How many documents to get per page.
    :param max_pages: The maximum number of pages to attempt.
    :param chunk_size: How many pages to fetch in each concurrency batch.
    :param concurrency_limit: The max number of concurrent tasks.
    :param max_retries: How many times to retry each page on certain errors (503, 5xx).
    :param backoff_factor: Base factor for exponential backoff in seconds.
    :param opts: Object that has `batch_upsert_dataframe(df, table_name, unique_columns)`
                 for DB upsert. If None, results are not persisted.
    :return: None (results are upserted to DB if `opts` is provided).
    """
    
    encoded_query = urllib.parse.quote_plus(query)
    base_url = "https://app.vlex.com"
    
    sem = asyncio.Semaphore(concurrency_limit)
    pages = list(range(1, max_pages + 1))

    async with aiohttp.ClientSession() as session:
        for i in range(0, max_pages, chunk_size):
            chunk = pages[i:i + chunk_size]
            
            tasks = []
            for page in chunk:
                tasks.append(_fetch_with_sem(
                    sem, session, page, base_url, encoded_query, doc_type, count, max_retries, backoff_factor
                ))

            # Wait for all tasks in this chunk
            results = await asyncio.gather(*tasks)
            
            # Accumulate all URLs for this chunk
            all_urls_in_chunk = []
            continue_fetching = True

            for full_urls, has_results in results:
                if not has_results:
                    # No results => likely end of pages
                    continue_fetching = False
                if full_urls:
                    all_urls_in_chunk.extend(full_urls)

            # If we got any results, upsert them
            if all_urls_in_chunk and opts is not None:
                dict_data = {
                    'query': [query] * len(all_urls_in_chunk),
                    'pdf_url': all_urls_in_chunk
                }
                df = pd.DataFrame(dict_data)
                await opts.batch_upsert_dataframe(
                    df, table_name='new_documents', unique_columns=['pdf_url']
                )

            # If we encountered an empty page in this chunk, break early
            if not continue_fetching:
                break


async def _fetch_with_sem(
    sem, session, page, base_url, encoded_query,
    doc_type, count, max_retries, backoff_factor
):
    """
    Simple wrapper to respect concurrency limit via a semaphore.
    """
    async with sem:
        return await fetch_page(
            session=session,
            page=page,
            base_url=base_url,
            encoded_query=encoded_query,
            doc_type=doc_type,
            count=count,
            max_retries=max_retries,
            backoff_factor=backoff_factor
        )

async def search_caselaw(query: str, type: str = 'document', per_page: int = 50, max_pages: int = 20) -> pd.DataFrame:
    """
    Paginate through vLex caselaw results for a given query, compile into one DataFrame.
    """
    encoded_query = urllib.parse.quote_plus(query)
    all_records = []

    for page in range(1, max_pages + 1):
        url = (
            f"https://app.vlex.com/search.json?product_id=WW&content_type=2"
            f"&date=1933-01-01..&reporter_ids=30748&textolibre={encoded_query}"
            f"&source=14062_01&jurisdiction=US&bypass_rabl=true"
            f"&include=parent%2Cabstract,snippet,properties_with_ids,citation_counts"
            f"&per_page={per_page}&page={page}&sort=score&type={type}&locale=en&hide_ct6=true"
            f"&t=1744702830"
        )

        r = requests.get(url, headers=headers).json()

        results = r.get('results', [])
        if not results:
            break

        for i in results:
            most_neg = i.get('most_negative') or {}
            citations = i.get('citation_counts') or {}

            record = {
                'id': i.get('id'),
                'title': i.get('title'),
                'snippet': clean_snippet(i.get('snippet')),
                'type': i.get('type'),
                'color': most_neg.get('color'),
                'last_cited': most_neg.get('last_cited_in'),
                'cited_in_count': most_neg.get('cited_in_count'),
                'treating_example': most_neg.get('treating_example'),
                'total_citations': citations.get('total'),
                'positive_citations': citations.get('positive'),
                'negative_citations': citations.get('negative'),
            }
            all_records.append(record)

    df = pd.DataFrame(all_records)
    df.to_csv(f"vlex_results_{query.replace(' ', '_')}.csv", index=False)
    print(f"[✓] Final DataFrame saved: vlex_results_{query.replace(' ', '_')}.csv")
    await opts.batch_upsert_dataframe(df=df, table_name='new_caselaw', unique_columns=['id', 'snippet'])
    return df

import asyncio
def sanitize_filename(text):
    return re.sub(r'[\\/*?:"<>|]', "_", text)

def save_urls_as_pdfs(urls, output_folder="pdf_output"):
    os.makedirs(output_folder, exist_ok=True)

    for url in urls:
        try:
            filename = sanitize_filename(url.split('/')[-1])[:80] + ".pdf"
            filepath = os.path.join(output_folder, filename)

            if url.lower().endswith('.pdf'):
                # Direct PDF download
                r = requests.get(url, headers=headers, stream=True)
                if r.status_code == 200:
                    with open(filepath, 'wb') as f:
                        for chunk in r.iter_content(1024):
                            f.write(chunk)
                    print(f"[✓] Direct PDF saved: {filename}")
                else:
                    print(f"[✗] Failed to download: {url} — Status {r.status_code}")
            else:
                # Render HTML to PDF
                pdfkit.from_url(url, filepath, configuration=config)
                print(f"[✓] Rendered HTML to PDF: {filename}")
        except Exception as e:
            print(f"[❌] Failed for {url}: {e}")



async def main():
    await opts.connect()
    urls = await get_documents_concurrently("best interest of the child", opts=opts)
    save_urls_as_pdfs(urls, output_folder="abuse of discretion")

asyncio.run(main())
