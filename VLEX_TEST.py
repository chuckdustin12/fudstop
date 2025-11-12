import aiohttp
import asyncio
import re
from fudstop4.apis.vlex.vlex_sdk import VlexSDK
from IPython.display import display
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions()
import pandas as pd
vlex = VlexSDK(cookie="cookieyes-consent=consentid:V1Jud24zbmhoUXo5ZzRWTEkzTWNwM3p6b3E2a1AyMFY,consent:no,action:,necessary:yes,functional:no,analytics:no,performance:no,advertisement:no,other:no,lastRenewedDate:1751660224000; hubspotutk=75aead09fd586044872738a61b17d369; __hssrc=1; ALT=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VySWQiOjExNjI4MTU5Mjh9.tTJLNue5HbuzcW_52UlDJtgV4Rci4Ola6MWi25mi_oY; _ga_KB4HEYN24H=GS2.1.s1762278072$o1$g0$t1762278072$j60$l0$h335377593; _ga_BLFQ3RHFYK=GS2.1.s1762278072$o1$g0$t1762278072$j60$l0$h0; _ga_3TZPTEZXBM=GS2.1.s1762278072$o1$g0$t1762278072$j60$l0$h0; _ga_KVKSRXVYPV=GS2.1.s1762278072$o1$g0$t1762278072$j60$l0$h0; _ga_PERNN228VM=GS2.1.s1762278072$o1$g0$t1762278072$j60$l0$h0; idioma_id=EN; keymachine=5A4NSRLYP7OA; LT=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3NjIyNzg5MzIsImxvZ2luX21ldGhvZCI6Imdvb2dsZV9hZG1pbiIsInZsZXhfdXNlcl9pZCI6MTE2MjgxNTkyOCwiaXNfYWNjb3VudF9tYW5hZ2VyIjp0cnVlLCJhY3RpdmVfcHJvZHVjdHMiOlsiVklOQ0VOVF9SRVNFQVJDSF9BU1NJU1RBTlRfVVNfU01BTExfTEFXX1RSSUFMIl0sInNpdGVfZG9tYWluIjoiYXBwLnZsZXguY29tIiwidXNlcl9yb2xlcyI6W10sImVtYWlsIjoiZG9va2VyYmFsbHpAZ21haWwuY29tIiwiaW5oZXJpdGVkX2FjY291bnRzIjpbMTE2MjgxNTkyOF19.Q5bc6xQaUhBz5AxAgnBtkMdgTKYzuuCUyy-jhZgFz5c; id_token=eyJhbGciOiJSUzI1NiIsImtpZCI6IkUxOEFFOTJGNTZEMjUyMTU4QzU1Q0ZGMDM4OTgwQzYyNzhEMTY1OEIiLCJ0eXAiOiJKV1QiLCJ4NXQiOiI0WXJwTDFiU1VoV01WY193T0pnTVlualJaWXMifQ.eyJuYmYiOjE3NjIyNzg5MTUsImV4cCI6MTc2MjI3OTIxNSwiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS52bGV4LmNvbS8iLCJhdWQiOiJ2bGV4LXdlYmFwcCIsImlhdCI6MTc2MjI3ODkxNSwiYXRfaGFzaCI6IkJPV1U1R0k5QTZ1MGxodWM1bzF2VFEiLCJzX2hhc2giOiJ4cmZCSVdVUXJILTVOeWZSbmdoTWZRIiwic2lkIjoiMkNBTy1MMVZYNlJuNDV6NVNWLVM4USIsInN1YiI6IjExMTk1MTg0MDQ4MTQyNjc4OTE1MyIsImF1dGhfdGltZSI6MTc2MjI3ODkxNCwiaWRwIjoiR29vZ2xlIiwibmFtZSI6ImRvb2tlcmJhbGx6QGdtYWlsLmNvbSIsImh0dHA6Ly92bGV4LmNvbS9jbGFpbXMvYXV0aHN0YXR1cyI6IlZBTElEX1VTRVIiLCJ2bGV4X3VzZXJfaWQiOiIiLCJ1c2VyX2lwX2FkZHJlc3MiOiIxNzIuOTguMzMuMjMzIiwiaHR0cDovL3ZsZXguY29tL2NsYWltcy9hdXRocHJvdmlkZXIiOiJHb29nbGUiLCJsb2dpbl90eXBlIjoiZ29vZ2xlIiwiZXh0ZXJuYWxfc3ViamVjdCI6IjExMTk1MTg0MDQ4MTQyNjc4OTE1MyIsImVtYWlsIjoiIiwiYW1yIjpbImV4dGVybmFsIl19.OzLWPd3bsK8FmiPw6gqRPxMNyUcg08f3kedawrlvzYrmdN2YjeYzpuATSUMyk70avRw4e0ZomxWFvskJa6btnOXvLAGzjHw0YS0IBBdX4VS5G35ywoKOF2yXhRAl_HrmD47XkNgpojuKyuXOGPffRSVYRcyymO8EAwOfSG5VHqO-icfqpwsTnd3ogzDCyRmhIw9U_1uPQhBZ5stoobVQ7MmUQHnWTj3uplXLfS6YPP23JblA-yJvtfUQ78R6WGA3ME-j9aoDnb7WTrPLWYsA571fNJA9E8Q06dib2_0MMLqjfPnXuUii4mF1vpjEjjv1nbLAFHm5bz3zYrt5uTaUmQ; vlex-session=BAh7HUkiD3Nlc3Npb25faWQGOgZFRiIlYmJiMTllOTMxOWNiMTQ0ODYyODcwYWIxZTk2YmQ1OTVJIhNza2lwX2F1dG9sb2dpbgY7AEZGSSIUbW9iaWxlX3ZlcnNpb24%2FBjsARkZJIgxwYWlzX2lkBjsARkkiB1VTBjsAVEkiDmlkaW9tYV9pZAY7AEZJIgdFTgY7AFRJIhRjb250ZW50X3BhaXNfaWQGOwBGQAtJIhF1c2VyX3BhaXNfaWQGOwBGQAtJIhJ3ZWJhcHBfbG9jYWxlBjsARkkiB2VuBjsAVEkiEF9jc3JmX3Rva2VuBjsARkkiMWJseHFpNDFVT2MwUlBWanY3WWtFMjVjWFVYWUJxZlJ3Y0Nhclk0TkIwVWM9BjsARkkiD3V0bV9tZWRpdW0GOwBGSSIJc2VycAY7AFRJIg91dG1fc291cmNlBjsARiIYYWNjb3VudHMuZ29vZ2xlLmNvbUkiEWxhbmRpbmdfcGFnZQY7AEZJIhxodHRwczovL2xvZ2luLnZsZXguY29tLwY7AEZJIhVvcmlnaW5hbF9yZWZlcmVyBjsARiJbaHR0cHM6Ly9sb2dpbi52bGV4LmNvbS9lcnJvcnMvb2F1dGhfZmFpbHVyZT9lcnJvcl9jb2RlPTAwNyZwcm92aWRlcj12bGV4SWRlbnRpdHlTZXJ2ZXJJIhB1cmxfcmVmZXJlcgY7AEYiFmh0dHBzOi8vdmxleC5jb20vSSITd2ViYXBwX3JlZmVyZXIGOwBGSSIABjsARkkiDHVzdWFyaW8GOwBGbCsHuClPRUkiDGRvbWluaW8GOwBGIg12bGV4LmNvbUkiCmxvZ2luBjsARkkiEWdvb2dsZV9hZG1pbgY7AFRJIhNsYXRlc3Rfc2Vzc2lvbgY7AEZvOhBMb2dTZXNpb25lcw06EEBhdHRyaWJ1dGVzexZJIgdpZAY7AFRJIg40MDkxNzQ0NDAGOwBUSSIPdXN1YXJpb19pZAY7AFRJIg8xMTYyODE1OTI4BjsAVEkiEGtleV9tYXF1aW5hBjsAVEkiEVdJUkxGSE9RWEw3MwY7AFRJIg9zdF9kb21pbmlvBjsAVEkiDXZsZXguY29tBjsAVEkiFHRfaW5pY2lvX3Nlc2lvbgY7AFRJIh8yMDI1LTExLTA0IDE2OjU2OjIwLjQzNDUwMgY7AFRJIhR0X3VsdGltYV9hY2Npb24GOwBUSSIfMjAyNS0xMS0wNCAxNjo1NjoyMC40MzQ1MDQGOwBUSSITYl9kZXNjb25lY3RhZG8GOwBUSSIGZgY7AFRJIhZjdF9wYWdpbmFzX3Zpc3RhcwY7AFRJIgYwBjsAVEkiD3N0X2NvZGFyZWEGOwBUSSIyVklOQ0VOVF9SRVNFQVJDSF9BU1NJU1RBTlRfVVNfU01BTExfTEFXX1RSSUFMBjsAVEkiDmlwX3JlbW90YQY7AFRJIhM1NC4xNjQuMTg5LjEzNgY7AFRJIhZ1c3VhcmlvX2FnZW50ZV9pZAY7AFQwSSIhdXN1YXJpb3N4YXJlYXNwcmVtaXVtX2JfZGVtbwY7AFRJIgZ0BjsAVEkiEGtleV9wZXJzb25hBjsAVEkiE3JlbWVtYmVyX21lX0xUBjsAVEkiEWN0X2Rlc2NhcmdhcwY7AFRJIgYwBjsAVEkiFHJvb3RfdXN1YXJpb19pZAY7AFRJIg8xMTYyODE1OTI4BjsAVEkiDWJfaGlkZGVuBjsAVDBJIg1wbGF0Zm9ybQY7AFQwOhhAY2hhbmdlZF9hdHRyaWJ1dGVzewA6GEBwcmV2aW91c2x5X2NoYW5nZWR7ADoWQGF0dHJpYnV0ZXNfY2FjaGV7BkkiFHRfdWx0aW1hX2FjY2lvbgY7AEZVOiBBY3RpdmVTdXBwb3J0OjpUaW1lV2l0aFpvbmVbCEl1OglUaW1lIzIwMjUtMTEtMDRUMTY6NTY6MjAuNDM0NTA0MDAwWgY7AEZJIghVVEMGOwBGQE46HEBtYXJrZWRfZm9yX2Rlc3RydWN0aW9uRjoPQGRlc3Ryb3llZEY6DkByZWFkb25seUY6EEBuZXdfcmVjb3JkRkkiIWRheXNfc2luY2VfcHJldmlvdXNfYWN0aXZpdHkGOwBGaQBJIgdpZAY7AEZpBMQXZBhJIg9jbHVzdGVyX2lkBjsARmn6SSIRYWxnb3JpdG1vX2lkBjsARmkGSSIRc2Vzc2lvbl90eXBlBjsARkAl--099c765f797bae8e52afbec523cda2ac9d5a708e; _hjSessionUser_3307954=eyJpZCI6ImMxY2RkZDE3LWJhMmMtNWU2MS05ZGVlLTZiNDVjNDQ1NTE3NiIsImNyZWF0ZWQiOjE3NjIyNzg3NjE0NjMsImV4aXN0aW5nIjp0cnVlfQ==; _hjSession_3307954=eyJpZCI6IjJjOTc5YmQwLWM5ODQtNDczZC1hZDE3LWQzOWVjMjZlYTM3OSIsImMiOjE3NjIyODYyMjI3MzMsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowfQ==; _ga_XCF0ZL4X3Y=GS2.1.s1762286228$o16$g0$t1762286228$j60$l0$h0; _ga=GA1.1.112495237.1761748999; __hstc=217543049.75aead09fd586044872738a61b17d369.1761308440647.1762278073644.1762286228725.19; __hssc=217543049.1.1762286228725; _fbp=fb.1.1762286229002.321251116475258667; _gcl_au=1.1.543710295.1762286228.1373547981.1762286269.1762286269")
def clean_text(text: str) -> str:
    """
    Fully general-purpose text cleaner.
    Removes all forms of \n and related patterns (\n\n, \n\nB, etc.)
    and replaces them with spaces or appropriate punctuation spacing.
    """

    # Normalize all newline styles
    text = text.replace('\r\n', '\n').replace('\r', '\n')

    # Remove control/non-printable characters
    text = re.sub(r'[^\x20-\x7E\n]', '', text)

    # Remove explicit newline patterns like '\n\n', '\n\nB', '\nB', etc.
    text = re.sub(r'\n+\s*[A-Z]?\s*', ' ', text)

    # Collapse multiple spaces
    text = re.sub(r' {2,}', ' ', text)

    # Ensure spacing after punctuation
    text = re.sub(r'([.,!?])([A-Za-z0-9])', r'\1 \2', text)

    # Strip leading/trailing spaces
    text = text.strip()

    return text

def safe_get(d, key, default=None):
    v = d.get(key)
    return default if v is None else v

def clean_text(s):
    # your cleaner here; keep a no-op if not needed
    return (s or "").strip()

async def main():
    await db.connect()
    data = await vlex.get_user_history()
    items = data.get('items', []) or []

    rows = []
    for it in items:
        q  = clean_text(it.get('question', '') or '')
        sk = it.get('skill')
        for t in (it.get('tasks') or []):
            rows.append({
                'question': q,
                'skill': sk,
                'type': t.get('type'),
                'task_id': t.get('task_id'),
                'heading': t.get('heading'),
                'text': clean_text(t.get('text', '') or ''),
            })

    df = pd.DataFrame(rows, columns=['question','skill','type','task_id','heading','text'])
    await db.batch_upsert_dataframe(df, table_name='vlex_final', unique_columns=['task_id'])

asyncio.run(main())