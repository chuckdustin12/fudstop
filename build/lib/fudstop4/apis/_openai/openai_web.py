import sys
from pathlib import Path
import base64
# Add the project directory to the sys.path
import requests
project_dir = str(Path(__file__).resolve().parents[1])
import os
from dotenv import load_dotenv
import aiohttp
import asyncio
load_dotenv()
import time
import aiohttp
import json
from openai import OpenAI
import pandas as pd
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
import asyncpg
import datetime
import aiohttp
from .tools_shcema import tools, serialize_record
class OpenaiWEB:
    def __init__(self, bearer_token=None, cookie=None, device_id=None):

        self.device_id = device_id
        self.bearer_token = bearer_token
        self.cookie = cookie
        self.headers  = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5MzQ0ZTY1LWJiYzktNDRkMS1hOWQwLWY5NTdiMDc5YmQwZSIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS92MSJdLCJjbGllbnRfaWQiOiJhcHBfWDh6WTZ2VzJwUTl0UjNkRTduSzFqTDVnSCIsImV4cCI6MTc0MDYyOTY0MSwiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS9hdXRoIjp7InVzZXJfaWQiOiJ1c2VyLWRTYXo5OEtBY1RaV01lSzJ3ZDJJRTFCRCJ9LCJodHRwczovL2FwaS5vcGVuYWkuY29tL21mYSI6eyJyZXF1aXJlZCI6InllcyJ9LCJodHRwczovL2FwaS5vcGVuYWkuY29tL3Byb2ZpbGUiOnsiZW1haWwiOiJjaHVja2R1c3RpbjEyQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlfSwiaWF0IjoxNzM5NzY1NjQxLCJpc3MiOiJodHRwczovL2F1dGgub3BlbmFpLmNvbSIsImp0aSI6ImVlM2I0ZjRiLWYyNTgtNDkxMi1hZTM3LTA5MDczNzI4N2ZlOCIsIm5iZiI6MTczOTc2NTY0MSwicHdkX2F1dGhfdGltZSI6MTczODkwMDYwNzU0Nywic2NwIjpbIm9wZW5pZCIsImVtYWlsIiwicHJvZmlsZSIsIm9mZmxpbmVfYWNjZXNzIiwibW9kZWwucmVxdWVzdCIsIm1vZGVsLnJlYWQiLCJvcmdhbml6YXRpb24ucmVhZCIsIm9yZ2FuaXphdGlvbi53cml0ZSJdLCJzZXNzaW9uX2lkIjoiYXV0aHNlc3NfM0NwQ2JjQVFmd2U3eEF0ZTB1ZTdEVmNOIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDU1ODc0Nzc1MDc0MTQzNDY1ODIifQ.vp_oXrF7JgOr_TVdcPEAfhIsCkpvmbgBrlo2sjccxi-ta3nrdMg2mLsRHNrTdX1HuLEIYpNMoDloBL2YMmACapUA5jSl3AJRcQ0zIEcSxMwVSr7NBao1Ode5TXrgWPAJSA81nG1UI8YEQW2hVnGAra2FgNEZrTe_h3W3fIsgoPeZonSdBOrHAItKzURfkCv3uJCXkDVx4M6bL-kdnxWN-1WEZaF0cC54CjMw_d56dYTKnC39AO555TIfBtGPBCDYMMMUPh5ps9le3lL1oapKjnuKPG5eAn-8JQt1iFzdPiFz36QIB3SU_CQkSU5J3WTjD6Q4HuS2xeggI8yHW3N2jYbnuEUWGEbsjJGm4zM2hKj-ZvOZHxIXHKNvwm7THiIhbPQ9EBBwo4h7mSNcPdKpleOaTASCV6NTd4zYYZsFgcQAP0MoIUIEtmAA2wIiUNZ9WXaB96yrqLPrc1K7_Av-bkuraoLcDju9NWwj18Oh7W2LlmDW2KjB_FClQaGCP3vn6gPGqc330wUiz1-bifHuPlEfoNPQxQG4Aa6KNGNNR-ZLDZ-bDHGF6wTcLFf2SySEh3aKygPHCbGzFzz2DD5oJF2Tu8RoC4EN76FAF88FA_gY4G7DE66zAZYoB74KjOlnuz9cb2UcqHaCAplB8iKoa51WUXQ7AIUkEABT45c3z_A",
    "cache-control": "no-cache",
    "cookie": """oai-did=50ed7d6c-fe11-47b4-88e0-f0077cc93e55; oai-hlib=true; _account=personal; oai-nav-state=1; oai-sc=0gAAAAABnkWV-lmZuTz_MgC5JoBajQ1e3zFtNxiFrTcUDqyQV1OzKhFmksQVyMLYrpNNMs2wkaWBxgOHDLluPyxBr_nook0D4bgYljLP5_svG1VlqnkdNap1KrSX5TWNUxITNUPgQz7NfiqnJ3upter-dzCiCnS_ZxmAzftvmYD6ineUcXeG8iiufbVLmEUO2FuLKRcmNvHNYUSbEQVEGkcFA6P0pkLKx5b3n8g99n2ZqcG_Ebae-c1o; oai-sh-c-i=679c5a62-5670-800f-a58b-851fdb582ed7; __Host-next-auth.csrf-token=0bd238e6f83bc532701fe13437454a8a0f89b69916b28a95513941b274a48054%7C8cd2e2804671bc61b157bd7ee1b6dd8a62dfcff20c4042ded7324ce126c7c11b; __Secure-next-auth.callback-url=https%3A%2F%2Fchatgpt.com; oai-last-model=o1-pro; oai-thread-sidebar=%22%257B%2522isOpen%2522%253Afalse%257D%22; _uasid="Z0FBQUFBQm5yN01qOV9PN3NfLXFQakpQdDFUdFVwVHkzZEFUU2cwOVAzYzBjdDJTcUZBRUpDcmR0MkZ1T0tOSkE3bjQ4OE5jMnYxdm81X1l2dnlLNl9vNlFFQjZZU0VnSlZpTGFJTDVjc3BROUVaOHlyTFdzN1NQelNkWnByUm9NOVFBRXY2d0gtM1gyck4tWW9fcHhaTS0xSElnT1FHVmFTZVdQNDYwQ2s5RVpUeUJyT2Noa2J4YkdhMkhaYko3akRSd3Y5UW9wTkRJLXotWHJhbDVkVjBFZ290NXp0RUl6YUZ6TllXMVJBenpLV0Q3amg3VDZXczdkUHdkazc3ZEp5WlU4bHFsTFFNRXREUXlqMkd6dWFYRVNxeUVGejNaQlF0VC1pdmVhRDR3cHktamJkQV9Td2QtNG9JeEhoU3VoNVdJT0Vjdkc3OWVkSWhOLXVYU0pRTlJpNlZqUWUzcUlBPT0="; _umsid="Z0FBQUFBQm5yN01qeC1hYXk2YTNmRHl2ZHI3ZGdCbmZhWkowMW02MVA4emFUbS10dklBa2lhOUExNDNqaURDSUZSVW9vd2NBbG52TzM2ZTItNVktcUlpUFhSaWYyWDI3bTA3S05abkZCWVFFWjI4dW5PWFUzRTlQVTZHY3VQWGJuc0ZBemU2WXdRNjJiM1l3ci0wQjVacjdNWUZ3QlgwOVB3LUpqblhSSFExUEt5U3BkTTBhQ3B2OHlPbUxFWFR3YjJtVml0LU8yQnF2TXJoOWZaSEZPaUpnU2xJaXppQ2xEMmZyYlFWOUJwTE94ZnBzRmgyWUxRbz0="; __cf_bm=qsFI6k6aY03BZ4XcRyRttmNQwIczFJPp6TJN7loCKEY-1739568095-1.0.1.1-domgQ.nWAUgHlUMw8UScp3ETKShzZXBibrQrVxEggu45LLPfIS4U0mG0db3C6hCp1QXgXSoaalFTZgJxSYhxSw; _cfuvid=FFCQ5E9PcsYlVvfu3XfjZL_fVzQ_SyR7jj2PjQ1GzXM-1739568120982-0.0.1.1-604800000; cf_clearance=9WTfTZ8E_GzCJYliL6D6Os1Ar0AKPWEIVr2VsyEQU_Y-1739568122-1.2.1.1-sJgGT7nba16HrlrdUnjmVdznwFKdAg.jGbS4lXwbf5sQpa3xrmC9ktJH.O7NZxxGTaMdRuRXjaI4VA9bLD0wrJg666p7kJgAee__NgjSg0SsBvTp.xOujqH_4qTYMB.3uC9vhVDmxPF.7CTjqnJH.ETYAUV54z41K6CUBLCbLqsZGvwXw_kgtVygX3343r0wE9XqJfiounQsEZZijQWsatfBCKeCMND1fKWd8fE3EuLJ.QDYPiLqD4DVnLHKDQ6sxTpNlPUbVUEm7UAL4vsunENlCHN21NdHonSMX8lpCM80hS00tT4iI4133tAuzYwI7hAwBNmm98YJVSoSIMvWuA; oai-sc=0gAAAAABnr7P6My078uYWD98jetJdTzJgNib_uXnrZ6SI9XYlOtf6nP7SUx3blkT5WuZHJbR1BECygXLJi0QpED_Zm_2Rpe6hINZ-GFwnnV9zXQCf-8MukDbifMgB1b5TKtkM8bO8wru0K6dE9eJVrweUo-AnJn6rwe4PqKiGlv9U3y2Q2rhLgeE0VW3GC5bW4R6-SsH1zSDAv-Va7pKCTLEQ92PaaVxuErWehorK0XgIAM-S701MsDw; __Secure-next-auth.session-token=eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..PQ7jYpjO73INwPB2.41bIvujq1B5PH0BddI2185Wgz5L8w9PxJh62NCZpQNFQuu_8CTU9TX47CTUn0YqoQvLLRVNRpm5H3dGAKUFd3TwQLuejJqLaWU2Ei0FuAmtvW-wOXRT80C0OIWEa3CEw7-Y1bsYFnAD7j9MupXwIqz91pf9vktWB0g4k0sZ5NhBqlu1BCUcI3iQer2mZI1OSjd2yIyduB6FNuu5GbAOjxpPqcnipwBDr5cguEhwBY6yz4R0zrsAppacQcyRCjCAqMQENJD9wlwZ72AoOtBMmObUTuLg3NJdh9W5rpuu6cqhYD2myMZ0ffMeAFPeaxOQsL1w4AhgpqT37kZyEevWqzGPaNFUpy-blABwnDESWVcNXy9DLrE0MD837k8zH4KU41HU6pCAV1kCWBECAVV0FqyTZThYlb8S0d4KuTnPbM5qIJ2JmrW_DJpdL_quOOdRo_icthjXIz0PuPynzZpji_WsS0OovmasSeV8qvqI_2V9-1TQAnV8HkdAcr6KunC21srBni6DPGoQWPTu-dM2K6sSW2GmrRyPf4aaknTtcO4zJ5MPRtCL_bBkY0cPfJAUpzsjKcIjtoMTGw5zLEqJagQf_MNSuPJm4_3fxc4bQ9YFnDZiVY7xR3MkSNZ4xO7F--XGuQkhiVJOyWzb7qRqisUwbTrayU7fbrj1Shtl-ABJFGm8EUdyAeEtPs-J35Qc9ZdkTkq9LbltxML96oHtUyZQnZ_nrnRKVDeFIOwcFwTFdcCnUo0EJvnnvjPtjNh9ewlZdpdoPSnavMV8HTYd-DpGZjgo6TgQSwq4Wu1LGBe0s_2N1Kcx-_OHLlDeRHRxn6D7n0FKQsA58BO6m_FwegNJXHLPsIfAv4O_K9PW4rTjq5wJA83eq9rL7P2s6kKwh-hxLVR_Pvwx_R4Bdw3r9gCOhFEWbszOWNhdxyi7R80PMZ60xtEj9WCVjbCZJYYOG6Ps9M3XNaCTU2VO6oe3Q3kFmQtqsi5qTSp1lVrvJucjygU4jYMLUVC3cdzIuhsdJ19Kn7nXZ_H_WTNP_iwhNxALqG8jbd5xV9tbQh4RjfdGs955BgfjoHLsHGuzp0XFJAhVGvjNSVGe0rgCTS_-2Scqp985OD9Ilj7GRPRY6sPERkcKGaLoYlUZ4Q5w_n-4D3n8UrzEmH7mBgEAuCd2kzyckNcXLkh1OzXLjJRuiPFduLiVi5dzbw0TSbOPRqOVOgBtRpvjQT-LTSh35fM_CsMxR-hFQV0OTVIrAapnSPZXh1hH4ZWFBCyYuJlCAsfh99aquaR_w5ywCN2Si7-I2I0lx9upPA-osZ4I-lI_NHPzH8k-2KOnXjMemBnR6QUldyD2q3zHYHwYF6YKtXPeECefS484HZObKuF6u99krfkKENytRHNhrWxz8KtHmrPCZ45QDyQsGxW6W549t2EO6zvQCvS74PnAdInJFuhq5u0dBbCobFkzqOFPCj-xO0j6njY_LYmIXNNZig5sfuB37XjkK-X-NLdCuVquNyDOT9lO-AqDJogJJogi2CoehJZkYL07xG8zyN0-oSu4s2oeZQdoeyX2EFgIVwuhkUF7j1rhW9T3kgzrDpWZogZzjxziRTFm8BvHYvHXZgPhE8PJVs0HMj7-ej9hBLiyCwkaMW1IvdZC3XZ3P7e-KKkHq0ZQDy1f7XKhsUbWv91wrWyZWL-naEX60nLIJ3hFLKkoh7NO0ezlTkPrdsKuXfWQdf7yTr5pPsln7n62mJIE65_0akDY_8EodYq7YvYf1Et8rkucprlYlkoWCgHiGPcJ8t2_snj_VjpDCmGxYOzSW5eWLQZ61DrH9oam632pBt8CqmBbNiK-ElYLk0IeDYKwLMSOjD7Srn1vPhwMBUuJp8UcTI6dw38cvjD7G_yQbqAFoTaANgMXuDl8lMWxU9Rk87ZnZadLg81Ot7ExvokG2frnK2oGfR8VB90wesHaLKxVg4fxs35AqgVBJnFYs-_7dxc_s5nlb3KScEEa9YO-yhAempYiFpf_ioip7N7iOmIipXL7nDKzaW6Wuwa_g42l6_yNYN1nFIFhxtAI8bSuc6Kfb1QibCygVPlcbyuCMBoHMeq5_k_RGLu9-pZQBJ2VpWNsRLigDU98_pxk1xiG6vkWEMsSF71nI0oNa6fIl0jPYvxZbToHbKUR1a4xWSdKtgWD5s4bMUQCqMHPlvJzm5EBe5yVB_jPml7QYE0shbIYzd2XkS4ERK5c6MPndTMlMGlnQPvM2jyZSRspQVJigRj98jDESa_f5Il9BrTxE6-0-2S_6jDuy7h_0s0ir8cgn3ohXHlgXUMlXVkRFz3HjCzQtEpVGEQOrQ9gX3MVdod7ayq9yz-AsQbVaUfOZybLU99c5vfSc1sPIZtxi9is__6HeCorFmnXYGGcgAbA7MfbdiagW698Qm7IKMIz_1Z08vz5zT7c3Tqef8ayt3eiVo1uQZLI-MEHpK8_0qWl5UAHY9pe-RNI4NJSh09jjyQvC8mrxLyzuQ0s9u-BvELqd_Wm1vimUBS3sFtw5ph52u0JARxptVAWR_uSG3h7mkW-lQlt1S1FkmIgdpWDzhb7Is37G3JGKt2jajV2oh3xp4LH5LMoUfWGQi48ux_LWRDc1u6W7mvOzDiw6sr-RrVNlqRt30UNvV4wrC1DJqHy-u0ysnBctBuVEaKmAttJLlnp2JaMoEt73xhageb-ilTDnQDq4fEzKEotY3JoJ1lJtEQ7L0Rl5Yu3-fzGeJZy-8VTAxScqoz1ZHODFkKgHVOv9zFgQUbImI7_V3zI4JrWpPpUX6Tsz9iBfncR5E-z2iOpmBacrfA1vqN7Bil5ObFbmbdZ2K0WZFGXdgJ_tYHOCE4NsDWaqJvQcFajzfpEcAYBYsgeZczgPQkhODlGDa3Svw6Dpq3gYxmSEFCjlmDaHF7UUGSXA9Pn6dD_yG6Lcub9dRQWMMWzevkK_mfGrdu1X.lqX1OTQO0jr2vVbEAr_-Lw; _dd_s=rum=0&expire=1739569147785&logs=1&id=aaa3bd77-5a04-4f8f-8357-b29605dc27ca&created=1739567566801; __cflb=0H28vzvP5FJafnkHxisreRzMoYYPBHyXhoND4uXxajo; _puid=user-dSaz98KAcTZWMeK2wd2IE1BD:1739568247-meUhuduulaedEein3yiWh%2FJV%2BNb86eA21T%2B1S3An4C8%3D""",
    "dnt": "1",
    "oai-device-id": "50ed7d6c-fe11-47b4-88e0-f0077cc93e55",
    "oai-language": "en-US",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": "https://chatgpt.com/c/67afb1d8-4f4c-8003-b2f0-7cc854132eb6",
    "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Google Chrome\";v=\"132\"",
    "sec-ch-ua-arch": "\"x86\"",
    "sec-ch-ua-bitness": "\"64\"",
    "sec-ch-ua-full-version": "\"132.0.6834.197\"",
    "sec-ch-ua-full-version-list": "\"Not A(Brand\";v=\"8.0.0.0\", \"Chromium\";v=\"132.0.6834.197\", \"Google Chrome\";v=\"132.0.6834.197\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-model": "\"\"",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-ch-ua-platform-version": "\"19.0.0\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
}



    ###SCRIPT TO GATHER DATA AND RETURN AND SAVE AS A CSV FILE###
    def gather_gpt_conversations(self):
        """Returns a dataframe of archived GPT chats. Pass in your cookie and bearer token from OPENAI."""
        all_data = []  # This will store each conversation as a dict
        offset = 0
        limit = 100  

        while True:
            url = f"https://chatgpt.com/backend-api/conversations?offset={offset}&limit=100&order=updated&is_archived=true"
            response = requests.get(url, headers=self.headers)
            
            # If the response is not OK, log and wait before retrying.
            if response.status_code != 200:
                print(f"Received status code {response.status_code} at offset {offset}. Retrying in 10 seconds...")
                time.sleep(0.5)
                continue

            data = response.json()
            items = data.get("items", [])
            
            # If no items are returned, break out of the loop.
            if not items:
                print("No more items found. Exiting pagination.")
                break

            # Process each item and append to our list.
            for item in items:
                all_data.append({
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "created": item.get("create_time"),
                    "last_updated": item.get("update_time"),
                    "gizmo_id": item.get("gizmo_id"),
                    "workspace_id": item.get("workspace_id")
                })

            print(f"Fetched {len(items)} items from offset {offset}.")

            # Increase offset by the number of items requested (in this case, 20).
            offset += limit

            # Wait 10 seconds before the next API call.
            time.sleep(0.01)

        # Once all pages have been processed, build the final DataFrame.
        df = pd.DataFrame(all_data)
        print(df)

        # Optionally, save the DataFrame to a CSV file.
        df.to_csv("archived_gpt.csv", index=False)
        print("Data saved to final_dataframe.csv")

        return df

    async def archive_or_unarchive_chats(self, id, true_false:bool):
        """Select TRUE OR FALSE to archive or unarchive your GPT chats."""


  
        await self.opts.connect()

        query = f"""SELECT id from archived_gpt"""

        results = await self.opts.fetch(query)

        ids = [i.get('id') for i in results]

        for id in ids:

            url=f"https://chatgpt.com/backend-api/conversation/{id}"
            payload = {"is_archived":true_false}
            r = requests.patch(url, headers=self.headers, json=payload)


            return r.text



    async def fetch_page(self, cursor, query, session):
        url = f"https://chatgpt.com/backend-api/conversations/search?query={query}&cursor={cursor}"
        try:
            async with session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    return None  # Use None to signal no more valid data
                data = await response.json()
                items = data.get('items', [])
                conversation_id = [i.get('conversation_id') for i in items]
                current_node_id = [i.get('current_node_id') for i in items]
                title = [i.get('title') for i in items]
                update_time = [i.get('update_time') for i in items]
                payload = [i.get('payload') for i in items]
                message_id = [p.get('message_id') for p in payload]
                snippet = [p.get('snippet') for p in payload]
                return {
                    'conversation_id': conversation_id,
                    'current_node_id': current_node_id,
                    'title': title,
                    'update_time': update_time,
                    'message_id': message_id,
                    'snippet': snippet
                }
        except Exception as e:
            print(f"Error fetching cursor {cursor}: {e}")
            return None

    async def search_chats(self, query):
        all_search_results = []
        cursor = 0
        batch_size = 5  # number of concurrent requests per batch; adjust as needed

        async with aiohttp.ClientSession() as session:
            while True:
                tasks = []
                # Prepare a batch of tasks
                for _ in range(batch_size):
                    cursor += 20
                    tasks.append(self.fetch_page(cursor, query, session))
                # Run the batch concurrently
                results = await asyncio.gather(*tasks)
                
                # If any page returned None, assume there are no more pages (or an error occurred)
                if any(result is None for result in results):
                    # Optionally, add successful pages from this batch
                    for result in results:
                        if result is not None:
                            all_search_results.append(result)
                    break
                else:
                    all_search_results.extend(results)

        return pd.DataFrame(all_search_results)