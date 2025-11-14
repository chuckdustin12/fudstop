import requests
import asyncio
import pandas as pd
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
opts = PolygonOptions()
headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5MzQ0ZTY1LWJiYzktNDRkMS1hOWQwLWY5NTdiMDc5YmQwZSIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS92MSJdLCJjbGllbnRfaWQiOiJhcHBfWDh6WTZ2VzJwUTl0UjNkRTduSzFqTDVnSCIsImV4cCI6MTc0MjQwMTIzOCwiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS9hdXRoIjp7InVzZXJfaWQiOiJ1c2VyLWRTYXo5OEtBY1RaV01lSzJ3ZDJJRTFCRCJ9LCJodHRwczovL2FwaS5vcGVuYWkuY29tL21mYSI6eyJyZXF1aXJlZCI6InllcyJ9LCJodHRwczovL2FwaS5vcGVuYWkuY29tL3Byb2ZpbGUiOnsiZW1haWwiOiJjaHVja2R1c3RpbjEyQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlfSwiaWF0IjoxNzQxNTM3MjM3LCJpc3MiOiJodHRwczovL2F1dGgub3BlbmFpLmNvbSIsImp0aSI6IjQ0M2VjYmQwLWQ5ZWItNDY5Zi1hNGVhLTAzNzVmYWM5MTNkMCIsIm5iZiI6MTc0MTUzNzIzNywicHdkX2F1dGhfdGltZSI6MTczODkwMDYwNzU0Nywic2NwIjpbIm9wZW5pZCIsImVtYWlsIiwicHJvZmlsZSIsIm9mZmxpbmVfYWNjZXNzIiwibW9kZWwucmVxdWVzdCIsIm1vZGVsLnJlYWQiLCJvcmdhbml6YXRpb24ucmVhZCIsIm9yZ2FuaXphdGlvbi53cml0ZSJdLCJzZXNzaW9uX2lkIjoiYXV0aHNlc3NfM0NwQ2JjQVFmd2U3eEF0ZTB1ZTdEVmNOIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDU1ODc0Nzc1MDc0MTQzNDY1ODIifQ.X-QL7hVkZSo-T2G6pK6WzBJ5NKTZ-szy6EFDlGWM-Fcupm4zJ7rvtK590z6OIcRFxCnGIMUR8Ba0Z0rCXfyHKk2mBc1Hjao4RIBKJ9gjmEBwriblYZ9KqFo7U261G9v9AO0t3q-HIGwKKY1J51E-KNpT8QCr4F4LDg5sAy06yyU7JAmmnIG7WIPR-YAl-En1y91gnBcUJ1LR4dd8389bvF8BRxdT7WgxRGuU-0pUMb6iq0LpcKaADk_43CzdkNWAGuxXQlT-iXWZUdgA1gEwjRfJ_rVLMBu_pcg2STD1lDDuejKwecs6nTnEVMM36Qki-RdrXJNA691FGAKutCGRW8_A1NCzFVIexD0O7fCbRu2r7cRa1C0l4nEhvy-FbXjpeRTuouatRnOeHu6JjNlAiekDlvlg51mLY4O9JmKRM28TvKsIfhuDrIns2ERn6ynmRj2Dcq34-wCOvw-SwFUdVmpNNXe5WUEYMPruV0jDdOOSF4Dt0kB-C1iZsRYplmsleLiP9IO8whBy2PdqV3iX9vIrpDSi5wtP-5WwYutyagU_UtiLPCjX9yDo5FTKeI6BEuO6rzVDWhODBCL3vEdZuBw3Er3aqXJqF8oJoHm_b62JvYKqvG9FVOmaiSeRrC4kIn5s8vMlSowJagKAltyQeZf_uxFmVe7aUcfDqUmSAwA",
    "cache-control": "no-cache",
    "cookie": """oai-did=50ed7d6c-fe11-47b4-88e0-f0077cc93e55; oai-hlib=true; oai-sc=0gAAAAABnkWV-lmZuTz_MgC5JoBajQ1e3zFtNxiFrTcUDqyQV1OzKhFmksQVyMLYrpNNMs2wkaWBxgOHDLluPyxBr_nook0D4bgYljLP5_svG1VlqnkdNap1KrSX5TWNUxITNUPgQz7NfiqnJ3upter-dzCiCnS_ZxmAzftvmYD6ineUcXeG8iiufbVLmEUO2FuLKRcmNvHNYUSbEQVEGkcFA6P0pkLKx5b3n8g99n2ZqcG_Ebae-c1o; oai-sh-c-i=679c5a62-5670-800f-a58b-851fdb582ed7; oai-locale=en-US; _account=personal; __Host-next-auth.csrf-token=ed37d2eb504d0541ec144167c39bc2cc8fa44698b58ea325945e70fa95cb2cc1%7Cb3c0484e31b95ca0c6fef3e76c282cd7968a87f9454124c36fb1f4d9b1451e8d; __Secure-next-auth.callback-url=https%3A%2F%2Fchatgpt.com; oai-thread-sidebar=%7B%22isOpen%22%3Atrue%7D; oai-nav-state=1; oai-last-model=o1-pro; __cflb=0H28vzvP5FJafnkHxj4MG7JYrJ3L8nFoFvsVfMuZWWw; _cfuvid=zoRd8dWbvQ3zhgFmeaFrtS3Jn4rdnz9DaVJDpiP6A.c-1741853191679-0.0.1.1-604800000; _puid=user-dSaz98KAcTZWMeK2wd2IE1BD:1741854333-8TqRi0G98KgDmccL0lDiGgUJwrd4qhr2tR8E4CGGRww%3D; cf_clearance=HIxgLTwjx3jnnBOhmmt9ZAfayBuGcTPxcLWjd0wK2YA-1741854333-1.2.1.1-BGVcU3QDtMc51CAhuRmEw.pQvX5va2XiqlEwPYJuu8ESDQ4rXLi3sK2sy9coHk7zOy76qGHr4LWpEQGdVBS1PMTMIqpyJCsMcX36W7yIsECTm_YYdWVRNLqQ.F9De7WA9pGoWyEhwtZbLy2qklxH1LYOGJdXnDeNMFVCrJuRXpXeMDeSs64uJ9euV9cB7zmc1IjnB7mz9g7is8oQVPF6k35o0HHU4CeDwSh3B88evpLHwgpw0N.aEtQ1bdMlIpdu_4jsGY7ZjgTl_jBFYsZ6eDFAv4KJBGb4KHnUNnzrPAiJHN1AfaGq.ovDszWHFVybywKzUVcMosmEJUksHBZKGjNiKsar_ufFMHZq60XRvRA; oai-sc=0gAAAAABn0py4BqhzHXuvwUuH7MjfmDEK18VQUH6ipI9zc1vXK_W4hrsZ7cd3spwv07NPs-SyjaUmVqPzImUeMGzKTgfQLjfDHPw3xlUxa7z5TZxiwxYNbVhwQqHThbPpXaB7p8AWO-RAdEl8YO6guJTWuTnVgyb9w8H815TrKEUYODnEhYJzdB9nuM-WfFqRuKPKJC-f5o7mbFhyeUI2lvxyKS8UPIJQp-HPKM9rw6fjv0GwsAICXX4; _uasid="Z0FBQUFBQm4wcHk1NnE5d1pScmE0aWtzTTAtcWthYWpuWG8xLUxhSUVuSENMZzB4a0J0ajNEUWdCWnhCSXBySVVBNmJ5cW5PR0ppUzN1OU1NX3pxV3R1eWUteEQ3T3RfejJBcHo5S3I2WjJDNml4N0RqR0RUc3hfUE5NZTBMNGxQVWxJNFBidjNrQThLeGJ0c2VwOHQtZWN2dEQxSnRwY2pPdTBLYUw4M3l6VjJEVG1KNUxiYXlGVGR6SjRyOGVsUkdfTnhCMVpkTlpZZXNsWHlSaWJ3SXFYejl3eHZiMVp2a09JT284Tkd0OE5FbmJVZngxS1otWV9uY0JOYWNYSFpoMkY3TEY1VV80Vk9nc2Y1elhUeHZUYnByeFppVVNGTjBweU1KMWREcEpNQnpXUGNjaVowSENIRUgxY05SbkNySkNnSTA4WEp1TExjOGpNeWdtRV9QcEV4OEVWY3ViMHhRPT0="; _umsid="Z0FBQUFBQm4wcHk1bGVqY25rY0tpS012QnBwWFE2YzZ1R19ZVGVoRXR6SkNXbnY3dHJXQlFfUzQ2Rm9FVERGakcxQlFSV0FJLU40QmpjS2NzWUJSUmc1TWkyeHlHZjBkVzNaZFEwME91WW1tdWMtdkdaWDJiQTZTOEpKM2s1eWR3Z3hHN05DQXdsMklmWl9Nd2MwR2YyRl9FX1NwbFl3Qkx1VWl1bHBIcVBGbmF4bDgzR1hKRVFTYWpnVWtJbkFBeUVzdW4yWnZweHpNcnlLUW82cHZiQWhyQVVvMURwSS0yRWQyaU93ejlpalhtODNsbnBSbXRaQT0="; __cf_bm=qbkkWtOlQqh0WutGXCRERnGgQYfWAnhqvovk_YkjnN4-1741856078-1.0.1.1-_WpCQo8BdbCuM3FxIbhnGVwSqruKnuPGkc3QChcfMwUwOmbWeVbDVYA_Z6JFUC8lr91irA3NCm3BKVYU2_EB72rdc5of_C._FWg_vz615BQ; __Secure-next-auth.session-token=eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..mDowJqDd-JjHUGEY.F41Mv2CD3j4PE8_CrV4ttulriTpScFDoHrKV65xbAYe7_MdWiqVIG1n2TEx6N_nb7dR4Y-ZNr-0aV3-IMVHpsstl49qt9dUBPOSl0iSsNaNcQyxnoGTN9DFrpMm3Lw15R33khhqcoU8oLH0Qdd_D-36mUjrEaQS_pW8OW7ozS_12uoAATGrW1AK33QroXnyyTU0rNy3hqs3ca4mLTe_FoKO5OAS5xcisxBsp83CDoRijmflq6Oyzk_ByC8Z-FVajgj51JQ0WpK3LMYz8dPjk3wEoYg4KnpxJ8vFtBRSzrOuVvggTCdviUycJqfK6hWq8NZG5E-O9K5_AV43CHIB4F4M-sfHe1GpVINdMUtU5NiFhC-SH8yWcNwuiSztPWkcvL1vRLmJLaEn9pVtCyf45xe4BYc9aLOhN8rsFRtqatxZmm6ooJSATlqhWTXhWejuaDlotYXXmmenKXWj3C_JhrvfYEIDvySgeSpnZPVl5DU3xYXPCxVPA-mtfAG_HiL-bJyX32Ik6Uhb2kF0YyONnXP7j_loVldxGi7xpW7cVkXC7L7XTkYgQ4MCHp_e7WK32mQSTkrDSK89ay-6xETXGh0PbYEkiR0pX931O6Zvjlg62am9ejvb9x7XRRwNg54nYgIXlaKUxhuy39PCxhY5tziaa2FnJh0SYqBnze9bNr8yIXR3rZCj6f5Ba1Pnf-D0CSsSll7Y40vbip7nUFrdBWBATT14ATUNZIqiVYvUM5Lp5LVi7LROHlj50styZIYbyiJqoK5FNAKQkooOCQ-2wUR60lsulXEKqznousBcnKcluQnxaxnfvjPex3Jb4yvVKdANgX0mgqnhy-oTjmjysYJYcFyAUCKpsnV-LDcWt_2HeToXOM3hTICn5K8MGppHxpsQfKuK8EbUECXHPfYOmb_E9dz1tWJo6-O0ArSxysGT5GCvm7zPbOTT6K5rC8RvaO-goQJU08gKyeQQ-JluyRkquLM-FtDoRziYJSmfLkL_sEP0-21O-BPhN3cdr7lC6WuWpY-3iJgP-Z0icf3KxcKk21bPKjm5L7RVcY6FdJWWyDZFRLAJN1r73bKAYtjUVUiMRH06Q13HPBCn-L8retj783zk9sNnPkec8OgRF11e33y2qshzto7zTl7aR8w0jEWvHDRClw0iH3HRMB3lcEFhHRWLNA08MZNso6g0ElrGJwPzhrbrhvq5LBTR9UKotAZKRcUn5vReojAFZYwU-s55dIItiieVD_hurAmsu6ntA2lmhs163pTVu9F_noWA67hVY-DOWd7d02Yrsh9yieFR2ypVjVpTAttUiZ0fhsM3EswW15GMzD69dAc4hsYFAnHwCewMZP5VtRcfbxI40sCNKdR_IeGkBRAZRIgXEAVhB3FJq-5EJLooF_oBxo83fBgEkSMqXeAfpYDVHkD9WFhRwGZ94nyax34LI9lvZv_qnfxxuSJmW25XrRYJnQsi_PVCWxhIY39FtSSKMucsZP8zZw9IQ2RsAazT0eASZZUQFoznRgwfTZ6y7WgfgaIObiA1N4_Da6bITLVypfPYdg5Isv1EvsjTEahacuckWJi1K0TyULz2JXIOaXZov0WaDzX3GSk5c7dOd5lXnKNB9EKkjzKVR6xNBGv6-jYY93vGgvQ_OZSF8gZFaocboigQz79wENYH6GLacyBgAX_YYD-Sz7dr9LKJaSF--iguEVL11vaYafcR-yj7Hbp434ffAYT2YsHqtERu14-2CI91qdLv2nf7koQw1fqiYQaib08WMF5mMfs6UbIEZO1-TDo8UfpAq54MopwuUW4NZl8hlDX1wGW2NhxBEs-_G_Qd3BpQbFvzT2STKPn5E1jCn7BQQfTrFowo2LidTNLG4Dsh3iFLQb9sDzYHqS_P73xFZM0jVBtqfMcYuC-s-21Q0KndRclTjX7S-qynSfNPsDHC1HCEN_Dx_5CrPcrl7ONBu05kTfor4j1jP3UeC4KtclEkHR64M_ijuJgt6Idq3fZDJbKqzHlqepptXScjBP-fhFH7MW2GkmS4iSIAbywSsrg4Zflpp9D-ItXV7DTLjWA7p9Lq8hpgLb1hw6S_Io61G6zSQ5mYEUJn9rQefpBj-Lp1_uJ8fUgRqoohnH0AbMX31tpxqzo1wZECpp1ai_RXbrnZrUyqrVDx9Ave7XioEm7JOU46N4BQzRlFchJokYXF5uK2f-R_hsEYcZSNYtZVw1hn1zdfGnZtlaoZgWRI9mUJb2EocRFBxgTw_i2xz3ncFsd2UEsFIZcCt4PX8wQ4SBbVCPXGb9M0Y71mCTiWrxbdD9ZGB7gf62NqytV9eSogKUw6eH9XwkbofGnhPBoJidbn_NViW5Hb8OTtdz2xws2_oo6HeLwxG5iRxMJ5LQVs6-Y6lf9IjIB5Q4w8sXSEfdsGdNZ3eJByuFRaEmUxPYhvPvaZR4pWAzYx-oeJJfTZllzH_3KF3AAusiPTwETeeMEC6JerJ25vwhcD8WunCtBvQlbV1N0_LLJMkFgA0GB5p0-_81RWwYXgsknKFBU3VKm8Gwa_9p8wlLh7SiQoteDkq4MUbiRo0TzCd87GLdbyzOV0UWT2Z9G14xS3ZTVRiv53e652vUSF9WVtouEd2ulkJTG7q5SSebzRzqGqQv32K3l0pPR4gKY4Sujx0PMaVHKXkahpTmZ_zmca6S5i2lur9FMD4xl2Ret2dteTgYVW7ETw2plX5sXdZVjHd7cvcQzs9jzTwHhVxUcpI3bwZwSJeUaLhU0xS2VcA8TpNlqMTHh91jce8cq8c1aI8JlxZtmRkl_cX6KW4nnWKC9eMrrAigRAm0DqrjD2lm4QLXNEaC1ru_ScA3ZLlSyl1mbb-4rKCTAegYB6NZB1u_pIoj3BbC_xeYjmGf1Me-B5WemUg27sS0KwFeV94-LTPxd74IjgPg-Oo_VvTC3L2g6Xr5idrZhZ_wlBSuLr1PbS3.PoaRGulivZuIEgAtmRXVQw; _dd_s=rum=0&expire=1741857107065&logs=1&id=d406f959-9aab-4b76-b6b0-6c945c728b3a&created=1741855793864""",
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



async def main():
    await opts.connect()
    r = requests.get("https://chatgpt.com/backend-api/conversation/67d12f5e-d628-8003-8296-a918645538bf", headers=headers).json()
    # Extract mapping data
    mapping = r.get("mapping", {})

    # Iterate through all the IDs in mapping
    for key, value in mapping.items():print(f"🔹 Key (ID): {key}")  # Print the main ID

    #for i in value[0]:print(i)

    message = value['message'] if 'message' in value else None


    metadata = message['metadata']

    content_references = metadata['content_references']
    citations = metadata['citations']



    metadata = [i.get('metadata') for i in citations]

    print(metadata)
    df = pd.DataFrame(metadata)
    if 'extra' in df.columns:
        df = df.drop(columns=['extra'])
    print(df)
    await opts.batch_upsert_dataframe(df, table_name='deep_research', unique_columns=['url'])


asyncio.run(main())