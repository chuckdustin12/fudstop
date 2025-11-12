from datetime import datetime
from zoneinfo import ZoneInfo

# Convert system time (UTC or local) to Central Time
central_time = datetime.now(ZoneInfo("America/New_York")
)

# Format it
formatted_time = central_time.strftime("%Y-%m-%d %H:%M:%S %Z")
print("Current Central Time:", formatted_time)

import asyncio
import aiohttp
from fudstop4.apis.helpers import generate_webull_headers
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
import pandas as pd
opts = PolygonOptions()


import requests


SIGNAL_HOOKS={"price💲cross":"https://discord.com/api/webhooks/1350875202900197500/0E9My9S_P1krxO0OB3oCVunretViiBDS3bec_3HBIvpJ6szkFx5WaQOjj27R1An-d9sM",
"williams🎯r":"https://discord.com/api/webhooks/1350875208365637682/_SXJoFKwjRzEz5RzdyaZyyVMDZNiWgT6D-ehALob3UyP3cqwxqqvNBhW3DdG58uQvD5m",
"double📊ma":"https://discord.com/api/webhooks/1350875213960712285/NYOreWj4f-J_IL-PDK02cBv6kfXo-CdWpXtXCgsKmOm1qAXlRcgQPgv_vfWlvrL_zPr5",
"triple📊ma":"https://discord.com/api/webhooks/1350875219601915944/CAawA5FLi8zrzTZDd3b8F56wCxJPXTbN71ELorphS7kF7lXtb2bxPS0MvlAceQzgQHYJ",
"momentum⚡":"https://discord.com/api/webhooks/1350875225146789959/Jh-mgyUb0K5umG2d1LVndfDk-0me7spVpXz8QJgun9NNEmGDkwNcAeKdRBn7hoS0MYxj",
"short📉kst":"https://discord.com/api/webhooks/1350875231476256869/7-ix6Y349n9VBAwRuqJZjR4OlWqTP1lq-F9PpUXOb0wee3aykqTjgKYH5ARehF8w2NQq",
"long📈kst":"https://discord.com/api/webhooks/1350875236656091260/f-TwIHrUrd7tv0WK_35squ2hKYaqBuGfuzoVPjPVZKXvZNp66WsdQe2AoFWy91RSVc2d",
"slow🐢stoch":"https://discord.com/api/webhooks/1350875247678849044/WRx755PL19jY8NUIY1d1VeKjXsUYO8MsgrkxjbE4pNdzicefKQ9cw1neqQjLZAMUxkPp",
"interm📉kst":"https://discord.com/api/webhooks/1350875252636385290/1tsfxHAz1MfXy7hU8tnV1qxa92O8TZlP_Qh15upfXfBgfjxpe86l4NKJhWm58YWTyvsY",
"hs🗿top":"https://discord.com/api/webhooks/1350875257086672926/EQEAYu_Jc0-T1Z4ULJvRBSTKPn5xqCMwsotzSGBbwCabbOS5szgO-1_hSSMcLJi1z1qC",
"macd📉":"https://discord.com/api/webhooks/1350875261511663637/J1WqBZqBBkMQcfcjaep5CPvE06VgERx-ucerlUH-gI7jdcQ8EZ-JQVxRPI5KrmXhF8nM",
"fast🚀stoch":"https://discord.com/api/webhooks/1350875266427260989/zL6Vv8fSTuqA7Fgw54fIj5nkFfrpTFbBckhHzFd-SGugpGa7A0TwWwchq4RaL5NRf_5Y",
"cci📊":"https://discord.com/api/webhooks/1351314744350474250/5mO67Nrs9hlbEEVe0RilGpOWSnWbihguV8RyRpcXQYpvwfuyoMlOTf8fGjTbe-Us82Gm",
"megaphone📢top": "https://discord.com/api/webhooks/1351316554427666573/RmWCQd3wp3FEUQRj8ailxxI_ci15XGOKTtWcRpVtP-yM5zoakXTRwfVOFvHPFHwiqWbG",
"megaphone📢bottom":"https://discord.com/api/webhooks/1351320235373760633/cTws8a0Cc98JIe-pvrI0Es8rWFdDJ2sQi3_LY2dWISu4WW2ujPcf-_FGs6M64UIekZC4",
"rounded🔵top": "https://discord.com/api/webhooks/1351316549230661763/TqMiinpNdb2rYULVw5qTgEx8BQRm2qzntgag0LovsGPlCk1tvz_CCh0lwN6RqE_OITv2",
"rounded🔵bottom": "https://discord.com/api/webhooks/1351652433813700678/C_WdTzjLFcx9kLH7e1Qg41UAxOlp-2aqA4S93SRPupz5DRTHTGd1uydHNiQWL1f0WoXb",
"rsi📊": "https://discord.com/api/webhooks/1351317456538435595/YZLWsq3bReS9Uv2OH1oM4T8UTKoDIQZdj7OzGdUYyJZR1yHgxRAU6kPDczKNSo7RD1g9",
"triangle🔺bullish": "https://discord.com/api/webhooks/1351317463945580666/Ks7OjXg3kLw-CgyGGvVZxsUzRwgOtN7Z_WW3E9uPspLTvGn8LyAp34h5gCazXAd1yKvR",
"triangle🔻bearish": "https://discord.com/api/webhooks/1351317904745959484/P4lOWYJdNjcRmfRSQydT0UtGUiz_Xh3YTXhox6gXBlk_enfm7J7CFa7yNHxeqIuWCnuo",
"hs🗿bottom":"https://discord.com/api/webhooks/1351320225626456135/X5q59e7RjE9nmOkIg9-PHq0EBreaFAx-s3MtNxRu_GbHYzwWP9rSMZcb5h7Yy2KaFlvI",
"ascending🪽triangle":"https://discord.com/api/webhooks/1351320230558826557/PevREgn74G01x5SkFdHDVsfBeFUj_alx40yYwbaq9FacS57tmxVnA22FPQT2LVS2rlqO",
"bottom🧀wedge":"https://discord.com/api/webhooks/1351320240126165123/NlIpKcZFu2TArfH2STIxM_p0YJU_LM_iTF1UTEbwSc7Dc8fVtUiHng5LI0TFFynFGtlf",
"diamond🐂bullish":"https://discord.com/api/webhooks/1351320245339685055/TLBeHSjeyi4hwI9kR7NqcbW9VWxVE7o2rxjZ6x9P8QxGGufLxezUlVomx3KWP9NdN7e_",
"upside🚀breakout":"https://discord.com/api/webhooks/1351320250095767665/YqGA74d9kNNCxhW5E8crTBGeH7K6em-90V3kmWa6QoCrg4IH-fyjby6FOp8DzfXelQ6J",
"downside🩸breakout":"https://discord.com/api/webhooks/1351652808432287835/9iyY9EvQ2ftQLVEouODPeyXeul_4YqayGyVKDAaywQRSaGQpIzKSCZFe5i9X90YQZOj8",
"top🧀wedge":"https//discord.com/api/webhooks/1351320255506681886/vIwywiiP4O6dssO1tHUydPh_pSEuhcTHOjLD4kn59_12GVXROjbt8vXQeVo2EcWQ1yLX",
"descending🪽triangle":"https://discord.com/api/webhooks/1351320260246245456/289hQoP-bBPrRGtFRSTSqnlxx_HzPpp4au0dKAcARZw3SSrt7kPUu75tk0nUd7Tuef-t",
"diamond🐻bearish":"https://discord.com/api/webhooks/1351320265182937179/ET0ZFa60rs82YydXTBwKErNi8Znmk4FEP1g3RWUrm9AiDI9wO6iPkS6L0-ui-ImpVwwI",
"inside📈bull": "https://discord.com/api/webhooks/1351323242115235912/iI0Fbn18gAS8CA2cxQrOZJdD2nop62hxf6nxzP5ncqL9MNaHbwHdGVpMf8vHaCBRalmP",
"inside📉bear": "https://discord.com/api/webhooks/1351323727828226088/Ep5oJ128TUzs7lpmurlnesvuOHH4fxMUHMoeuDMH66k1rya6pjHg-Q1Ty9ndWpI6FBNh",
"double🥈bottom": "https://discord.com/api/webhooks/1351323247203061850/OxJ9_PazDqbhCmOvqkBQYfLROKm4s3lEgfMWa43dJUP7ASIj32HGvroRAaGcNj4bf1pq",
"double🥈top":"https://discord.com/api/webhooks/1351651104500486194/8qBjfKEe7gASHf0tP_Woe427M4gGugNP-CnshpqdI-X-IQGXRmyoWzhMB9E0o4A45cIe",
"triple🥉top":"https://discord.com/api/webhooks/1351651677349875712/74Njn7HUNsOqlM2OSE2eSiZg06yS5OKij595DKNiAPm0cUB5yTG23doULbDyeWSgUW0V",
"triple🥉bottom":"https://discord.com/api/webhooks/1351651869390278676/Ln9VXRJUBwqI_u4yih3Ho1ywV7C2_avBLgs1nQYPx1REwHu6o_6XrKTdKxokSJh763bk",
"hammer🔨": "https://discord.com/api/webhooks/1351653134878904383/bgzIIi_3MrRW8vIznJ91jFQjgaO1hlzNIIyHoAk-3NJfWTP7YiSv-K0oFBF5-Eu42vnF",
"outside🔳bearish": "https://discord.com/api/webhooks/1352669525363130438/qqvjkgwhGcyF-5c6Rro3nvYXvCYVLT_xPImGmUHgjJMwc_weH9x1nD-BL9trlyN47r3E",
"outside🔳bullish": "https://discord.com/api/webhooks/1352672052292882534/K-KHJwY2zQC0SRo3a20CfzUBHc2YW428F4CjfBySjWpIEMT1QbblRufDbydrub8BsaRZ",
"diamond🔹bottom": "https://discord.com/api/webhooks/1352669881396494357/DsbiH92zK316yfAReZbXSaYekj_iz2VK6e0RatrpfGNwz6wVrj4qnzEVz5PyoTzQcAFT",
"diamond🔹top": "https://discord.com/api/webhooks/1352670284443811940/75GzP1_xHooXDGTNVguYcFapCmfLrLmBUwfrK6V_NGI1d1bMkiCPia0ardJ1P4wUun-M",
"bear📭flag": "https://discord.com/api/webhooks/1352671474053414932/mq9xuH3LC-M1Iua6YlZgPfsjXK96oaKYbrDFbG8NZGUS4_vvgaNiM70Aio-Los_Ejvcg",
"bull📫flag": "https://discord.com/api/webhooks/1352671670212755508/NejLwEF0qykOjQ9_RataYd-OeS5vz_HJSNZW2bh1XPUeISHpqG6rTawArD-iqbXlZhaK",
"bull🐂engulf": "https://discord.com/api/webhooks/1352672490006253719/2DCuEeB2NsPXkwxXyEw3fLVNkXyMKyrmBu2fDe_ZsB3AKQLqxY8XQwzvOY3Q9ZcF4EvF",
"bear🐻engulf": "https://discord.com/api/webhooks/1352672723918258246/Jfof09_nFW0SCF5BxaTc76YMVi33yZGoxUucVDq2U8MxWWRaVmFqBuK35JDu7xaLP5IO",
"inverted": "https://discord.com/api/webhooks/1352673100784730142/R8aBBHYFOipKBqel9pqMN_q1mZeqlRej3tZ5JlvyIEGP4dlxmJjm6f7iTHVCZOR5Q2TF",
"hanging🪂man": "https://discord.com/api/webhooks/1352673185597882532/96VZawcpWep7nOrIFB_rj2o3aZhBJ5Dl6YshXEB0cnUCmTY8gHeyEauin0noJDNDf881",
"inverted🪼hammer": "https://discord.com/api/webhooks/1352673100784730142/R8aBBHYFOipKBqel9pqMN_q1mZeqlRej3tZ5JlvyIEGP4dlxmJjm6f7iTHVCZOR5Q2TF",
"twobar🍫bull": "https://discord.com/api/webhooks/1352674106658783382/leqeKstMxZZ7h7W-7LN_khXahH651yAbfC7dZBs_tKZJmsIg71bHydb7wjr5E88kVGQs",
"twobar🍫bear": "https://discord.com/api/webhooks/1352674017214988340/NFsQNYe026aoNjF8WK_O4v2Ya50vu0zDhd4t-Z3y9dZNINpdNVhZYkHoG6yREx3nLL95",
}


indicator_abbreviations = {
    "Short_term_KST": "short📉kst",
    "Long_term_KST": "long📈kst",
    "Triple_Moving_Average_Crossover": "triple📊ma",
    "Price_Crosses_Moving_Average": "price💲cross",
    "Momentum": "momentum⚡",
    "Double_Moving_Average_Crossover": "double📊ma",
    "Intermediate_term_KST": "interm📉kst",
    "Slow_Stochastic": "slow🐢stoch",
    "Moving_Average_Convergence_Divergence_(MACD)": "macd📉",
    "Fast_Stochastic": "fast🚀stoch",
    "Head_and_Shoulders_Bottom": "hs🗿bottom",
    "Head_and_Shoulders_Top": "hs🗿top",
    "Williams_%R": "williams🎯r",
    "Upside_Breakout": "upside🚀breakout",
    "Downside_Breakout": "downside🩸breakout",
    "Continuation_Diamond_(Bullish)": "diamond🐂bullish",
    "Continuation_Diamond_(Bearish)": "diamond🐻bearish",
    
    "Commodity_Channel_Index_(CCI)": "cci📊",
    "Megaphone_Top": "megaphone📢top",
    "Megaphone_Bottom":"megaphone📢bottom",
    "Rounded_Top": "rounded🔵top",
    "Rounded_Bottom":  "rounded🔵bottom",
    "Relative_Strength_Index_(RSI)": "rsi📊",
    "Symmetrical_Continuation_Triangle_(Bullish)": "triangle🔺bullish",
    "Symmetrical_Continuation_Triangle_(Bearish)": "triangle🔻bearish",
    "Bottom_Triangle___Bottom_Wedge": 'bottom🧀wedge',
    "Top_Triangle___Top_Wedge": 'top🧀wedge',
    "Ascending_Continuation_Triangle": "ascending🪽triangle",
    "Descending_Continuation_Triangle": "descending🪽triangle",
    "Double_Bottom": "double🥈bottom",
    "Double_Top": "double🥈top",
    "Triple_Top": "triple🥉top",
    "Double_Bottom": "double🥈bottom",
    "Triple_Bottom": "triple🥉bottom",
    "Pennant_(Bearish)": "pennant📉bearish",
    "Pennant_(Bullish)": "pennant📈bullish",
    "Inside_Bar_(Bullish)": "inside📈bull",
    "Inside_Bar_(Bearish)": "inside📉bear",
    "Continuation_Wedge_(Bullish)": "contuation🥠bull",
    "Continuation_Wedge_(Bearish)": "continuation🥠bear",
    "Hammer": "hammer🔨",
    "Outside_Bar_(Bearish)": "outside🔳bearish",
    "Outside_Bar_(Bullish)": "outside🔳bullish",
    "Diamond_Bottom": "diamond🔹bottom",
    "Diamond_Top":"diamond🔹top",
    "Shooting_Star":"shooting💫star",
    "Engulfing_Line_(Bullish)": "bull🐂engulf",
    "Engulfing_Line_(Bearish)": "bear🐻engulf",
    "Flag_(Bearish)": "bear📭flag",
    "Flag_(Bullish)": "bull📫flag",
    "Hanging_Man": "hanging🪂man",
    "Inverted_Hammer": "inverted🪼hammer",
    "Two_Bar_Reversal_(Bullish)": "twobar🍫bull",
    "Two_Bar_Reversal_(Bearish)": "twobar🍫bear",
    
    


}



image_dict ={ 
    "Head_and_Shoulders_Top":'https://pre-social-video.webullbroker.com/us/office/98b1cd5c4ec04cfbacd6c9ac3c82b3c6.gif',
    "Head_and_Shoulders_Bottom": "https://pre-social-video.webullbroker.com/us/office/e1255ee1bded47dfb1ad5b149e5ae357.gif",
    'Double_Moving_Average_Crossover': 'https://u1sweb.webullfinance.com/suggestion/17aff3cf388541ba8417862b09d7a176.gif',
    'Triple_Moving_Average_Crossover': 'https://u1sweb.webullfinance.com/suggestion/1902a4d52de74a4bb5a9b4b3badd3a2a.gif',
    'Slow_Stochastic': 'https://pre-social-video.webullbroker.com/us/office/fafd3076f18f40789ed55dfc473d22ce.gif',
    'Price_Crosses_Moving_Average': 'https://u1sweb.webullfinance.com/suggestion/15ca404b7b1b40788fac11747d048edd.gif',
    'Fast_Stochastic': 'https://pre-social-video.webullbroker.com/us/office/961af9fdf8a348948617dddac0a5382b.gif',
    'Moving_Average_Convergence_Divergence_(MACD)': 'https://u1sweb.webullfinance.com/social/ad633d95b6e94684b038919a0ab0a5ef.png',
    "Rounded_Top": "https://pre-social-video.webullbroker.com/us/office/533f3e4bbe2e414a93f8e99a1801657e.gif",
    "Rounded_Bottom": "https://pre-social-video.webullbroker.com/us/office/6614b983f9cc43ebb7f50cc5ff476519.gif",
    "Double_Top": "https://pre-social-video.webullbroker.com/us/office/5932f20b1c214223877b05851585ab29.gif",
    "Triple_Top": "https://pre-social-video.webullbroker.com/us/office/08c735d66b114ed9804ccb10303e3438.gif",
    "Double_Bottom": "https://pre-social-video.webullbroker.com/us/office/10001a318a80438184f53f87838d9786.gif",
    "Triple_Bottom": "https://pre-social-video.webullbroker.com/us/office/add50eee33a54f2384578344b3b7ec15.gif",
    "Pennant_(Bearish)": "https://pre-social-video.webullbroker.com/us/office/32694d0d2a2e423c82fcfa211e935f88.gif",
    "Pennant_(Bullish)": "https://pre-social-video.webullbroker.com/us/office/5d4dab3f68ac464bbb35961dc6182edd.gif",
    "Diamond_Top": "https://media.discordapp.net/attachments/1323529995628318761/1323529995812999242/a328d0c954fe465ab8a22eeb19f15b67.gif?ex=67d9b39d&is=67d8621d&hm=6f43bcdd48c1a0f26e82b7b8dc5cb0abba5d7eef2d4470045e508ca07ef37e11&=&width=251&height=251",
    "Top_Triangle__Top_Wedge": "https://cdn.discordapp.com/attachments/1323530950860083362/1323530951409668148/abbe73257fbe460e97cc9cf7fc1d72e7.gif?ex=67d9b480&is=67d86300&hm=d4a36a7179ea7b88a6e7c1e4a9274dfa75175ba48f74aa6091f995378fec062a&width=251&height=251",
    "Bottom_Triangle__Bottom_Wedge": "https://media.discordapp.net/attachments/1323533375411060786/1323533375885021184/8696e1d0602b45c18d9dab078874e5df.gif?ex=67d9b6c2&is=67d86542&hm=aa9026d5acc13a1ad9bfb00897fb5496c76114a132f2230239d556128df0fa63&=&width=251&height=251",
    "Downside_Breakout": "https://cdn.discordapp.com/attachments/1323522524071333989/1323522524226650283/0ae44b8c686e4d6d8e2332d8f18c86f2.gif?ex=67d9aca7&is=67d85b27&hm=47cd03efc7b5c4d666509673f00d018b5d1fac56ccd1697171467d2a48fd539c&width=251&height=251",
    "Inside_Bar_(Bullish)": "https://media.discordapp.net/attachments/1323518606314831922/1323518606511837244/bdf7f26d2a2a4797b286587eeeaa306f.gif?ex=67d9a901&is=67d85781&hm=1b9ec5443d9fefc67b551a60955a3273078fc381cccd5ae5bad7ccce2d391e92&=&width=251&height=251",
    "Upside_Breakout": "https://media.discordapp.net/attachments/1323512543494275072/1323512543641079848/4d2005ffc7a94476971d70f3dcfa6060.gif?ex=67d9a35c&is=67d851dc&hm=0a691eaba90e1b3f806f0603986ddb7c7d766614b8185b87e60060cd03e818ef&=&width=251&height=251",
    



}





sentiment_dict = { 
    1: 'bullish',
    2: 'bearish'
}

sentiment_list = [1,2]
time_horizon_list = ['tcShort', 'tcMiddle', 'tcLong']



import asyncio
import pandas as pd
import os

# Make sure opts is defined somewhere with `connect()` and `fetch()` methods

async def main():
    await opts.connect()
    
    query = """
    SELECT node_id, content 
    FROM all_gpt_messages 
    WHERE author_name = 'research_kickoff_tool'
    """

    results = await opts.fetch(query)

    df = pd.DataFrame(results, columns=['node_id', 'content'])

    # Create a directory to store the files
    os.makedirs("gpt_messages", exist_ok=True)

    for i, row in df.iterrows():
        node_id = row['node_id']
        content = row['content'].strip()

        # Sanitize node_id for filename
        safe_id = "".join(c for c in node_id if c.isalnum() or c in ('-', '_'))

        file_path = os.path.join("gpt_messages", f"message_{safe_id}.txt")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

asyncio.run(main())