from fudstop4.apis.webull.webull_trading import WebullTrading


trading = WebullTrading()

# from fudstop4.apis.polygonio.mapping import CRYPTO_WEBHOOKS
# import asyncio
# import pandas as pd

# import asyncio
# from typing import Any, Dict, Optional, Tuple

# # Assumes you have:
# # - CRYPTO_WEBHOOKS: Dict[str, Any]
# # - trading.search_ticker: async function taking symbol -> dict | None

# async def fetch_ticker(symbol_raw: str, *, max_retries: int = 2, delay: float = 0.5) -> Tuple[str, Optional[Dict[str, Any]]]:
#     """Normalize symbol, query trading.search_ticker with simple retries, return (symbol, result_or_none)."""
#     k_clean = symbol_raw.replace("-", "").upper()

#     attempt = 0
#     while True:
#         try:
#             x = await trading.search_ticker(k_clean)
#             return k_clean, x if x else None
#         except Exception as e:
#             attempt += 1
#             if attempt > max_retries:
#                 print(f"[ERROR] {k_clean}: {e}")
#                 return k_clean, None
#             await asyncio.sleep(delay)
#             delay *= 2  # backoff
# async def main() -> None:
#     # Build tasks for concurrent fetching
#     tasks = [fetch_ticker(k) for k, _ in CRYPTO_WEBHOOKS.items()]
#     results = await asyncio.gather(*tasks)

#     # List of dicts for DataFrame
#     rows = []

#     for k_clean, x in results:
#         if x is None:
#             print(f"[WARN] No data for {k_clean}")
#             continue

#         ticker_id = x.get("ticker_id")
#         ticker = x.get("ticker")
#         if 'USD' not in ticker:
#             continue
#         if ticker_id is None or ticker is None:
#             print(f"[WARN] Incomplete data for {k_clean}: {x}")
#             continue

#         # Collect each as a row dict
#         rows.append({"ticker_id": ticker_id, "ticker": ticker})

#     # Make DataFrame
#     df = pd.DataFrame(rows, columns=['ticker_id', 'ticker'])
#     df.to_csv('crypto_tickers.csv', index=False)

# if __name__ == "__main__":
#     asyncio.run(main())


print(trading.coin_to_id_map.get('BTCUSD'))