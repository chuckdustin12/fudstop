import pandas as pd
import asyncio
import re
import aiohttp
import random
from typing import Optional, Dict, Any, List

from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions()


class TransparencyUSASDK:
    def __init__(self):
        # only states supported by TransparencyUSA
        self.SUPPORTED_STATES = [
            "al", "az", "ca", "co", "fl", "ga", "il", "in", "ia", "mi", "mn",
            "nv", "nh", "nm", "ny", "nc", "oh", "pa", "sc", "tx", "va", "wa", "wi", "wy"
        ]

        # per-state cycle mapping rule
        self.CYCLE_BY_STATE = {
            "tx": "2015-to-now",
            "va": "2016-to-now",
        }
        self.DEFAULT_CYCLE = "2017-to-now"

    def _cycle_for(self, state: str) -> str:
        """Return the correct cycle string for a state per your rule."""
        return self.CYCLE_BY_STATE.get(state.lower(), self.DEFAULT_CYCLE)

    # ---------- networking helpers (retries, backoff, timeout) ----------

    async def _get_json_with_retries(
        self,
        session: aiohttp.ClientSession,
        url: str,
        *,
        max_retries: int = 5,
        backoff_base: float = 1.6,
        per_request_timeout: float = 30.0,
        retry_for_statuses: Optional[List[int]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        GET json with retries. Returns None if still failing after retries.
        Retries on network errors, timeouts, and certain HTTP statuses (e.g., 429, 5xx, 524).
        """
        if retry_for_statuses is None:
            retry_for_statuses = [408, 429, 500, 502, 503, 504, 522, 523, 524]

        timeout = aiohttp.ClientTimeout(total=per_request_timeout)

        for attempt in range(max_retries + 1):
            try:
                async with session.get(url, timeout=timeout) as resp:
                    # retry on retryable statuses
                    if resp.status in retry_for_statuses:
                        # respect Retry-After if present (seconds)
                        retry_after = resp.headers.get("Retry-After")
                        if attempt < max_retries:
                            if retry_after and retry_after.isdigit():
                                sleep_for = float(retry_after)
                            else:
                                # exponential backoff + jitter
                                sleep_for = (backoff_base ** attempt) + random.uniform(0, 0.5)
                            # optional: print/log
                            print(f"[warn] {resp.status} on {url} — retrying in {sleep_for:.1f}s (attempt {attempt+1}/{max_retries})")
                            await asyncio.sleep(sleep_for)
                            continue
                        else:
                            print(f"[error] {resp.status} on {url} — giving up after {attempt} retries")
                            return None

                    resp.raise_for_status()
                    return await resp.json()

            except (aiohttp.ClientResponseError, aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                if attempt < max_retries:
                    sleep_for = (backoff_base ** attempt) + random.uniform(0, 0.5)
                    print(f"[warn] {type(e).__name__} on {url} — retrying in {sleep_for:.1f}s (attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(sleep_for)
                    continue
                else:
                    print(f"[error] {type(e).__name__} on {url} — giving up after {attempt} retries")
                    return None
            except aiohttp.ClientError as e:
                # non-retryable aiohttp error
                print(f"[error] ClientError on {url}: {e}")
                return None
            except Exception as e:
                # unexpected error; do not crash the entire run
                print(f"[error] Unexpected error on {url}: {e}")
                return None

    # ------------------------- public methods --------------------------

    async def get_payees(
        self,
        cycle: str,
        state: str = 'tx',
        sortBy: str = 'electionExpenditures',
        sortOrder: str = 'desc',
        offset: int = 0,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Fetches all payees for the given cycle/state from TransparencyUSA,
        automatically paginating until all results (info.total) are retrieved.
        Returns a DataFrame with expenditures as int64 (no scientific notation).
        """
        base_url = "https://www.transparencyusa.org/v2/payees"
        params = {
            "state": state,
            "cycle": cycle,
            "sortBy": sortBy,
            "sortOrder": sortOrder,
            "offset": offset,
            "limit": limit
        }

        all_names, all_slugs, all_categories, all_expenditures = [], [], [], []
        total = None
        fetched = 0

        async with aiohttp.ClientSession() as session:
            while True:
                url = (
                    f"{base_url}?state={params['state']}"
                    f"&cycle={params['cycle']}"
                    f"&sortBy={params['sortBy']}"
                    f"&sortOrder={params['sortOrder']}"
                    f"&offset={params['offset']}"
                    f"&limit={params['limit']}"
                )

                data = await self._get_json_with_retries(session, url)
                if data is None:
                    # graceful stop for this state, return what we have
                    print(f"[warn] Stopping pagination early for {state} at offset={params['offset']}")
                    break

                results = data.get('results', []) or []
                info = data.get('info') or {}
                if total is None:
                    total = info.get('total')

                for i in results:
                    all_names.append(i.get('payeeName'))
                    all_slugs.append(i.get('payeeSlug'))
                    all_categories.append(i.get('lobbyistClientCategory'))
                    all_expenditures.append(i.get('electionExpenditures'))

                page_count = len(results)
                fetched += page_count

                if page_count == 0:
                    break
                if isinstance(total, int) and fetched >= total:
                    break

                params['offset'] += params['limit']

        df = pd.DataFrame({
            'name': all_names,
            'slug': all_slugs,
            'lobbyist_category': all_categories,
            'expenditures': all_expenditures
        })

        df['expenditures'] = (
            pd.to_numeric(df['expenditures'], errors='coerce')
              .fillna(0)
              .astype('int64')
        )

        return df

    async def _fetch_payees_for_state(
        self,
        session: aiohttp.ClientSession,
        state: str,
        sortBy: str,
        sortOrder: str,
        limit: int
    ):
        """
        Internal helper: fetch ALL paginated payees for a single state.
        Enforces the per-state cycle rule:
          tx -> 2015-to-now, va -> 2016-to-now, others -> 2017-to-now.
        Gracefully handles transient errors with retries and stops early on persistent failures.
        """
        base_url = "https://www.transparencyusa.org/v2/payees"
        cycle = self._cycle_for(state)
        offset = 0
        total = None
        rows = []

        while True:
            url = (
                f"{base_url}?state={state}"
                f"&cycle={cycle}"
                f"&sortBy={sortBy}"
                f"&sortOrder={sortOrder}"
                f"&offset={offset}"
                f"&limit={limit}"
            )

            data = await self._get_json_with_retries(session, url)
            if data is None:
                print(f"[warn] Stopping {state.upper()} early at offset={offset} due to repeated failures.")
                break

            results = data.get("results", []) or []
            info = data.get("info") or {}
            if total is None:
                total = info.get("total")

            if not results:
                break

            for i in results:
                rows.append({
                    "state": state,
                    "cycle": cycle,
                    "name": i.get("payeeName"),
                    "slug": i.get("payeeSlug"),
                    "lobbyist_category": i.get("lobbyistClientCategory"),
                    "expenditures": i.get("electionExpenditures"),
                })

            offset += limit
            if isinstance(total, int) and len(rows) >= total:
                break

        return rows

    async def get_all_payees(
        self,
        cycle: str = "2017-to-now",  # kept for signature compatibility; per-state rule overrides
        sortBy: str = "electionExpenditures",
        sortOrder: str = "desc",
        limit: int = 100,
        states: list[str] | None = None,
        concurrency: int = 6
    ) -> pd.DataFrame:
        """
        Fetch all payees across SUPPORTED_STATES (or a provided subset),
        enforcing per-state cycles and handling transient failures gracefully.
        If a state's pagination fails repeatedly, we keep partial data
        and continue with the rest.
        """
        use_states = states or self.SUPPORTED_STATES

        connector = aiohttp.TCPConnector(limit_per_host=concurrency)
        sem = asyncio.Semaphore(concurrency)

        async with aiohttp.ClientSession(connector=connector) as session:
            async def run_one(st):
                async with sem:
                    return await self._fetch_payees_for_state(
                        session=session,
                        state=st,
                        sortBy=sortBy,
                        sortOrder=sortOrder,
                        limit=limit
                    )

            results_per_state = await asyncio.gather(*[run_one(s) for s in use_states], return_exceptions=True)

        rows = []
        for st, result in zip(use_states, results_per_state):
            if isinstance(result, Exception):
                # shouldn't happen due to internal handling, but guard anyway
                print(f"[error] State {st.upper()} raised: {result}. Continuing with others.")
                continue
            rows.extend(result)

        df = pd.DataFrame(
            rows,
            columns=["state", "cycle", "name", "slug", "lobbyist_category", "expenditures"]
        )

        df["expenditures"] = pd.to_numeric(df["expenditures"], errors="coerce").fillna(0).astype("int64")
        return df

    



    async def get_individuals(self,
        state: str = 'tx',
        cycle: str = '2015-to-now',
        sortOrder: str = 'desc',
        offset: int = 0,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Fetch all individual contributors from TransparencyUSA for the given state/cycle.
        Paginates until info.total (if provided) is reached or no more results.
        Returns a DataFrame with contributions as int64 (avoids scientific notation).
        """
        base_url = "https://www.transparencyusa.org/v2/contributors"
        params = {
            "state": state,
            "cycle": cycle,
            "sortBy": "electionContributions",
            "sortOrder": sortOrder,
            "offset": offset,
            "limit": limit,
            "type": "individuals",
        }

        all_names = []
        all_slugs = []
        all_contribs = []

        total = None
        fetched = 0

        async with aiohttp.ClientSession() as session:
            while True:
                url = (
                    f"{base_url}?state={params['state']}"
                    f"&cycle={params['cycle']}"
                    f"&sortBy={params['sortBy']}"
                    f"&sortOrder={params['sortOrder']}"
                    f"&offset={params['offset']}"
                    f"&limit={params['limit']}"
                    f"&type={params['type']}"
                )

                async with session.get(url) as resp:
                    resp.raise_for_status()
                    payload = await resp.json()

                results = payload.get('results', []) or []
                info = payload.get('info') or {}
                if total is None:
                    total = info.get('total')

                # accumulate
                for i in results:
                    all_names.append(i.get('contributorName'))
                    all_slugs.append(i.get('contributorSlug'))
                    all_contribs.append(i.get('electionContributions'))

                page_count = len(results)
                fetched += page_count

                # stop when no more results or we've reached total (if known)
                if page_count == 0:
                    break
                if isinstance(total, int) and fetched >= total:
                    break

                # next page
                params['offset'] += params['limit']

        df = pd.DataFrame({
            'name': all_names,
            'slug': all_slugs,
            'contributions': all_contribs
        })

        # ensure contributions are integers (prevents scientific notation)
        df['contributions'] = (
            pd.to_numeric(df['contributions'], errors='coerce')
            .fillna(0)
            .astype('int64')
        )

        # optional: pretty display column
        # df['contributions_display'] = df['contributions'].map(lambda x: f"{x:,}")

        return df
    

    async def _fetch_individuals_for_state(
        self,
        session: aiohttp.ClientSession,
        state: str,
        sortOrder: str,
        limit: int
    ):
        """
        Internal helper: fetch ALL paginated individual contributors for a single state.
        Enforces the per-state cycle rule:
          tx -> 2015-to-now, va -> 2016-to-now, others -> 2017-to-now.
        Uses the same retry/backoff logic as payees.
        """
        base_url = "https://www.transparencyusa.org/v2/contributors"
        cycle = self._cycle_for(state)
        sortBy = "electionContributions"
        offset = 0
        total = None
        rows = []

        while True:
            url = (
                f"{base_url}?state={state}"
                f"&cycle={cycle}"
                f"&sortBy={sortBy}"
                f"&sortOrder={sortOrder}"
                f"&offset={offset}"
                f"&limit={limit}"
                f"&type=individuals"
            )

            data = await self._get_json_with_retries(session, url)
            if data is None:
                print(f"[warn] Stopping individuals {state.upper()} early at offset={offset} due to repeated failures.")
                break

            results = data.get("results", []) or []
            info = data.get("info") or {}
            if total is None:
                total = info.get("total")

            if not results:
                break

            for i in results:
                rows.append({
                    "state": state,
                    "cycle": cycle,
                    "name": i.get("contributorName"),
                    "slug": i.get("contributorSlug"),
                    "contributions": i.get("electionContributions"),
                })

            offset += limit
            if isinstance(total, int) and len(rows) >= total:
                break

        return rows

    async def get_all_individuals(
        self,
        cycle: str = "2017-to-now",  # kept for signature compatibility; per-state rule overrides
        sortOrder: str = "desc",
        limit: int = 100,
        states: list[str] | None = None,
        concurrency: int = 6
    ) -> pd.DataFrame:
        """
        Fetch all individual contributors across SUPPORTED_STATES (or a provided subset),
        enforcing per-state cycles and handling transient failures gracefully.
        If a state's pagination fails repeatedly, we keep partial data and continue.
        """
        use_states = states or self.SUPPORTED_STATES

        connector = aiohttp.TCPConnector(limit_per_host=concurrency)
        sem = asyncio.Semaphore(concurrency)

        async with aiohttp.ClientSession(connector=connector) as session:
            async def run_one(st):
                async with sem:
                    return await self._fetch_individuals_for_state(
                        session=session,
                        state=st,
                        sortOrder=sortOrder,
                        limit=limit
                    )

            results_per_state = await asyncio.gather(*[run_one(s) for s in use_states], return_exceptions=True)

        rows = []
        for st, result in zip(use_states, results_per_state):
            if isinstance(result, Exception):
                print(f"[error] State {st.upper()} (individuals) raised: {result}. Continuing with others.")
                continue
            rows.extend(result)

        df = pd.DataFrame(rows, columns=["state", "cycle", "name", "slug", "contributions"])
        df["contributions"] = pd.to_numeric(df["contributions"], errors="coerce").fillna(0).astype("int64")
        return df