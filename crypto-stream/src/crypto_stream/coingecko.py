"""Thin client for the CoinGecko public REST API.

Only the `/coins/markets` endpoint is used — it returns one rich object per
coin (price, volume, market cap, 24h high/low/change) in a single call.

Docs: https://docs.coingecko.com/reference/coins-markets
"""

from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger(__name__)


class CoinGeckoError(RuntimeError):
    """Raised when the API cannot be reached or returns an error status."""


class CoinGeckoClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        max_retries: int = 3,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "User-Agent": "crypto-stream-producer/0.1"}
        )

    def fetch_markets(self, coins: list[str], vs_currency: str) -> list[dict]:
        """Return the raw market objects for `coins`, priced in `vs_currency`.

        Retries transient failures (network errors, 5xx, 429) with exponential
        backoff, then raises `CoinGeckoError` if every attempt fails.
        """
        url = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": vs_currency,
            "ids": ",".join(coins),
            "order": "market_cap_desc",
            "per_page": max(len(coins), 1),
            "page": 1,
            "price_change_percentage": "24h",
        }

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = exc
                log.warning("request failed (attempt %d): %s", attempt, exc)
            else:
                if resp.status_code == 200:
                    data = resp.json()
                    if not isinstance(data, list):
                        raise CoinGeckoError(f"unexpected response shape: {type(data)}")
                    return data
                # 429 = rate limited, 5xx = server side: both worth retrying.
                if resp.status_code == 429 or resp.status_code >= 500:
                    last_error = CoinGeckoError(f"HTTP {resp.status_code}")
                    log.warning(
                        "retryable status %d (attempt %d)", resp.status_code, attempt
                    )
                else:
                    raise CoinGeckoError(
                        f"HTTP {resp.status_code}: {resp.text[:200]}"
                    )

            if attempt < self.max_retries:
                backoff = 2 ** (attempt - 1)
                time.sleep(backoff)

        raise CoinGeckoError(
            f"giving up after {self.max_retries} attempts: {last_error}"
        )
