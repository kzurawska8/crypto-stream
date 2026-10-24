"""Transform raw CoinGecko objects into clean, flat records.

* `event_id`      — unique id per emitted record (good for dedup downstream)
* `ingested_at`   — when *we* pulled it (UTC, ISO-8601)
* `source_updated_at` — when the exchange data was last updated (from the API)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

SOURCE = "coingecko"


@dataclass
class CryptoPriceRecord:
    event_id: str
    ingested_at: str
    source: str
    coin_id: str
    symbol: str
    name: str
    currency: str
    price: float | None
    market_cap: float | None
    market_cap_rank: int | None
    volume_24h: float | None
    high_24h: float | None
    low_24h: float | None
    price_change_24h: float | None
    price_change_percentage_24h: float | None
    circulating_supply: float | None
    source_updated_at: str | None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_record(raw: dict, vs_currency: str) -> CryptoPriceRecord:
    """Map one raw CoinGecko market object to a `CryptoPriceRecord`."""
    symbol = (raw.get("symbol") or "").upper()
    return CryptoPriceRecord(
        event_id=str(uuid.uuid4()),
        ingested_at=_now_iso(),
        source=SOURCE,
        coin_id=raw.get("id", ""),
        symbol=symbol,
        name=raw.get("name", ""),
        currency=vs_currency,
        price=raw.get("current_price"),
        market_cap=raw.get("market_cap"),
        market_cap_rank=raw.get("market_cap_rank"),
        volume_24h=raw.get("total_volume"),
        high_24h=raw.get("high_24h"),
        low_24h=raw.get("low_24h"),
        price_change_24h=raw.get("price_change_24h"),
        price_change_percentage_24h=raw.get("price_change_percentage_24h"),
        circulating_supply=raw.get("circulating_supply"),
        source_updated_at=raw.get("last_updated"),
    )


def to_records(raw_list: list[dict], vs_currency: str) -> list[CryptoPriceRecord]:
    return [to_record(item, vs_currency) for item in raw_list]
