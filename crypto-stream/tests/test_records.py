"""Tests for the raw-API → clean-record transformation."""

from crypto_stream.records import SOURCE, to_record, to_records

RAW = {
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin",
    "current_price": 81026.0,
    "market_cap": 1628464963350,
    "market_cap_rank": 1,
    "total_volume": 24071009015,
    "high_24h": 81939,
    "low_24h": 80548,
    "price_change_24h": -268.58,
    "price_change_percentage_24h": -0.29563,
    "circulating_supply": 20102484.0,
    "last_updated": "2026-10-23T23:49:52.000Z",
}


def test_to_record_maps_fields():
    rec = to_record(RAW, "usd")
    assert rec.coin_id == "bitcoin"
    assert rec.symbol == "BTC"  # upper-cased
    assert rec.name == "Bitcoin"
    assert rec.currency == "usd"
    assert rec.price == 81026.0
    assert rec.volume_24h == 24071009015
    assert rec.source == SOURCE
    assert rec.source_updated_at == "2026-10-23T23:49:52.000Z"


def test_to_record_adds_metadata():
    rec = to_record(RAW, "usd")
    assert rec.event_id  # non-empty id
    assert "T" in rec.ingested_at  # ISO-8601 timestamp
    # each record gets its own unique id
    assert to_record(RAW, "usd").event_id != rec.event_id


def test_to_record_handles_missing_keys():
    rec = to_record({"id": "ethereum", "symbol": "eth"}, "usd")
    assert rec.coin_id == "ethereum"
    assert rec.symbol == "ETH"
    assert rec.price is None
    assert rec.market_cap is None


def test_to_records_maps_a_list():
    recs = to_records([RAW, {"id": "solana", "symbol": "sol"}], "usd")
    assert len(recs) == 2
    assert recs[0].coin_id == "bitcoin"
    assert recs[1].coin_id == "solana"
