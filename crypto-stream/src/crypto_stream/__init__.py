"""crypto-stream — a crypto market-data producer.

It polls CoinGecko's `/coins/markets` endpoint on an interval, turns each
response into clean keyed records, and pushes them through a `Sink`. Run it
with `uv run crypto-stream` (or `uv run python -m crypto_stream`).
"""

from __future__ import annotations

import logging
import signal
import threading
from dataclasses import asdict

from .coingecko import CoinGeckoClient, CoinGeckoError
from .config import Config
from .records import to_records
from .sinks import Sink, build_sink

log = logging.getLogger("crypto_stream")


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def _poll_once(client: CoinGeckoClient, sink: Sink, config: Config) -> int:
    """Fetch, transform, and emit one batch. Returns the number of records."""
    raw = client.fetch_markets(config.coins, config.vs_currency)
    records = to_records(raw, config.vs_currency)
    for record in records:
        # Key by coin id so Kafka co-locates each coin's events on one partition.
        sink.send(record.coin_id, asdict(record))
    return len(records)


def run(config: Config) -> None:
    client = CoinGeckoClient(
        base_url=config.api_base_url,
        timeout=config.request_timeout_seconds,
        max_retries=config.max_retries,
    )
    sink = build_sink(config)

    # Interruptible sleep + clean shutdown on Ctrl-C / SIGTERM (e.g. docker stop).
    stop = threading.Event()

    def _handle_signal(signum, _frame):
        log.info("received signal %s, shutting down...", signum)
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info(
        "starting producer: coins=%s currency=%s interval=%ss sink=%s",
        ",".join(config.coins),
        config.vs_currency,
        config.poll_interval_seconds,
        config.sink,
    )

    try:
        while not stop.is_set():
            try:
                count = _poll_once(client, sink, config)
                log.info("emitted %d records", count)
            except CoinGeckoError as exc:
                log.error("fetch failed, will retry next cycle: %s", exc)

            if config.run_once:
                break
            stop.wait(config.poll_interval_seconds)
    finally:
        sink.close()
        log.info("stopped")


def main() -> None:
    config = Config.from_env()
    _configure_logging(config.log_level)
    run(config)
