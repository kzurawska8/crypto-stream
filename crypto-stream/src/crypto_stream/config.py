"""Runtime configuration, loaded from environment variables.

Everything here has a sensible default so the script runs with zero setup
(`uv run crypto-stream`). Override any value with an environment variable (or a `.env`
file sourced into the shell) — see `.env.example`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Coins fetched by default. Any CoinGecko coin id works (see /coins/list).
DEFAULT_COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "ripple",
    "dogecoin",
]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Config:
    """Immutable snapshot of runtime settings."""

    # --- Data source (CoinGecko) ---
    api_base_url: str
    vs_currency: str
    coins: list[str]

    # --- Polling behaviour ---
    poll_interval_seconds: float
    request_timeout_seconds: float
    max_retries: int
    run_once: bool
    log_level: str

    # --- Downstream sink (step 2+): "stdout" today, "kafka" later ---
    sink: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    # SASL auth — only needed for cloud brokers like Azure Event Hubs.
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    # Optional: fetch the connection string from Azure Key Vault (Entra ID auth)
    # instead of kafka_sasl_password. Blank = disabled.
    keyvault_url: str
    keyvault_secret_name: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            api_base_url=os.getenv(
                "COINGECKO_API_URL", "https://api.coingecko.com/api/v3"
            ),
            vs_currency=os.getenv("VS_CURRENCY", "usd"),
            coins=_env_list("COINS", DEFAULT_COINS),
            poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "15")),
            request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            run_once=_env_bool("RUN_ONCE", False),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            sink=os.getenv("SINK", "stdout").lower(),
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", "crypto.prices"),
            kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            kafka_sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            kafka_sasl_username=os.getenv("KAFKA_SASL_USERNAME", ""),
            kafka_sasl_password=os.getenv("KAFKA_SASL_PASSWORD", ""),
            keyvault_url=os.getenv("KEYVAULT_URL", ""),
            keyvault_secret_name=os.getenv("KEYVAULT_SECRET_NAME", ""),
        )
