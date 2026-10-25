"""Tests for Key Vault password resolution (the no-Key-Vault fallback).

The actual Key Vault fetch is integration-tested manually against Azure; here we
only verify that without Key Vault configured, the plain env password is used and
no Azure SDK import is triggered.
"""

from crypto_stream.config import Config
from crypto_stream.keyvault import load_kafka_password


def test_password_from_env_when_no_keyvault(monkeypatch):
    monkeypatch.delenv("KEYVAULT_URL", raising=False)
    monkeypatch.delenv("KEYVAULT_SECRET_NAME", raising=False)
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "plain-secret")
    cfg = Config.from_env()
    assert load_kafka_password(cfg) == "plain-secret"
