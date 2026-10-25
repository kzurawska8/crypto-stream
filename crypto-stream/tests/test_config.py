"""Tests for environment-driven configuration."""

from crypto_stream.config import DEFAULT_COINS, Config, _env_bool, _env_list


def test_defaults(monkeypatch):
    for var in [
        "SINK",
        "COINS",
        "POLL_INTERVAL_SECONDS",
        "RUN_ONCE",
        "KEYVAULT_URL",
    ]:
        monkeypatch.delenv(var, raising=False)
    cfg = Config.from_env()
    assert cfg.sink == "stdout"
    assert cfg.coins == DEFAULT_COINS
    assert cfg.run_once is False
    assert cfg.poll_interval_seconds == 15.0


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("SINK", "kafka")
    monkeypatch.setenv("COINS", "bitcoin, ethereum ,solana")
    monkeypatch.setenv("RUN_ONCE", "true")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "60")
    cfg = Config.from_env()
    assert cfg.sink == "kafka"
    assert cfg.coins == ["bitcoin", "ethereum", "solana"]  # trimmed
    assert cfg.run_once is True
    assert cfg.poll_interval_seconds == 60.0


def test_env_bool(monkeypatch):
    monkeypatch.setenv("FLAG", "YES")
    assert _env_bool("FLAG", False) is True
    monkeypatch.setenv("FLAG", "off")
    assert _env_bool("FLAG", True) is False
    monkeypatch.delenv("FLAG", raising=False)
    assert _env_bool("FLAG", True) is True  # falls back to default


def test_env_list(monkeypatch):
    monkeypatch.delenv("ITEMS", raising=False)
    assert _env_list("ITEMS", ["a"]) == ["a"]  # default when unset
    monkeypatch.setenv("ITEMS", "x, y ,z")
    assert _env_list("ITEMS", ["a"]) == ["x", "y", "z"]
