"""Tests for the sink factory and the stdout sink."""

import json

import pytest

from crypto_stream.config import Config
from crypto_stream.sinks import StdoutSink, build_sink


def test_build_sink_returns_stdout(monkeypatch):
    monkeypatch.setenv("SINK", "stdout")
    assert isinstance(build_sink(Config.from_env()), StdoutSink)


def test_build_sink_rejects_unknown(monkeypatch):
    monkeypatch.setenv("SINK", "nonsense")
    with pytest.raises(ValueError):
        build_sink(Config.from_env())


def test_stdout_sink_writes_json_line(capsys):
    StdoutSink().send("bitcoin", {"coin_id": "bitcoin", "price": 1})
    out = capsys.readouterr().out
    assert json.loads(out) == {"coin_id": "bitcoin", "price": 1}
