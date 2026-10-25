"""Output destinations for records — the seam where Kafka plugs in.

Every sink implements `send(key, value)` and `close()`. Today only
`StdoutSink` exists; it prints newline-delimited JSON (NDJSON), which is the
exact shape a Kafka message takes: a partition key + a JSON value.

`KafkaSink` here (confluent-kafka or kafka-python) writes to
the topic in `Config.kafka_topic`. `main()` picks the sink by name, so nothing
else has to change.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Protocol

log = logging.getLogger(__name__)


class Sink(Protocol):
    def send(self, key: str, value: dict) -> None: ...

    def close(self) -> None: ...


class StdoutSink:
    """Write each record as one JSON line to stdout."""

    def send(self, key: str, value: dict) -> None:
        # Keep stdout as a clean NDJSON stream (pipe it to a file or `jq`).
        sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    def close(self) -> None:
        pass


class KafkaSink:
    """Publish each record to a Kafka topic as a JSON message, keyed by coin id.

    Uses kafka-python (pure Python, no native deps). The key is UTF-8 encoded so
    Kafka hashes it to a stable partition; the value is JSON bytes.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str = "PLAIN",
        sasl_username: str = "",
        sasl_password: str = "",
    ) -> None:
        from kafka import KafkaProducer  # lazy import: only needed for SINK=kafka

        self.topic = topic
        kwargs = {
            "bootstrap_servers": [s.strip() for s in bootstrap_servers.split(",")],
            "key_serializer": lambda k: k.encode("utf-8") if k is not None else None,
            "value_serializer": lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
            "acks": "all",  # wait for the broker to persist before considering it sent
            "retries": 3,
        }
        # Local Kafka is PLAINTEXT; Azure Event Hubs needs SASL_SSL + PLAIN.
        if security_protocol != "PLAINTEXT":
            kwargs.update(
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                sasl_plain_username=sasl_username,
                sasl_plain_password=sasl_password,
            )
        self._producer = KafkaProducer(**kwargs)
        log.info(
            "connected to Kafka %s (%s), topic=%s",
            bootstrap_servers,
            security_protocol,
            topic,
        )

    def send(self, key: str, value: dict) -> None:
        future = self._producer.send(self.topic, key=key, value=value)
        future.add_errback(lambda exc: log.error("kafka send failed: %s", exc))

    def close(self) -> None:
        # Flush buffered messages before exit so nothing is lost on shutdown.
        self._producer.flush()
        self._producer.close()


def build_sink(config) -> Sink:
    """Factory: return the sink named by `config.sink`."""
    if config.sink == "stdout":
        return StdoutSink()
    if config.sink == "kafka":
        from .keyvault import load_kafka_password  # resolves via Key Vault if set

        return KafkaSink(
            config.kafka_bootstrap_servers,
            config.kafka_topic,
            security_protocol=config.kafka_security_protocol,
            sasl_mechanism=config.kafka_sasl_mechanism,
            sasl_username=config.kafka_sasl_username,
            sasl_password=load_kafka_password(config),
        )
    raise ValueError(f"unknown sink: {config.sink!r}")
