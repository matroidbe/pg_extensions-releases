"""Pytest fixtures for pg_kafka integration tests.

These fixtures provide configuration and skip helpers for testing
pg_kafka with the kafka-python client.
"""
import os
import socket

import pytest

# Configuration from environment or defaults
PG_KAFKA_BOOTSTRAP = os.environ.get("PG_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
TEST_TOPIC = os.environ.get("TEST_TOPIC", "test-topic")


@pytest.fixture
def kafka_bootstrap():
    """Return Kafka bootstrap servers address."""
    return PG_KAFKA_BOOTSTRAP


@pytest.fixture
def test_topic():
    """Return test topic name."""
    return TEST_TOPIC


@pytest.fixture
def skip_if_no_server(kafka_bootstrap):
    """Skip test if pg_kafka server is not running.

    This fixture checks TCP connectivity to the Kafka server
    and skips the test gracefully if it's not available.
    """
    host, port_str = kafka_bootstrap.split(":")
    port = int(port_str)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        result = sock.connect_ex((host, port))
    finally:
        sock.close()

    if result != 0:
        pytest.skip(f"pg_kafka server not running at {kafka_bootstrap}")
