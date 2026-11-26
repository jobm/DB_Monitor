"""Kafka container fixture using testcontainers for consumer integration tests.

Provides a session-scoped ``kafka_bootstrap_servers`` fixture that starts a
real Kafka broker via testcontainers_ (``confluentinc/cp-kafka``) and yields
the ``host:port`` bootstrap string. The container is torn down after the last
test in the session.

If the ``KAFKA_BOOTSTRAP_SERVERS`` environment variable is set (e.g., in CI
where Kafka is provided as a GitHub Actions service container), the fixture
uses that value directly instead of starting a testcontainers container.

Usage in a test:

.. code:: python

    @pytest.mark.slow
    @pytest.mark.anyio
    async def test_produce_and_consume(kafka_bootstrap_servers: str) -> None:
        # The fixture provides the bootstrap server address.
        ...

Requires:
    - Docker (daemon running) for the container to start.
    - ``testcontainers`` and ``kafka-python`` packages (dev dependency group).

.. _testcontainers: https://testcontainers-python.readthedocs.io/
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterator

import pytest
from testcontainers.kafka import KafkaContainer

logger = logging.getLogger(__name__)

KAFKA_IMAGE = "confluentinc/cp-kafka:latest"
"""Docker image used for the embedded Kafka broker.

``confluentinc/cp-kafka`` is the standard Confluent Community distribution
and is compatible with plain Kafka protocol clients (aiokafka, kafka-python).
"""


def _docker_available() -> bool:
    """Return True if the Docker Python SDK can reach a daemon.

    Uses the same ``docker`` SDK that testcontainers relies on internally.
    Returns False when the daemon is unreachable (not installed, socket
    permission denied, or Podman CLI emulation without SDK support) so
    tests can skip gracefully.
    """
    try:
        import docker

        client = docker.from_env()
        client.ping()
        client.close()
        return True
    except Exception as exc:
        logger.info(
            "Docker daemon not reachable via SDK: %s",
            exc,
        )
        return False


@pytest.fixture(scope="session")
def kafka_bootstrap_servers() -> Iterator[str]:
    """Start a Kafka container and yield the ``host:port`` bootstrap string.

    The container stays alive for the entire test session and is stopped
    (removed) during session finalisation.

    If the ``KAFKA_BOOTSTRAP_SERVERS`` environment variable is set, that
    value is used directly (for CI where Kafka is a service container).
    Otherwise, a testcontainers Kafka container is started.

    Skips the test if the Docker daemon is not reachable when no env var
    is set.
    """
    env_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if env_servers:
        logger.info(
            "Using KAFKA_BOOTSTRAP_SERVERS from environment: %s",
            env_servers,
        )
        yield env_servers
        return

    if not _docker_available():
        pytest.skip("Docker daemon is not reachable — cannot start Kafka container")

    with KafkaContainer(KAFKA_IMAGE) as kafka:
        bootstrap = kafka.get_bootstrap_server()
        logger.info("Kafka container ready at %s", bootstrap)
        yield bootstrap


# ── Helpers ──────────────────────────────────────────────────────────────
# These utilities make it easier to write integration tests that produce
# messages to the containerised broker and consume them back.


async def wait_for_topic(
    bootstrap_servers: str,
    topic: str,
    *,
    timeout: float = 90.0,
    interval: float = 0.25,
) -> None:
    """Wait until *topic* is visible in the cluster metadata.

    Raises ``TimeoutError`` if the topic does not appear within *timeout*
    seconds.
    """
    from aiokafka import AIOKafkaConsumer

    # Ensure the topic exists (auto-creation may be disabled on the broker).
    try:
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic

        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
        await admin.start()
        try:
            await admin.create_topics(
                [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
            )
        except Exception:
            pass
        finally:
            await admin.stop()
    except Exception:
        pass

    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
        )
        try:
            await consumer.start()
            topics = await consumer.topics()
            if topic in topics:
                return
        except Exception:
            pass
        finally:
            await consumer.stop()
        if asyncio.get_event_loop().time() >= deadline:
            raise TimeoutError(
                f"Topic '{topic}' did not become available within {timeout}s"
            )
        await asyncio.sleep(interval)


async def produce_message(
    bootstrap_servers: str,
    topic: str,
    payload: bytes,
    *,
    key: bytes | None = None,
) -> None:
    """Produce one message to *topic* and confirm the send."""
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        future = await producer.send(topic, value=payload, key=key)
        await future  # wait for ack
    finally:
        await producer.stop()


async def consume_messages(
    bootstrap_servers: str,
    topic: str,
    *,
    expected_count: int = 1,
    timeout: float = 10.0,
) -> list[bytes]:
    """Consume *expected_count* messages from *topic* and return their values.

    Uses ``auto_offset_reset='earliest'`` so it reads from the beginning of
    the topic. Returns once all expected messages are received or the timeout
    expires, whichever comes first.
    """
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        values: list[bytes] = []
        deadline = asyncio.get_event_loop().time() + timeout
        while len(values) < expected_count:
            if asyncio.get_event_loop().time() >= deadline:
                break
            try:
                msg = await asyncio.wait_for(
                    consumer.getone(),
                    timeout=max(0.1, deadline - asyncio.get_event_loop().time()),
                )
                values.append(msg.value)
            except asyncio.TimeoutError:
                continue
        return values
    finally:
        await consumer.stop()
