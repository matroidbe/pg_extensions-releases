"""Protocol compatibility tests using kafka-python client.

These tests verify that pg_kafka's Kafka protocol implementation
is compatible with the kafka-python client library.

Run with: pytest extensions/pg_kafka/pytests/ -v
"""
import json
import time

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


class TestKafkaPythonProducer:
    """Test produce operations with kafka-python."""

    def test_connect(self, kafka_bootstrap, skip_if_no_server):
        """Test basic connection to pg_kafka server."""
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            request_timeout_ms=5000,
        )
        assert producer.bootstrap_connected()
        producer.close()

    def test_produce_bytes(self, kafka_bootstrap, test_topic, skip_if_no_server):
        """Test producing a bytes message."""
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            request_timeout_ms=5000,
        )

        future = producer.send(
            test_topic,
            key=b"python-key",
            value=b"python-value",
        )

        try:
            record_metadata = future.get(timeout=10)
            assert record_metadata.topic == test_topic
            print(
                f"Produced to partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
        except KafkaError as e:
            pytest.fail(f"Produce failed: {e}")
        finally:
            producer.close()

    def test_produce_json(self, kafka_bootstrap, test_topic, skip_if_no_server):
        """Test producing a JSON message."""
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
        )

        future = producer.send(
            test_topic,
            key=b"json-key",
            value={"name": "test", "count": 42},
        )

        try:
            record_metadata = future.get(timeout=10)
            assert record_metadata.topic == test_topic
        except KafkaError as e:
            pytest.fail(f"JSON produce failed: {e}")
        finally:
            producer.close()

    def test_produce_batch(self, kafka_bootstrap, test_topic, skip_if_no_server):
        """Test producing multiple messages in a batch."""
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            request_timeout_ms=5000,
            batch_size=16384,  # 16KB batch
            linger_ms=10,
        )

        messages = [
            (b"batch-1", b"Message 1"),
            (b"batch-2", b"Message 2"),
            (b"batch-3", b"Message 3"),
        ]

        futures = []
        for key, value in messages:
            future = producer.send(test_topic, key=key, value=value)
            futures.append(future)

        # Wait for all messages
        for i, future in enumerate(futures):
            try:
                future.get(timeout=10)
            except KafkaError as e:
                pytest.fail(f"Batch message {i} failed: {e}")

        producer.close()
        print(f"Produced batch of {len(messages)} messages")


class TestKafkaPythonConsumer:
    """Test consume operations with kafka-python."""

    def test_subscribe(self, kafka_bootstrap, test_topic, skip_if_no_server):
        """Test topic subscription."""
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            group_id="pytest-group",
        )

        # Subscription should succeed
        assert test_topic in consumer.subscription()
        consumer.close()

    def test_consume_messages(self, kafka_bootstrap, test_topic, skip_if_no_server):
        """Test consuming messages from a topic."""
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            group_id="pytest-consume-group",
        )

        messages = []
        for msg in consumer:
            messages.append(msg)
            if len(messages) >= 5:
                break

        consumer.close()

        # Should have received some messages (if topic has data)
        print(f"Consumed {len(messages)} messages")

    def test_consume_with_deserializer(
        self, kafka_bootstrap, test_topic, skip_if_no_server
    ):
        """Test consuming with value deserializer."""
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            group_id="pytest-deserialize-group",
            value_deserializer=lambda v: v.decode("utf-8") if v else None,
        )

        count = 0
        for msg in consumer:
            assert isinstance(msg.value, (str, type(None)))
            count += 1
            if count >= 3:
                break

        consumer.close()
        print(f"Consumed and deserialized {count} messages")


class TestKafkaPythonMetadata:
    """Test metadata operations with kafka-python."""

    def test_list_topics(self, kafka_bootstrap, skip_if_no_server):
        """Test listing topics."""
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap,
            request_timeout_ms=5000,
        )

        topics = consumer.topics()
        assert isinstance(topics, set)
        print(f"Topics: {topics}")
        consumer.close()

    def test_partitions_for_topic(
        self, kafka_bootstrap, test_topic, skip_if_no_server
    ):
        """Test getting partitions for a topic."""
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap,
            request_timeout_ms=5000,
        )

        partitions = consumer.partitions_for_topic(test_topic)
        # Should have at least partition 0
        assert partitions is not None
        assert 0 in partitions
        print(f"Partitions for {test_topic}: {partitions}")
        consumer.close()

    def test_beginning_offsets(self, kafka_bootstrap, test_topic, skip_if_no_server):
        """Test getting beginning offsets."""
        from kafka import TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap,
            request_timeout_ms=5000,
        )

        tp = TopicPartition(test_topic, 0)
        offsets = consumer.beginning_offsets([tp])

        assert tp in offsets
        print(f"Beginning offset for {test_topic}:0 = {offsets[tp]}")
        consumer.close()

    def test_end_offsets(self, kafka_bootstrap, test_topic, skip_if_no_server):
        """Test getting end offsets."""
        from kafka import TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap,
            request_timeout_ms=5000,
        )

        tp = TopicPartition(test_topic, 0)
        offsets = consumer.end_offsets([tp])

        assert tp in offsets
        print(f"End offset for {test_topic}:0 = {offsets[tp]}")
        consumer.close()
