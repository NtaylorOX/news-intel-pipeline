"""Kafka messaging utilities for inter-service communication."""

import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaConfig:
    """Kafka configuration."""

    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.topics = {
            "article_needs_analysis": "article_needs_analysis",
            "article_analyzed": "article_analyzed",
            "analysis_error": "analysis_error",
        }


class KafkaMessageProducer:
    """Produces messages to Kafka topics."""

    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.config = KafkaConfig(bootstrap_servers)

    def connect(self):
        """Connect to Kafka."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(f"✓ Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def send_message(self, topic: str, message: Dict[str, Any]) -> bool:
        """Send a message to a topic.

        Args:
            topic: Kafka topic name
            message: Message dictionary

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.producer:
            self.connect()

        try:
            future = self.producer.send(topic, value=message)
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Message sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Error sending message to {topic}: {e}")
            return False

    def queue_article_for_analysis(self, article_id: int, article_content: str) -> bool:
        """Queue an article for analysis.

        Args:
            article_id: Database article ID
            article_content: Article text to analyze

        Returns:
            True if queued successfully
        """
        message = {
            "article_id": article_id,
            "content": article_content,
            "timestamp": str(__import__("datetime").datetime.utcnow()),
        }
        return self.send_message(self.config.topics["article_needs_analysis"], message)

    def send_analysis_complete(self, article_id: int, analysis_result: Dict) -> bool:
        """Send analysis complete notification.

        Args:
            article_id: Database article ID
            analysis_result: Analysis result dictionary

        Returns:
            True if sent successfully
        """
        message = {
            "article_id": article_id,
            "analysis": analysis_result,
            "timestamp": str(__import__("datetime").datetime.utcnow()),
        }
        return self.send_message(self.config.topics["article_analyzed"], message)

    def send_analysis_error(self, article_id: int, error_message: str) -> bool:
        """Send analysis error notification.

        Args:
            article_id: Database article ID
            error_message: Error message

        Returns:
            True if sent successfully
        """
        message = {
            "article_id": article_id,
            "error": error_message,
            "timestamp": str(__import__("datetime").datetime.utcnow()),
        }
        return self.send_message(self.config.topics["analysis_error"], message)

    def close(self):
        """Close Kafka connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


class KafkaMessageConsumer:
    """Consumes messages from Kafka topics."""

    def __init__(self, topic: str, group_id: str, bootstrap_servers: str = "kafka:9092"):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.config = KafkaConfig(bootstrap_servers)

    def connect(self):
        """Connect to Kafka and subscribe to topic."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            logger.info(f"✓ Consumer connected to Kafka topic: {self.topic} (group: {self.group_id})")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def consume_messages(self, timeout_ms: int = 1000):
        """Consume messages from topic (generator).

        Args:
            timeout_ms: Timeout in milliseconds

        Yields:
            Message dictionaries
        """
        if not self.consumer:
            self.connect()

        for message in self.consumer:
            yield message.value

    def consume_one(self, timeout_ms: int = 1000) -> Optional[Dict]:
        """Consume a single message.

        Args:
            timeout_ms: Timeout in milliseconds

        Returns:
            Message dictionary or None if timeout
        """
        if not self.consumer:
            self.connect()

        try:
            msg = next(self.consumer, None)
            if msg:
                return msg.value
            return None
        except Exception as e:
            logger.error(f"Error consuming message: {e}")
            return None

    def close(self):
        """Close Kafka connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


def create_topics_if_not_exist(bootstrap_servers: str = "kafka:9092"):
    """Create required Kafka topics if they don't exist.

    Args:
        bootstrap_servers: Kafka bootstrap servers
    """
    from kafka.admin import KafkaAdminClient, NewTopic

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

        topics = [
            NewTopic(name="raw_articles", num_partitions=3, replication_factor=1),
            NewTopic(name="article_needs_analysis", num_partitions=3, replication_factor=1),
            NewTopic(name="article_analyzed", num_partitions=3, replication_factor=1),
            NewTopic(name="analysis_error", num_partitions=1, replication_factor=1),
        ]

        fs = admin_client.create_topics(new_topics=topics, validate_only=False)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"✓ Topic '{topic}' created")
            except Exception as e:
                logger.info(f"Topic '{topic}' already exists or error: {e}")

        admin_client.close()
    except Exception as e:
        logger.warning(f"Could not create Kafka topics: {e}")
