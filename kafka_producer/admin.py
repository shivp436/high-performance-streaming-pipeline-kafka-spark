"""
Kafka topic administration
"""
import logging
import time
from confluent_kafka.admin import AdminClient, NewTopic
from typing import List

from .config import TopicConfig


class KafkaTopicManager:
    """Manages Kafka topic operations"""

    def __init__(self, bootstrap_servers: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.bootstrap_servers = bootstrap_servers

        try:
            self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
            self.logger.info("AdminClient initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize AdminClient: {e}")
            raise

    def list_topics(self) -> List[str]:
        """List all topics"""
        try:
            topic_metadata = self.admin_client.list_topics(timeout=10)
            topics = list(topic_metadata.topics.keys())
            return topics
        except Exception as e:
            self.logger.error(f"Error listing topics: {e}")
            raise

    def topic_exists(self, topic_name: str) -> bool:
        """Check if topic exists"""
        try:
            return topic_name in self.list_topics()
        except Exception as e:
            self.logger.error(f"Error checking if topic exists: {e}")
            raise

    def create_topic(self, topic_config: TopicConfig) -> None:
        """Create a topic with given configuration"""
        if self.topic_exists(topic_config.name):
            self.logger.info(f"Topic '{topic_config.name}' already exists")
            return

        new_topic = NewTopic(
            topic_config.name,
            num_partitions=topic_config.num_partitions,
            replication_factor=topic_config.replication_factor,
            config=topic_config.get_topic_config()
        )

        fs = self.admin_client.create_topics([new_topic])

        try:
            fs[topic_config.name].result()
            time.sleep(2)  # Wait for creation to propagate
            self.logger.info(
                f"Topic '{topic_config.name}' created successfully "
                f"(partitions={topic_config.num_partitions}, "
                f"replication={topic_config.replication_factor})"
            )
        except Exception as e:
            self.logger.error(f"Failed to create topic '{topic_config.name}': {e}")
            raise

    def delete_topic(self, topic_name: str) -> None:
        """Delete a topic"""
        if not self.topic_exists(topic_name):
            self.logger.info(f"Topic '{topic_name}' does not exist")
            return

        fs = self.admin_client.delete_topics([topic_name])

        try:
            fs[topic_name].result()
            time.sleep(2)
            self.logger.info(f"Topic '{topic_name}' deleted successfully")
        except Exception as e:
            self.logger.error(f"Failed to delete topic '{topic_name}': {e}")
            raise

    def get_topic_info(self, topic_name: str) -> dict:
        """Get detailed topic information"""
        try:
            metadata = self.admin_client.list_topics(topic=topic_name, timeout=10)
            topic = metadata.topics.get(topic_name)

            if not topic:
                return None

            return {
                'name': topic_name,
                'partitions': len(topic.partitions),
                'partition_info': [
                    {
                        'id': p.id,
                        'leader': p.leader,
                        'replicas': p.replicas,
                        'isrs': p.isrs
                    }
                    for p in topic.partitions.values()
                ]
            }
        except Exception as e:
            self.logger.error(f"Error getting topic info: {e}")
            raise