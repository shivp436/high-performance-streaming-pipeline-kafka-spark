"""
High-performance Kafka producer
"""
import logging
import json
from typing import Optional, Any, Dict
from confluent_kafka import Producer

from .config import ProducerConfig


class KafkaProducer:
    """High-performance Kafka producer optimized for throughput"""

    def __init__(self, config: ProducerConfig, process_id: int = 0):
        self.logger = logging.getLogger(f"{self.__class__.__name__}-{process_id}")
        self.process_id = process_id
        self.config = config

        # Statistics
        self.success_count = 0
        self.error_count = 0

        try:
            self.producer = Producer(config.to_dict())
            self.logger.info(f"Producer {process_id} initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize producer: {e}")
            raise

    def _delivery_callback(self, err, msg):
        """Async delivery callback"""
        if err:
            self.error_count += 1
            self.logger.error(f"Delivery failed: {err}")
        else:
            self.success_count += 1

    def send(
            self,
            topic: str,
            value: Any,
            key: Optional[bytes] = None,
            headers: Optional[Dict[str, bytes]] = None
    ) -> None:
        """Send a message to Kafka (non-blocking)"""
        try:
            # Serialize value
            serialized_value = json.dumps(value).encode('utf-8')

            # Non-blocking produce
            self.producer.produce(
                topic,
                value=serialized_value,
                key=key,
                headers=headers,
                callback=self._delivery_callback
            )

            # Non-blocking poll
            self.producer.poll(0)

        except BufferError:
            # Queue is full - poll and retry
            self.logger.warning("Producer queue full, polling...")
            self.producer.poll(1)
            self.producer.produce(
                topic,
                value=serialized_value,
                key=key,
                callback=self._delivery_callback
            )
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            self.error_count += 1
            raise

    def flush(self, timeout: int = 30) -> int:
        """Flush pending messages"""
        try:
            pending = self.producer.flush(timeout)

            if pending > 0:
                self.logger.warning(f"{pending} messages still pending after flush")

            self.logger.info(
                f"Producer {self.process_id}: "
                f"Success={self.success_count:,}, "
                f"Errors={self.error_count:,}"
            )

            return pending

        except Exception as e:
            self.logger.error(f"Failed to flush producer: {e}")
            raise

    def close(self) -> None:
        """Close the producer"""
        self.flush()
        self.logger.info(f"Producer {self.process_id} closed")

    def get_stats(self) -> Dict[str, int]:
        """Get producer statistics"""
        return {
            'process_id': self.process_id,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'total_sent': self.success_count + self.error_count
        }