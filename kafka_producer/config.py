"""
Configuration classes for Kafka producer
"""
import logging
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class ProducerConfig:
    """Producer configuration with optimized defaults"""

    # Connection
    bootstrap_servers: str = "localhost:29092,localhost:39092,localhost:49092"

    # Batching - Key to high throughput
    batch_size: int = 1000000  # 1MB batches
    linger_ms: int = 10  # Wait 10ms to batch messages
    batch_num_messages: int = 10000  # Batch up to 10K messages

    # Compression
    compression_type: str = 'lz4'  # Fast compression

    # Buffer Management
    queue_buffering_max_messages: int = 10000000  # 10M message queue
    queue_buffering_max_kbytes: int = 4194304  # 4GB buffer

    # Network Optimization
    socket_send_buffer_bytes: int = 10485760  # 10MB
    socket_receive_buffer_bytes: int = 10485760  # 10MB

    # Reliability vs Speed
    acks: str = '1'  # Change to '1' for faster throughput, 'all' for max safety
    max_in_flight_requests: int = 10

    # Retries
    retries: int = 3
    retry_backoff_ms: int = 100

    # Timeouts
    request_timeout_ms: int = 30000
    delivery_timeout_ms: int = 120000

    # Performance
    enable_idempotence: bool = False  # Disable for max speed

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to Kafka producer config dict"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'batch.size': self.batch_size,
            'linger.ms': self.linger_ms,
            'batch.num.messages': self.batch_num_messages,
            'compression.type': self.compression_type,
            'queue.buffering.max.messages': self.queue_buffering_max_messages,
            'queue.buffering.max.kbytes': self.queue_buffering_max_kbytes,
            'socket.send.buffer.bytes': self.socket_send_buffer_bytes,
            'socket.receive.buffer.bytes': self.socket_receive_buffer_bytes,
            'acks': self.acks,
            'max.in.flight.requests.per.connection': self.max_in_flight_requests,
            'retries': self.retries,
            'retry.backoff.ms': self.retry_backoff_ms,
            'request.timeout.ms': self.request_timeout_ms,
            'delivery.timeout.ms': self.delivery_timeout_ms,
            'enable.idempotence': self.enable_idempotence,
        }


@dataclass
class TopicConfig:
    """Topic configuration"""
    name: str
    num_partitions: int = 12
    replication_factor: int = 3
    compression_type: str = 'lz4'
    min_insync_replicas: str = '2'

    def get_topic_config(self) -> Dict[str, str]:
        """Get topic-level config"""
        return {
            'compression.type': self.compression_type,
            'min.insync.replicas': self.min_insync_replicas
        }


@dataclass
class WorkloadConfig:
    """Workload configuration"""
    num_workers: int = None  # None = use cpu_count()
    duration_seconds: int = 60
    batch_size: int = 1000  # Records per generation batch
    target_throughput: int = 300000  # Messages per second

    def __post_init__(self):
        if self.num_workers is None:
            from multiprocessing import cpu_count
            self.num_workers = cpu_count()


class LogConfig:
    """Logging configuration"""

    @staticmethod
    def setup_logging(level=logging.INFO):
        """Setup logging configuration"""
        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=level,
            datefmt='%Y-%m-%d %H:%M:%S'
        )