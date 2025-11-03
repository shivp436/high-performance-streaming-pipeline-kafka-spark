"""
High-performance Kafka Producer Package
Optimized for 300K+ messages/second throughput
"""

__version__ = "1.0.0"
__author__ = "shivp436"

from .config import ProducerConfig, LogConfig
from .admin import KafkaTopicManager
from .producer import KafkaProducer
from .generator import FinanceRecordGenerator
from .worker import producer_worker

__all__ = [
    'ProducerConfig',
    'LogConfig',
    'KafkaTopicManager',
    'KafkaProducer',
    'FinanceRecordGenerator',
    'producer_worker'
]