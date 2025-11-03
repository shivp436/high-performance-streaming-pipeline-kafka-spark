"""
Data generators for Kafka messages
"""
import uuid
import random
import time
from typing import List, Dict, Any
from abc import ABC, abstractmethod


class DataGenerator(ABC):
    """Abstract base class for data generators"""

    @abstractmethod
    def generate_single(self) -> Dict[str, Any]:
        """Generate a single record"""
        pass

    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of records"""
        return [self.generate_single() for _ in range(batch_size)]


class FinanceRecordGenerator(DataGenerator):
    """Generate fake finance transaction records"""

    # Pre-define choices for performance
    CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY']
    MERCHANTS = ['Amazon', 'eBay', 'Walmart', 'Target']
    TRANSACTION_TYPES = ['purchase', 'refund', 'withdrawal', 'deposit']
    PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']

    def __init__(self, user_id_range: int = 1000):
        self.user_id_range = user_id_range

    def generate_single(self) -> Dict[str, Any]:
        """Generate a single finance transaction record"""
        return {
            'transaction_id': str(uuid.uuid4()),
            'user_id': f"user_{random.randint(1, self.user_id_range)}",
            'amount': round(random.uniform(10.0, 1000.0), 2),
            'currency': random.choice(self.CURRENCIES),
            'transaction_time': int(time.time()),
            'merchant': random.choice(self.MERCHANTS),
            'is_international': random.choice([True, False]),
            'transaction_type': random.choice(self.TRANSACTION_TYPES),
            'payment_method': random.choice(self.PAYMENT_METHODS)
        }
