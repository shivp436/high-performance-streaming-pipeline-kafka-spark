"""
Worker process for parallel message production
"""
import logging
import time
from multiprocessing import Queue
from typing import Dict, Any

from .config import ProducerConfig
from .producer import KafkaProducer
from .generator import FinanceRecordGenerator


def producer_worker(
        worker_id: int,
        producer_config: ProducerConfig,
        topic_name: str,
        duration_seconds: int,
        batch_size: int,
        stats_queue: Queue
) -> None:
    """
    Worker process that produces messages to Kafka

    Args:
        worker_id: Unique worker identifier
        producer_config: Producer configuration
        topic_name: Target topic name
        duration_seconds: How long to run
        batch_size: Number of records to generate per batch
        stats_queue: Queue to send statistics back to main process
    """
    logger = logging.getLogger(f"Worker-{worker_id}")
    logger.info(f"Worker {worker_id} starting...")

    # Initialize producer and generator
    producer = KafkaProducer(producer_config, process_id=worker_id)
    generator = FinanceRecordGenerator()

    start_time = time.time()
    message_count = 0

    try:
        while time.time() - start_time < duration_seconds:
            # Generate batch of records
            records = generator.generate_batch(batch_size)

            # Send all records in batch
            for record in records:
                # Use user_id as partition key for even distribution
                key = record['user_id'].encode('utf-8')
                producer.send(topic_name, record, key=key)
                message_count += 1

            # Periodic poll to keep queue flowing
            if message_count % 10000 == 0:
                producer.producer.poll(0)

        # Final flush
        producer.flush()

        elapsed = time.time() - start_time
        throughput = message_count / elapsed if elapsed > 0 else 0

        logger.info(
            f"Worker {worker_id} completed: "
            f"{message_count:,} messages in {elapsed:.2f}s "
            f"= {throughput:,.0f} msg/s"
        )

        # Send statistics back to main process
        stats: Dict[str, Any] = {
            'worker_id': worker_id,
            'message_count': message_count,
            'success_count': producer.success_count,
            'error_count': producer.error_count,
            'elapsed': elapsed,
            'throughput': throughput
        }
        stats_queue.put(stats)

    except KeyboardInterrupt:
        logger.info(f"Worker {worker_id} interrupted")
        producer.flush()
    except Exception as e:
        logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
        producer.flush()
        raise
