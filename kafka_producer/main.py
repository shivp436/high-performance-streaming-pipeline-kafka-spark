"""
Main entry point for high-performance Kafka producer
"""
import logging
import signal
import sys
import time
from multiprocessing import Process, Queue
from typing import List

from .config import (
    LogConfig,
    ProducerConfig,
    TopicConfig,
    WorkloadConfig
)
from .admin import KafkaTopicManager
from .worker import producer_worker


class KafkaProducerApplication:
    """Main application orchestrator"""

    def __init__(
            self,
            producer_config: ProducerConfig,
            topic_config: TopicConfig,
            workload_config: WorkloadConfig
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.producer_config = producer_config
        self.topic_config = topic_config
        self.workload_config = workload_config

        self.workers: List[Process] = []
        self.stats_queue = Queue()

    def setup_topic(self) -> None:
        """Create topic if it doesn't exist"""
        topic_manager = KafkaTopicManager(self.producer_config.bootstrap_servers)
        topic_manager.create_topic(self.topic_config)

    def start_workers(self) -> None:
        """Start all worker processes"""
        self.logger.info(
            f"Starting {self.workload_config.num_workers} workers "
            f"for {self.workload_config.duration_seconds} seconds"
        )

        for worker_id in range(self.workload_config.num_workers):
            process = Process(
                target=producer_worker,
                args=(
                    worker_id,
                    self.producer_config,
                    self.topic_config.name,
                    self.workload_config.duration_seconds,
                    self.workload_config.batch_size,
                    self.stats_queue
                )
            )
            process.start()
            self.workers.append(process)

    def wait_for_completion(self) -> None:
        """Wait for all workers to complete"""
        for worker in self.workers:
            worker.join()

    def collect_statistics(self) -> dict:
        """Collect and aggregate statistics from all workers"""
        total_messages = 0
        total_success = 0
        total_errors = 0
        worker_stats = []

        while not self.stats_queue.empty():
            stats = self.stats_queue.get()
            total_messages += stats['message_count']
            total_success += stats['success_count']
            total_errors += stats['error_count']
            worker_stats.append(stats)

            self.logger.info(
                f"Worker {stats['worker_id']}: "
                f"{stats['message_count']:,} msgs, "
                f"{stats['throughput']:,.0f} msg/s"
            )

        return {
            'total_messages': total_messages,
            'total_success': total_success,
            'total_errors': total_errors,
            'worker_stats': worker_stats
        }

    def print_summary(self, stats: dict, elapsed: float) -> None:
        """Print final summary"""
        overall_throughput = stats['total_messages'] / elapsed if elapsed > 0 else 0

        self.logger.info("=" * 80)
        self.logger.info("FINAL SUMMARY:")
        self.logger.info(f"Total Messages Sent: {stats['total_messages']:,}")
        self.logger.info(f"Total Success: {stats['total_success']:,}")
        self.logger.info(f"Total Errors: {stats['total_errors']:,}")
        self.logger.info(f"Total Duration: {elapsed:.2f} seconds")
        self.logger.info(f"Overall Throughput: {overall_throughput:,.0f} messages/second")
        self.logger.info(f"Workers Used: {self.workload_config.num_workers}")
        self.logger.info("=" * 80)

        if overall_throughput >= self.workload_config.target_throughput:
            self.logger.info(
                f"🎉 SUCCESS! Achieved target of "
                f"{self.workload_config.target_throughput:,}+ messages/second!"
            )
        else:
            self.logger.warning(
                f"⚠️  Did not reach {self.workload_config.target_throughput:,} target. "
                f"Consider tuning Kafka broker settings."
            )

    def run(self) -> None:
        """Run the complete producer workflow"""
        # Setup
        self.setup_topic()

        # Start workers
        start_time = time.time()
        self.start_workers()

        # Wait for completion
        self.wait_for_completion()
        total_elapsed = time.time() - start_time

        # Collect and display results
        stats = self.collect_statistics()
        self.print_summary(stats, total_elapsed)


def main():
    """Main entry point"""
    # Setup logging
    LogConfig.setup_logging()

    # Configuration
    producer_config = ProducerConfig(
        bootstrap_servers="localhost:29092,localhost:39092,localhost:49092",
        acks='all',  # Change to '1' for higher throughput
    )

    topic_config = TopicConfig(
        name='log_finance_transactions_v1',
        num_partitions=12,
        replication_factor=3
    )

    workload_config = WorkloadConfig(
        num_workers=None,  # Use all CPU cores
        duration_seconds=60,
        batch_size=1000,
        target_throughput=300000
    )

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        logging.info("Interrupted! Shutting down...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Run application
    app = KafkaProducerApplication(
        producer_config=producer_config,
        topic_config=topic_config,
        workload_config=workload_config
    )

    app.run()


if __name__ == "__main__":
    main()