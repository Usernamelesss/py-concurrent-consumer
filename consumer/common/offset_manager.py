import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Dict, List

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

from .message import Message
from .types import PARTITION


class OffsetManager:
    """
    Thread-safe handler of processed offsets for a topic. The idea comes from Golang's Sarama library.
    Each Worker marks concurrently every single message as processed. The OffsetManager ensures that we track only the
    most recent offset for each partition assigned to the consumer and periodically these offsets are flushed to the
    broker. This ensures high performance and throughput.

    A few notes behind this class:
        - Using the kafka's built-in autocommit, we can incur in data loss because the consumer can flush offsets to the
          broker before they are actually processed by the application
          (https://newrelic.com/blog/best-practices/kafka-consumer-config-auto-commit-data-loss).
        - Manually commit an offset to the broker after each message is processed slow down the entire
          application because you will wait the IO for each message.
        - Use async-commit can lead to huge data duplication if the consumer has a high message rate. Furthermore, some
          consumer implementation wrongly uses retry policies on a failed async commit.
    """

    def __init__(
        self, consumer: KafkaConsumer, topic: str, auto_commit_interval_sec: float
    ):
        self._lock = Lock()
        self._offsets: Dict[
            PARTITION, Message
        ] = dict()  # Keep the last processed message for a partition
        self._flushed = True
        self._consumer = consumer
        self._last_run = None
        self._interval = auto_commit_interval_sec
        self._logger = logging.getLogger(f"OffsetManager[{topic}]")
        self._executor = ThreadPoolExecutor(
            thread_name_prefix=f"OffsetManager[{topic}]", max_workers=1
        )

    def mark_message(self, message: Message):
        """
        Mark the given message as processed, this won't necessarily trigger a commit
        """
        with self._lock:
            try:
                if message.offset > self._offsets[message.partition].offset:
                    self._offsets[message.partition] = message
                    self._flushed = False

            except KeyError:
                # First time a message from this partition was processed
                self._offsets[message.partition] = message
                self._flushed = False

            finally:
                if not self._flushed and self._should_commit():
                    # If new offsets were queued and we haven't committed for a while spawn a sync commit in background
                    future = self._executor.submit(self.commit_offsets)
                    future.add_done_callback(self._commit_callback)

    def clean_offsets(self, revoked_partitions: List[TopicPartition]):
        """
        Remove the revoked partitions
        """
        with self._lock:
            for p in revoked_partitions:
                self._offsets.pop(p.partition, None)

    def shutdown(self):
        self._executor.shutdown()

    def commit_offsets(self):
        with self._lock:
            if self._flushed is True:
                # No new offsets to commit
                return

            offsets = self._get_latest_offsets()
            if not offsets:
                return

            self._flushed = True
            self._last_run = time.monotonic()

        # We can commit offsets releasing the lock, so other threads can continue processing messages while the caller
        # waits for IO
        self._sync_commit(offsets)

    def _sync_commit(self, offsets: Dict[TopicPartition, OffsetAndMetadata]):
        """
        Blocks until either the commit succeeds or an unrecoverable error is encountered
        """
        self._consumer.commit(offsets=offsets)

    def _get_latest_offsets(self) -> Dict[TopicPartition, OffsetAndMetadata]:
        """
        Return a list of topic partition to be committed to the broker.

        Note: By convention, committed offsets reflect the next message to be consumed, not the last message consumed.
        """

        return {
            TopicPartition(topic=offset.topic, partition=partition): OffsetAndMetadata(
                offset=offset.offset + 1, metadata=None
            )
            for partition, offset in self._offsets.items()
        }

    def _commit_callback(self, future):
        if future.exception() is not None:
            self._logger.warning("Error committing offsets: %s", future.exception())

    def _should_commit(self) -> bool:
        if self._last_run is None:
            return True

        return (time.monotonic() - self._last_run) > self._interval
