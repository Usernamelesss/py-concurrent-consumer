import itertools
import logging
import time
from queue import Full
from threading import Event
from typing import Dict, List

from kafka import KafkaConsumer, ConsumerRebalanceListener, TopicPartition
from kafka.consumer.fetcher import ConsumerRecord
from retry import retry

from ..common.message import Message
from ..common.offset_manager import OffsetManager
from ..common.pool_manager import ConsumerPoolManager
from ..common.message_handler import MessageHandler
from ..common.types import EVENT_NAME, ConsumerTopicConfig


class ConcurrentKafkaConsumer:
    def __init__(
        self,
        kafka_config: dict = {},
        topic: str = "T1",
        event_handlers: Dict[EVENT_NAME, MessageHandler] = {},
        consumer=None,
        consumer_config: ConsumerTopicConfig = ConsumerTopicConfig(),
        max_workers: int = 1,
    ):
        """
        @param kafka_config: Dictionary containing the consumer configuration for the kafka-python library
        @param topic: The topic name, this consumer is designed to be 'one per topic'
        @param event_handlers: Dictionary containing a mapping between handled messages
            (a consumer may not be interested in all topic events) and their handler class.
            i.e. {'Event1': MyEvent1HandlerClass}
        @param consumer: optional, a pre-instantiated consumer
        @param consumer_config: The consumer specific config, like auto-commit interval, starting mode (concurrent or parallel)
        @param max_workers: Number of worker threads handling the received messages concurrently
        """
        self.config = kafka_config.copy()
        self.consumer_config = consumer_config
        self.event_handlers: Dict[EVENT_NAME, MessageHandler] = event_handlers
        self.consumer_name = f"KafkaConsumer[{topic}]"
        self.logger = logging.getLogger(self.consumer_name)
        if consumer is None:
            self.consumer: KafkaConsumer = KafkaConsumer(**kafka_config)
        else:
            self.consumer: KafkaConsumer = consumer
        self.config["topic"] = topic
        self.offset_manager = OffsetManager(
            consumer=self.consumer,
            topic=topic,
            auto_commit_interval_sec=consumer_config.autocommit_interval_sec,
        )
        self.workers = ConsumerPoolManager(max_workers=max_workers)
        self._stop_event = Event()

    def start(self):
        """Start the consumer with an infinite loop"""
        self.logger.info("Starting %s in CONCURRENT mode", self.consumer_name)
        self.consumer.subscribe(
            topics=[self.config["topic"]], listener=RebalanceListener(self)
        )

        self.consume()

    def stop(self, *args):
        """Stop the consumer sending a stop signal to the worker pool and flush all processed offsets to the broker."""
        self.logger.info(
            "Stopping %s on topic %s", self.__class__.__name__, self.config["topic"]
        )
        self._stop_event.set()
        self.workers.shutdown()
        self.offset_manager.commit_offsets()  # Flush all processed offsets to the broker
        self.offset_manager.shutdown()
        self.logger.info("All processed offsets flushed, closing consumer...")
        self.consumer.unsubscribe()
        self.consumer.close(autocommit=False)
        self.logger.info("Consumer stopped")

    def on_assign(self, consumer: KafkaConsumer, partitions: List[TopicPartition]):
        """Callback invoked by kafka-python when @param partitions are assigned to this consumer"""

        start = time.monotonic()
        self.workers.add_workers(self, partitions)
        self.logger.info(
            "Successfully assigned %s partitions in %.02f ms: %s",
            len(partitions),
            (time.monotonic() - start) * 1000,
            [x.partition for x in partitions],
        )

    def on_revoke(self, consumer: KafkaConsumer, partitions: List[TopicPartition]):
        """Callback invoked by kafka-python when @param partitions are revoked from this consumer"""

        start = time.monotonic()
        self.workers.dispose_workers(partitions)
        # We commit processed offsets in this callback to prevent duplicates as doc suggest
        self.offset_manager.commit_offsets()
        self.offset_manager.clean_offsets(partitions)
        self.logger.info(
            "Successfully revoked %s partitions in %.02f ms: %s",
            len(partitions),
            (time.monotonic() - start) * 1000,
            [x.partition for x in partitions],
        )

    def consume(self):
        while not self._stop_event.is_set():
            try:
                messages: Dict[
                    TopicPartition, List[ConsumerRecord]
                ] = self.consumer.poll(
                    max_records=1000, update_offsets=False, timeout_ms=5000
                )

                # Flat nested list from [[1, 2, 3], [1, 2, 3, 4, 5]] to [1, 2, 3, 1, 2, 3, 4, 5]
                messages: List[ConsumerRecord] = list(
                    itertools.chain(*messages.values())
                )

                for message in messages:  # type: ConsumerRecord
                    if self._stop_event.is_set():
                        # consumer stopped
                        return

                    message: Message = Message.from_record(message)

                    message.key = message.key.decode(
                        "utf-8"
                    )  # Decode message key from byte to str
                    if self.event_handlers.get(message.key) is None:
                        self.logger.debug(
                            "No handler defined for %s, skipping.", message.key
                        )
                        self.offset_manager.mark_message(message)
                        continue

                    self.logger.debug(
                        "Dispatching Message[key=%s,topic=%s,partition=%s,offset=%s]",
                        message.key,
                        message.topic,
                        message.partition,
                        message.offset,
                    )
                    self.dispatch(
                        message
                    )  # Blocks until the message is successfully queued into a worker

            except BaseException as e:
                self.logger.critical(
                    "Unrecoverable error in consumer %s", e, exc_info=True
                )
                self.consumer.close(autocommit=False)
                raise e

    @retry(exceptions=Full, tries=-1, delay=0.1, max_delay=2, backoff=1.2)
    def dispatch(self, msg: Message):
        """
        Dispatch a message to a worker, if it's under heavy load it raises a Full exception and the @retry decorator
        ensures we keep trying
        """
        if self._stop_event.is_set():
            return

        self.workers.dispatch(msg)

    def _has_healthy_connections(self) -> bool:
        return any([connection.connected() for connection in self._get_connections()])

    def _get_connections(self):
        return list(self.consumer._client._conns.values())

    def is_alive(self):
        return self.workers.is_alive()

    def is_ready(self):
        return self._has_healthy_connections()


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer: ConcurrentKafkaConsumer):
        self._consumer = consumer

    def on_partitions_revoked(self, revoked: List[TopicPartition]):
        self._consumer.on_revoke(self._consumer.consumer, revoked)

    def on_partitions_assigned(self, assigned: List[TopicPartition]):
        self._consumer.on_assign(self._consumer.consumer, assigned)
