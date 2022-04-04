import logging
from queue import Full
from threading import Event
from typing import Dict, List

from confluent_kafka import Consumer, Message, TopicPartition

from .fixture_handler import BaseEventHandler
from .pool_manager import ConsumerPoolManager

EVENT_NAME = str


class ConcurrentKafkaConsumer:
    """
    TODO write doc
    """

    def __init__(self, config, topic='T1', topic_partitions: int = 100,
                 event_handlers: Dict[EVENT_NAME, BaseEventHandler] = {}):
        self.config = config
        self.event_handlers: Dict[EVENT_NAME, BaseEventHandler] = event_handlers
        self.logger = logging.getLogger(f'{self.__class__.__name__}[{topic}]')
        self.consumer = Consumer(config, logger=self.logger)  # todo adjust config
        self.config['topic'] = topic
        self.workers = ConsumerPoolManager(max_workers=topic_partitions)
        self._stop_event = Event()

    def start(self):
        # self.consumer.subscribe(topics=[self.config['topic']], on_assign=self.on_assign, on_lost=self.on_lost, on_revoke=self.on_revoke)
        self.consumer.subscribe(topics=[self.config['topic']], on_assign=self.on_assign, on_revoke=self.on_revoke)
        self.consume()

    def stop(self, *args):
        self.logger.info('Stopping %s on topic %s', self.__class__.__name__, self.config['topic'])
        self._stop_event.set()
        self.workers.shutdown()
        self.logger.info('PartitionWorker pool stopped, closing consumer...')
        self.consumer.close()
        self.logger.info('Consumer stopped')

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]):
        self.workers.add_workers(self, partitions)
        self.logger.info("Successfully assigned %s partitions: %s", len(partitions), [x.partition for x in partitions])

    def on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]):
        self.workers.dispose_workers(partitions)
        self.logger.info("Successfully removed %s partitions: %s", len(partitions), [x.partition for x in partitions])

    def consume(self):
        while not self._stop_event.is_set():
            try:
                messages: List[Message] = self.consumer.consume(num_messages=500, timeout=1)
                for msg in messages:
                    if msg is None:
                        # msg is None if poll timeouts without new messages
                        continue

                    if msg.error():
                        self.logger.error('Received error message from poll(): %s', msg.error())
                        continue

                    self.workers.dispatch(msg)  # Blocks until the message is queued into a worker

            except Full:
                # If queue is full, we need to seek consumer to that offset and retry again
                offset = TopicPartition(msg.topic(), msg.partition(), msg.offset())
                self.consumer.seek(offset)

                self.logger.warning("Queue is full, retrying later at offset %s", offset)
                continue

            except BaseException as e:
                self.logger.exception('Unrecoverable error in consumer %s', e)
                self.consumer.close()
                raise e
