import logging
import sys
from queue import Full
from threading import Event, Thread
from time import sleep
from typing import Dict, List

import schedule
from confluent_kafka import Consumer, Message, TopicPartition
from retry import retry

from .fixture_handler import BaseEventHandler
from .pool_manager import ConsumerPoolManager

EVENT_NAME = str


def run_scheduled():
    while True:
        schedule.run_pending()
        sleep(1)


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
        self.workers = ConsumerPoolManager(max_workers=topic_partitions)  # todo configure max worker
        self._stop_event = Event()
        schedule.every(5).seconds.do(self.is_alive)
        self._health_check = Thread(target=run_scheduled)  # fixme if this thread die, we won't notice eventually failed jobs
        self._health_check.start()

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

                    if self._stop_event.is_set():
                        while self.workers.is_alive():
                            # wait for gracefully shutdown
                            continue
                        break

                    if msg is None:
                        self.logger.debug('None message')
                        # msg is None if poll timeouts without new messages
                        continue

                    if msg.error():
                        self.logger.error('Received error message from poll(): %s', msg.error())
                        continue

                    self.logger.debug('Dispatching Message[topic=%s,partition=%s,offset=%s]', msg.topic(), msg.partition(), msg.offset())
                    self.dispatch(msg)  # Blocks until the message is queued into a worker

            except Full:
                # If queue is full, we need to seek consumer to that offset and retry again
                # fixme seek() seems not safe on multiple partition topic. Remove this
                offset = TopicPartition(msg.topic(), msg.partition(), msg.offset())
                self.consumer.seek(offset)

                self.logger.warning("Queue is full, retrying later at offset %s", offset)
                continue

            except BaseException as e:
                self.logger.exception('Unrecoverable error in consumer %s', e)
                self.consumer.close()
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

    def is_alive(self):
        if self._stop_event.is_set():
            return

        if not self.workers.is_alive():
            self.logger.warning("Not all workers are running, stopping process")
            self._stop_and_exit()

    def _stop_and_exit(self):
        self.stop()
        sys.exit(1)
