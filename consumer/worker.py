import logging
from queue import Queue, Empty
from threading import Thread, Lock, Event
from typing import Dict, List

from confluent_kafka import Message, KafkaException, TopicPartition, Consumer
from retry import retry

from .exceptions import CommitFailedError
from lib_commons.consumer.handler import BaseEventHandler


class Worker:
    """
    TODO allow to handle more than one partition to a Worker, so we can fix the FIXME in dispose_workers
    """
    MAX_QUEUE_SIZE = 100
    MAX_BLOCK_SECONDS = 1

    def __init__(self, kafka_consumer: Consumer, event_handlers: Dict[str, BaseEventHandler],
                 partition: TopicPartition):
        # confluent Consumer should be thread safe https://github.com/confluentinc/confluent-kafka-python/issues/460
        self.consumer = kafka_consumer
        self.event_handlers = event_handlers
        self.topic_name = partition.topic
        self.name = f'PartitionWorker[name={self.topic_name},partitions={partition.partition}]'
        self._worker = Thread(target=self.handle_next_record, name=self.name, daemon=False)
        self.queue = Queue(maxsize=Worker.MAX_QUEUE_SIZE)
        self.logger = logging.getLogger(self.name)
        self._shutdown_lock = Lock()
        self._stop_event = Event()
        self._lock_queue = Lock()

    def enqueue_message(self, message: Message) -> None:
        if not self._stop_event.is_set():
            self.queue.put(message, block=True,
                           timeout=Worker.MAX_BLOCK_SECONDS)  # todo think about block, timeout --> possible deadlock with clear Queue
        else:
            raise RuntimeError("Cannot enqueue new messages on a shutting down worker")

    def is_alive(self):
        return self._worker.is_alive()

    def start(self):
        self._worker.start()  # FIXME non idempotent call --> RuntimeError: threads can only be started once

    def stop(self):
        self.logger.debug('Stopping %s', self.name)
        self._stop_event.set()
        # Wait for all elements in queue to be processed --> cannot wait if not all task_done()
        self.queue.join()
        self.logger.debug('All elements in queue dequeued')
        # Wait for thread to finish its execution
        self._worker.join()
        self.logger.debug('%s stopped', self.name)

    def handle_next_record(self):
        while True:
            try:
                # Remove and return an item from the queue
                message: Message = self.queue.get(block=True, timeout=Worker.MAX_BLOCK_SECONDS)

                self._message_handler(message)

            except Empty:
                # self.logger.debug('Empty queue for more than %s seconds', Worker.MAX_BLOCK_SECONDS)
                if self._stop_event.is_set():
                    return
                continue

            # except BaseException as e:
            #     # An error here means that queue is compromised: we could not have consumed the message,
            #     # but it has been already removed from queue. We must seek the offset and populate again the queue
            #     self.logger.exception("Error in %s: %s", self.name, e)  # todo this should be lower than error
            #     with self._lock_queue:
            #         self.queue = Queue(maxsize=Worker.MAX_QUEUE_SIZE)  # clear the queue
            #     self.logger.debug('Queue restarted due to an exception. Seeking offset...')
            #     self._seek_to_current(message)
            #     continue

    @retry(backoff=1.2, delay=0.1, max_delay=2)
    def _message_handler(self, message: Message):
        if self._stop_event.is_set():
            # This will dequeue all element from the Queue without processing them
            self.queue.task_done()
            self.logger.info('%s is stopped, skipping message %s', self.name, message.key())
            return

        try:
            event_name = message.key().decode('utf-8')
            self.logger.debug('Received %s from topic %s at offset %s of partition %s', event_name, message.topic(),
                              message.offset(), message.partition())

            event_handler = self.event_handlers[event_name]
            event_payload = event_handler.decode(message.value())

            event_handler.handle(event_payload)

            self.queue.task_done()  # check the docs, this is a kind of counter on tasks in queue.
            self._commit_offset(message)

            self.logger.info("successfully consumed %s from %s", event_payload.event_id, event_payload.topic)

        except Exception as e:
            self.logger.exception("Error in %s: %s", self.name, e)
            raise e

    def _commit_offset(self, message: Message):
        commit_result: List[TopicPartition] = self.consumer.commit(message=message, asynchronous=False)
        for res in commit_result:
            if res.error is not None:
                self.logger.debug('Failed to commit message %s', message)
                raise CommitFailedError(res)
