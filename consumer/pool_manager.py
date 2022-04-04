import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import List, Dict

from confluent_kafka import Message, TopicPartition

from .worker import Worker

PARTITION = int


class ConsumerPoolManager:
    """
    This is a ThreadPool which dispatch jobs to an underlying pool of threads.
    The main difference from the standard ThreadPoolExecutor is that the worker thread which will queue a specific job
    is predictable this gives us the advantage to queue jobs in sync way (J2 will be executed after J1 in T1).
    """

    def __init__(self, max_workers):
        self.max_workers = max_workers
        self._pool: Dict[int, Worker] = {}
        self._stop_event = Event()
        self.logger = logging.getLogger(self.__class__.__name__)

    def dispatch(self, message: Message):
        if not self._stop_event.is_set():
            # Select which worker will process this message
            worker = self._choose_worker(message.partition())
            self._pool[worker].enqueue_message(message)
        else:
            raise RuntimeError("Cannot dispatch new messages on a shutting down ConsumerPoolManager")

    def shutdown(self) -> None:
        self._stop_event.set()
        with ThreadPoolExecutor() as pool:
            for w in self._pool.values():
                pool.submit(w.stop)  # Send stop signal to all workers and wait concurrently

    def add_workers(self, consumer: "ConcurrentKafkaConsumer", partitions: List[TopicPartition]):
        if self._stop_event.is_set():
            return

        for p in partitions:
            worker = self._choose_worker(p.partition)
            if self._pool.get(worker) is not None:
                self.logger.debug("Adding worker for partition %s already assigned to a worker, skipping", p)
                continue
            self.logger.debug('Adding worker for partition %s', p)
            self._pool[worker] = Worker(kafka_consumer=consumer.consumer,
                                        event_handlers=consumer.event_handlers,
                                        partition=p)
            self._pool[worker].start()

    def dispose_workers(self, partitions: List[TopicPartition]):
        if self._stop_event.is_set():
            return

        # FIXME: if there are fewer workers than partitions, this is not safe, because we could remove a worker used by
        #        another partition
        for p in partitions:
            worker = self._choose_worker(p.partition)
            if self._pool.get(worker) is None:
                continue

            self.logger.debug('Removing worker for partition %s', p)

            with ThreadPoolExecutor() as pool:
                pool.submit(self._pool[worker].stop)
                pool.submit(self._pool.pop, worker)
                # self._pool[worker].stop()
                # self._pool.pop(worker)

    def _choose_worker(self, partition: PARTITION) -> int:
        return partition % self.max_workers
