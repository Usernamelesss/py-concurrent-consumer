import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from typing import List, Dict

from kafka import TopicPartition

from .message import Message
from .types import PARTITION
from .worker import Worker


class ConsumerPoolManager:
    """
    This is a ThreadPool which dispatch jobs to an underlying pool of threads.
    The main difference from the standard ThreadPoolExecutor is that the worker thread which will queue a specific job
    is predictable this gives us the advantage to queue jobs in sync way (J2 will be executed after J1 in T1).

    A ConsumerPoolManager handles all partitions of a topic
    """

    def __init__(self, max_workers):
        self.max_workers = max_workers
        self._pool: Dict[int, Worker] = {}
        self._stop_event = Event()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._rebalance_lock = Lock()

    def dispatch(self, message: Message):
        """Predictably queue a kafka message in a worker"""
        if not self._stop_event.is_set():
            # Select which worker will process this message
            worker_id = self._choose_worker(message.partition)
            self._pool[worker_id].enqueue_message(message)
        else:
            raise RuntimeError(
                "Cannot dispatch new messages on a shutting down ConsumerPoolManager"
            )

    def shutdown(self) -> None:
        """Send a stop signal to all workers and wait until they are stopped"""
        self._stop_event.set()
        with ThreadPoolExecutor() as pool:
            for w in self._pool.values():
                pool.submit(
                    w.stop
                )  # Send stop signal to all workers and wait concurrently

    def add_workers(
        self, consumer: "ConcurrentKafkaConsumer", partitions: List[TopicPartition]
    ):
        """Assign the @param partitions to the workers."""
        if self._stop_event.is_set():
            return

        with self._rebalance_lock:
            for p in partitions:
                worker_id = self._choose_worker(p.partition)
                if self._pool.get(worker_id) is not None:
                    self.logger.debug(
                        "Assigning partition %s to worker %s", p, worker_id
                    )
                    self._pool.get(worker_id).assign_partition(p)

                else:
                    self.logger.debug("Adding new worker for partition %s", p)
                    self._pool[worker_id] = Worker(
                        event_handlers=consumer.event_handlers,
                        partition=p,
                        offset_manager=consumer.offset_manager,
                        name=f"Worker-{worker_id}({p.topic})",
                    )
                    self._pool[worker_id].start()

    def dispose_workers(self, partitions: List[TopicPartition]):
        """
        Revoke the partitions from the worker. If a worker has no partition assigned, the worker will be disposed,
        stopped and finally removed from the pool.
        """
        if self._stop_event.is_set():
            self.logger.warning(
                "Disposing workers on a shutting down ConsumerPoolManager, doing nothing..."
            )
            return

        with self._rebalance_lock:
            disposable_workers = set()

            for p in partitions:
                worker_id = self._choose_worker(p.partition)
                if self._pool.get(worker_id) is None:
                    continue

                self.logger.debug("Revoking partition %s from worker %s", p, worker_id)
                self._pool[worker_id].revoke_partition(p)

                if self._pool[worker_id].has_partitions():
                    # Worker has other partitions assigned
                    continue

                self.logger.debug(
                    "Worker %s has no partitions assigned, stopping", worker_id
                )
                disposable_workers.add(worker_id)

            if not disposable_workers:
                return

            # Send stop signal to all workers and wait concurrently
            with ThreadPoolExecutor() as pool:
                for worker_id in disposable_workers:
                    pool.submit(self._pool[worker_id].stop)
                    pool.submit(self._pool.pop, worker_id)

    def _choose_worker(self, partition: PARTITION) -> int:
        """
        Ensures that messages from the same partition are queued in the same worker so that a worker will always process
        in the right order the messages in the same partition
        """
        return partition % self.max_workers

    def is_alive(self):
        """
        Returns True is all threads in pool are alive
        """
        with self._rebalance_lock:
            return all([thread.is_alive() for thread in self._pool.values()])
