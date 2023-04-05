import logging
from queue import Empty, Full
from threading import Thread, Event
from typing import Dict, Set

from kafka import TopicPartition
from retry import retry

from .message import Message

from .message_handler import MessageHandler
from .offset_manager import OffsetManager
from .queue import ClearableQueue


class Worker:
    """
    Worker is the actual handler, it dispatches Kafka records to the right application handler.
    It can handle more partitions from the same topic.

    Records from the same partition are enqueued to the same Worker, to meet order processing guarantee
    """

    MAX_QUEUE_SIZE = 500
    MAX_BLOCK_SECONDS = 10

    def __init__(
        self,
        event_handlers: Dict[str, MessageHandler],
        partition: TopicPartition,
        offset_manager: OffsetManager,
        name: str,
    ):
        self.event_handlers = event_handlers
        self.topic_name = partition.topic
        self._partitions: Set[int] = {partition.partition}
        self.name = name
        self._worker = Thread(
            target=self.handle_next_record, name=self.name, daemon=False
        )
        self.queue = ClearableQueue(maxsize=Worker.MAX_QUEUE_SIZE)
        self.logger = logging.getLogger(self.name)
        self.offset_manager = offset_manager
        self._stop_event = Event()

    def assign_partition(self, partition: TopicPartition):
        self._partitions.add(partition.partition)

    def revoke_partition(self, partition: TopicPartition):
        self._partitions.remove(partition.partition)

    def has_partitions(self) -> bool:
        return len(self._partitions) > 0

    def enqueue_message(self, message: Message) -> None:
        """Wait until the @param message is not enqueued or raises a Full exception after timeout"""
        self.queue.put(message, block=True, timeout=Worker.MAX_BLOCK_SECONDS)

    def is_alive(self):
        return self._worker.is_alive()

    def start(self):
        self._worker.start()

    def stop(self):
        """Stop the worker and wait until all messages in the Queue are discarded"""
        self.logger.debug("Stopping %s", self.name)
        self._stop_event.set()
        self._wakeup_worker()

        if self.is_alive():
            # If the worker is still alive, joining the Queue
            self.logger.debug('Joining %s Queue with size %s', self.name, self.queue.qsize())
            # Wait for all elements in queue to be processed --> cannot wait if not all task_done()
            self.queue.join()
            self.logger.debug("All elements in queue dequeued")

        self.logger.debug("%s stopped", self.name)

    def _wakeup_worker(self):
        """
        In case a Worker is idling in a queue.get() this will enqueue a None message which will immediately wake up the
        worker.
        """
        try:
            self.queue.put_nowait(None)
        except Full:
            pass

    def handle_next_record(self):
        """
        Get messages from the Queue and dispatch them to the right application handler
        """
        while not self._stop_event.is_set():
            try:
                # Remove and return an item from the queue
                message: Message = self.queue.get(
                    block=True, timeout=Worker.MAX_BLOCK_SECONDS
                )

                if message is None:
                    self.logger.debug('Found None message in Queue, exiting')
                    # stop() methods queues a None message to wake up worker thread if it's idling in queue.get()
                    # The worker thread will exit either by checking the stop_event or by handling a None message
                    self.queue.task_done()
                    break

                self._message_handler(message)

            except Empty:
                # self.logger.debug('Empty queue for more than %s seconds', Worker.MAX_BLOCK_SECONDS)
                continue

        self.logger.debug('Clearing queue')
        self.queue.clear()  # Clear the Queue after infinite loop to let queue.join() proceed
        self.logger.debug('Queue cleared')

    @retry(backoff=1.2, delay=0.1, max_delay=2)
    def _message_handler(self, message: Message):
        """
        Invoke the application handler and mark the message as processed when the handler returns without exceptions.

        If an exception is thrown the Handler will retry forever the same message.
        """
        if self._stop_event.is_set():
            self.queue.task_done()
            self.logger.debug(
                "%s is stopped, skipping message %s", self.name, message.key
            )
            return

        try:
            event_name = message.key
            self.logger.debug(
                "Received %s from topic %s at offset %s of partition %s",
                event_name,
                message.topic,
                message.offset,
                message.partition,
            )

            event_handler = self.event_handlers[
                event_name
            ]  # Pick the right application handler
            event_payload = event_handler.decode(
                message.value
            )  # Decode the message value

            event_handler.handle(event_payload)  # invoke the application handler

            self.offset_manager.mark_message(message)
            self.queue.task_done()  # this is a kind of counter on tasks in queue.

            self.logger.debug(
                "successfully consumed message at offset %s from topic %s",
                message.offset,
                message.topic,
            )

        except Exception as e:
            self.logger.exception("Error in %s: %s", self.name, e)
            raise e
