import sys
from enum import Enum
from multiprocessing.connection import Connection
from threading import Thread
from typing import Dict

from ..concurrent.concurrent_consumer import ConcurrentKafkaConsumer, RebalanceListener
from ..common.types import EVENT_NAME, HANDLER_INSTANCE, ConsumerTopicConfig


class ParallelKafkaConsumer(ConcurrentKafkaConsumer):

    def __init__(self,
                 kafka_config={},
                 topic='T1',
                 event_handlers: Dict[EVENT_NAME, HANDLER_INSTANCE] = {},
                 consumer=None,
                 consumer_config: ConsumerTopicConfig = ConsumerTopicConfig(),
                 max_workers: int = 1):
        super().__init__(kafka_config, topic, event_handlers, consumer, consumer_config, max_workers)

        # This consumer starts in a new Python process, we need to create few more threads
        self._main_routine = Thread(target=self.consume, daemon=True, name=self.consumer_name)
        # self._remote_command_handler = Thread(target=self.handle_rpi, daemon=True, name=f'CommandHandler[{topic}]')

    def start(self):
        self.logger.info('Starting %s in PARALLEL mode', self.consumer_name)

        self.consumer.subscribe(topics=[self.config['topic']], listener=RebalanceListener(self))

        self._main_routine.start()
        # self._remote_command_handler.start()

        while not self._stop_event.wait(5):
            if not self.workers.is_alive():
                self.logger.warning("Not all workers are running, stopping process")
                self._stop_and_exit()

    def _stop_and_exit(self):
        self.stop()
        sys.exit(1)
