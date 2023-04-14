import argparse
import logging
import os
import signal
import config
from multiprocessing import Process, Event as MEvent
from threading import Thread, Event
from time import sleep
from typing import List, Union

from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

from consumer.common.message_handler import MessageHandler
from consumer.concurrent.concurrent_consumer import ConcurrentKafkaConsumer
from consumer.parallel.parallel_consumer import ParallelKafkaConsumer

parser = argparse.ArgumentParser()
parser.add_argument('--mode', choices=['concurrent', 'parallel'])

logging.basicConfig(level=logging.DEBUG,
                    format="[%(asctime)s.%(msecs)03d][%(threadName)s][%(levelname)s][%(name)s] - %(message)s",
                    datefmt='%H:%M:%S')

config = {
    'bootstrap_servers': config.KAFKA_BOOTSTRAP,
    'api_version': (2, 1, 0),
    'group_id': 'my_consumer',
    'client_id': 'my_client_id',
    'enable_auto_commit': False,
    'auto_offset_reset': 'earliest',
    'partition_assignment_strategy': (RangePartitionAssignor, RoundRobinPartitionAssignor)
}


def start_concurrent_consumer(topic, app_reference):
    logging.getLogger('kafka').setLevel(logging.WARNING)
    c = ConcurrentKafkaConsumer(
        kafka_config=config,
        event_handlers={'Event': MessageHandler()},
        max_workers=2,
        topic=topic)
    app_reference.append(c)
    c.start()


def start_parallel_consumer(topic):
    logging.basicConfig(level=logging.DEBUG,
                        format="[%(asctime)s.%(msecs)03d][%(threadName)s][%(levelname)s][%(name)s] - %(message)s",
                        datefmt='%H:%M:%S')
    logging.getLogger('kafka').setLevel(logging.WARNING)
    logging.getLogger().info('Starting topic %s... on PID: %s', topic, os.getpid())
    c = ParallelKafkaConsumer(
        kafka_config=config,
        event_handlers={'Event': MessageHandler()},
        max_workers=2,
        topic=topic)
    signal.signal(signal.SIGTERM, c.stop)
    signal.signal(signal.SIGINT, c.stop)
    c.start()


thread_stop_event = Event()
process_stop_event = MEvent()


def start_and_wait(pool: List[Union[Thread, Process]], stop_event):
    [p.start() for p in pool]

    while not stop_event.wait(5):
        for p in pool:
            if not p.is_alive():
                logging.getLogger().warning("%s crashed", p.name)
                # signal.raise_signal(signal.SIGINT)

    # Wait for graceful shutdown
    while all([p.is_alive() for p in pool]):
        logging.info('Waiting for shutdown... %s', [p.is_alive() for p in pool])
        sleep(1)


def _handle_stop_thread(*args):
    logging.info('Setting Threading stop event... %s', args)
    thread_stop_event.set()
    logging.info('Stopping consumers %s', [c for c in consumer_references])
    [c.stop() for c in consumer_references]


def _handle_stop_process(*args):
    logging.info('Setting Processing stop event...')
    process_stop_event.set()


if __name__ == '__main__':
    program_args = parser.parse_args()

    if program_args.mode == 'concurrent':
        signal.signal(signal.SIGTERM, _handle_stop_thread)
        signal.signal(signal.SIGINT, _handle_stop_thread)

        consumer_references = []

        threads = [
            # ConsumerThread-T1 will subscribe and handle all partitions of topic T1
            Thread(target=start_concurrent_consumer, args=('T1', consumer_references), daemon=False, name='ConsumerThread-T1'),
            # ConsumerThread-T2 will subscribe and handle all partitions of topic T2
            Thread(target=start_concurrent_consumer, args=('T2', consumer_references), daemon=False, name='ConsumerThread-T2')
        ]

        start_and_wait(threads, thread_stop_event)

    else:
        signal.signal(signal.SIGTERM, _handle_stop_process)
        signal.signal(signal.SIGINT, _handle_stop_process)

        processes = [
            # ConsumerProcess-T1 will subscribe and handle all partitions of topic T1
            Process(target=start_parallel_consumer, args=('T1',), daemon=False, name='ConsumerProcess-T1'),
            # ConsumerProcess-T2 will subscribe and handle all partitions of topic T2
            Process(target=start_parallel_consumer, args=('T2',), daemon=False, name='ConsumerProcess-T2')
        ]

        start_and_wait(processes, process_stop_event)
