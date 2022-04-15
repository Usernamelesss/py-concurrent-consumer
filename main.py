#!/usr/bin/env python3
import logging
import os
import signal
import sys
import tracemalloc
from multiprocessing import Process
from threading import Event
from time import sleep

from consumer.consumer import ConcurrentKafkaConsumer
from consumer.fixture_handler import BaseEventHandler

logging.basicConfig(level=logging.DEBUG,
                    format="[%(asctime)s.%(msecs)03d][%(threadName)s][%(levelname)s][%(name)s] - %(message)s",
                    datefmt='%H:%M:%S')

config = {
    'bootstrap_servers': 'localhost:9093',
    'api_version': '2.1.0',
    'group_id': 'my_consumer',
    'client_id': 'my_client_id',
    'fetching': {
        'offset_reset_policy': 'earliest',
        'max_records': None,
        'max_interval': None
    }
}


def start_consumer(topic):
    logging.basicConfig(level=logging.DEBUG,
                        format="[%(asctime)s.%(msecs)03d][%(threadName)s][%(levelname)s][%(name)s] - %(message)s",
                        datefmt='%H:%M:%S')
    logging.getLogger('schedule').setLevel(logging.INFO)
    logging.getLogger().info('Starting topic %s... on PID: %s', topic, os.getpid())
    c = ConcurrentKafkaConsumer(
        config={
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'my_consumer',
            'enable.auto.commit': False,
            'auto.offset.reset': 'beginning'
            # 'enable.auto.offset.store': False
        },
        event_handlers={'Event': BaseEventHandler()},
        topic=topic)
    signal.signal(signal.SIGTERM, c.stop)
    signal.signal(signal.SIGINT, c.stop)
    c.start()


processes = [Process(target=start_consumer, args=('T1',), daemon=False, name='ConsumerProcess-T1'),
             Process(target=start_consumer, args=('T2',), daemon=False, name='ConsumerProcess-T2')]
stop = Event()


def cleanup(*args, **kwargs):
    stop.set()
    for proc in processes:
        proc.terminate()


if __name__ == "__main__":

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    tracemalloc.start()
    logging.getLogger().info('Starting... on PID: %s', os.getpid())
    try:
        # c.start()
        for p in processes:
            p.start()

        while not stop.is_set():
            sleep(5)
            for p in processes:
                if not p.is_alive():
                    logging.getLogger().info("%s crashed, stopping service", p.name)
                    signal.raise_signal(signal.SIGINT)

    finally:
        current, peak = tracemalloc.get_traced_memory()
        print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
        tracemalloc.stop()
