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
                    datefmt='%H:%M:%S', stream=sys.stdout)


def start_consumer(topic):
    logging.getLogger().info('Starting topic %s... on PID: %s', topic, os.getpid())
    c = ConcurrentKafkaConsumer(
        config={
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'myid',
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
            # 'enable.auto.offset.store': False
        },
        event_handlers={'Event': BaseEventHandler()},
        topic=topic)
    signal.signal(signal.SIGTERM, c.stop)
    signal.signal(signal.SIGINT, c.stop)
    c.start()


processes = [Process(target=start_consumer, args=('T1',), daemon=False),
             Process(target=start_consumer, args=('T2',), daemon=False)]
stop = Event()


def cleanup(*args, **kwargs):
    stop.set()
    for proc in processes:
        proc.terminate()


if __name__ == '__main__':
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

    finally:
        current, peak = tracemalloc.get_traced_memory()
        print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
        tracemalloc.stop()
