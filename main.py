import os
import signal

from consumer.consumer import ConcurrentKafkaConsumer
import logging
import tracemalloc

from consumer.fixture_handler import BaseEventHandler

logging.basicConfig(level=logging.DEBUG,
                    format="[%(asctime)s.%(msecs)03d][%(threadName)s][%(levelname)s][%(name)s] - %(message)s",
                    datefmt='%H:%M:%S',)

c = ConcurrentKafkaConsumer(config={
    'bootstrap.servers': 'localhost:9093',
    'group.id': 'myid',
    'enable.auto.commit': False,
    # 'enable.auto.offset.store': False
}, event_handlers={'Event': BaseEventHandler()})


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, c.stop)
    signal.signal(signal.SIGINT, c.stop)
    tracemalloc.start()
    logging.getLogger().info('Starting... on PID: %s', os.getpid())
    try:
        c.start()

    finally:
        current, peak = tracemalloc.get_traced_memory()
        print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
        tracemalloc.stop()

