import random
from collections import namedtuple

from kafka import KafkaProducer

# TODO write a self contained script/test which produces a lot of messages

Msg = namedtuple('Msg', 'topic value key partition')


def generate_records(topic, num=1000, num_partitions=100):
    for i in range(num):
        payload = f'Message: {i}'
        yield Msg(topic=topic,
                  value=bytes(payload, encoding='utf-8'),
                  key=b'Event',
                  partition=random.randrange(start=1, stop=num_partitions))
    # yield [Msg(topic=topic,
    #            value=f'Message: {i}',
    #            key='Event',
    #            partition=random.randrange(start=1, stop=num_partitions))
    #        for i in range(num)]


producer = KafkaProducer(bootstrap_servers='localhost:9093')
for t1_msg, t2_msg in zip(generate_records('T1'), generate_records('T2')):
    producer.send(topic=t1_msg.topic, value=t1_msg.value, key=t1_msg.key, partition=t1_msg.partition)
    producer.send(topic=t2_msg.topic, value=t2_msg.value, key=t2_msg.key, partition=t2_msg.partition)

    # producer.flush()

producer.flush()
