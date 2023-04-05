from dataclasses import dataclass

from kafka.consumer.fetcher import ConsumerRecord


@dataclass(eq=True)
class Message:
    __slots__ = ["key", "value", "topic", "offset", "partition"]

    def __init__(self, key=None, value=None, topic=None, offset=None, partition=None):
        self.key = key
        self.value = value
        self.topic = topic
        self.offset = offset
        self.partition = partition

    @staticmethod
    def from_record(record: ConsumerRecord):
        return Message(
            key=record.key,
            value=record.value,
            topic=record.topic,
            offset=record.offset,
            partition=record.partition,
        )
