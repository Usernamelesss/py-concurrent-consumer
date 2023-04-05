from enum import Enum
from typing import Type

from .message_handler import MessageHandler

PARTITION = int
EVENT_NAME = str
HANDLER_NAME = str
HANDLER_CLASS = Type[MessageHandler]
HANDLER_INSTANCE = MessageHandler


class ConsumerStartMode(Enum):
    CONCURRENT = 0
    PARALLEL = 1


class ConsumerTopicConfig:
    """
    Holds topic-specific consumer configurations
    """

    def __init__(
        self,
        mode: ConsumerStartMode = ConsumerStartMode.CONCURRENT,
        autocommit_interval_sec: float = 0.5,
    ):
        self.mode: ConsumerStartMode = mode
        self.autocommit_interval_sec: float = autocommit_interval_sec
