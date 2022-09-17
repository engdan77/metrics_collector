import dataclasses
from typing import Protocol
from abc import ABC


class AsyncService(Protocol):
    def start(self):
        ...


@dataclasses.dataclass
class BaseAction(ABC):
    def __repr__(self):
        return str(self.__dict__)


@dataclasses.dataclass
class BaseScheduleParams(ABC):
    def __format__(self, format_spec):
        return str(self.__dict__)