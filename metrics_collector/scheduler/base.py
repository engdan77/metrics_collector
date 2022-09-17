import dataclasses
from typing import Protocol
from abc import ABC, abstractmethod


class AsyncService(Protocol):
    def start(self):
        ...


@dataclasses.dataclass
class BaseAction(ABC):
    """Used for scheduling report to e.g. know whether to email to cache"""
    def __repr__(self):
        return str(self.__dict__)

    @abstractmethod
    def run(self):
        """Implement logic for executing this action"""
        ...


@dataclasses.dataclass
class BaseScheduleParams(ABC):
    """Baseclass for scheduling parameters"""
    def __format__(self, format_spec):
        return str(self.__dict__)