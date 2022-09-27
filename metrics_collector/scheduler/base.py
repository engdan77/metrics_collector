import dataclasses
from enum import Enum
from typing import Protocol
from abc import ABC, abstractmethod


class ActionType(str, Enum):
    Email = 'Email'
    Cache = 'Cache'


class AsyncService(Protocol):
    def start(self):
        ...


@dataclasses.dataclass(kw_only=True)
class BaseAction(ABC):

    """Used for scheduling report to e.g. know whether to email to cache, add required fields to abstract classes"""
    def __repr__(self):
        return str(self.__dict__)

    @abstractmethod
    def run(self):
        """Implement logic for executing this action"""
        ...

    @classmethod
    def action_type(cls) -> ActionType:
        """Return what of what type this action is"""
        ...


@dataclasses.dataclass
class BaseScheduleParams(ABC):
    """Baseclass for scheduling parameters"""
    def __format__(self, format_spec):
        return str(self.__dict__)



