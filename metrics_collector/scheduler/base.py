from typing import Protocol
from abc import ABC


class AsyncService(Protocol):
    def start(self):
        ...


class BaseAction(ABC):
    def __repr__(self):
        return self.__dict__
