import dataclasses
from typing import Protocol
from abc import ABC


class AsyncService(Protocol):
    def start(self):
        ...


@dataclasses.dataclass
class BaseAction(ABC):
    def __repr__(self):
        return self.__dict__

    def shorten(self, input_data, letters=8):
        return f'{input_data[:letters]}...' if len(input_data) >= letters else input_data
