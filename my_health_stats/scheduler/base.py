from typing import Protocol


class AsyncService(Protocol):
    def start(self):
        ...
