from pathlib import Path
from typing import TypedDict, Union, Annotated
from abc import ABC, abstractmethod
from loguru import logger
from datetime import date
import json
from appdirs import user_data_dir
from deepmerge import always_merger as merge

Number = Union[int, float]


class ActivityDetails(TypedDict):
    value: Union[list[Number], Number]
    unit: str


class DaysActivities(TypedDict):
    date: dict[Annotated[str, 'name of activity'], ActivityDetails]


class BaseService(ABC):

    def get_cache_file(self):
        cache_dir = user_data_dir(__package__)
        Path(cache_dir).mkdir(exist_ok=True)
        return f'{cache_dir}/{self.__class__.__name__}.json'

    @abstractmethod
    def get_data_from_service(self, date_: str) -> DaysActivities:
        """Get all data from that service"""

    def to_json(self, filename: str, data: DaysActivities) -> None:
        f = Path(filename)
        if f.exists():
            current_data = json.loads(f.read_text())
            data = merge(current_data, data)
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

    def from_json(self, filename: str, date_) -> Union[DaysActivities, None]:
        if not Path(filename).exists():
            return None
        with open(filename) as f:
            j = json.load(f)
        return {date_: j[date_]} if date_ in j else None

    def get_data(self, date_: str) -> DaysActivities:
        cache_file = self.get_cache_file()
        if (j := self.from_json(cache_file, date_)) is not None:
            return j
        j = self.get_data_from_service(date_)
        self.to_json(cache_file, j)
        return j
