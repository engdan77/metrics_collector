import datetime
import os
import shelve
from dataclasses import dataclass, fields
from inspect import get_annotations

import pandas as pd
from pathlib import Path
from typing import TypedDict, Union, Annotated, Optional, Iterable, Type
from abc import ABC, abstractmethod
from loguru import logger
import json
from appdirs import user_data_dir
from deepmerge import always_merger
from statistics import mean
from metrics_collector.orchestrator.generic import register_dag_name
from metrics_collector.utils import get_data_dir

Number = Union[int, float]

parameter_dict = dict[Annotated[str, "Field name"], Type]


class MetricDetails(TypedDict):
    value: Union[list[Number], Number]
    unit: str


DaysMetrics = dict[
    Annotated[str, "date"], dict[Annotated[str, "name of activity"], MetricDetails]
]


@dataclass
class BaseExtractParameters:
    """Base dataclass for extraction parameters.
    Assure that repr of individual attributes are serializable or set repr=False as field"""

    ...


class BaseExtract(ABC):

    list_value_processors = {"max": max, "min": min, "mean": mean, "sum": sum}
    data_dir = get_data_dir()
    params_file = f"{data_dir}/params"
    dag_name: str | Iterable = NotImplemented
    parameters = {}

    @abstractmethod
    def __init__(self, parameters: BaseExtractParameters):
        ...

    def __repr__(self):
        return f"{self.__class__.__name__}({self._get_arguments()}"

    def __init_subclass__(cls, **kwargs):
        if cls.dag_name is NotImplemented:
            raise NotImplemented(
                "the dag_name is required for extract, transform and load subclasses"
            )
        register_dag_name(cls)

    def _get_arguments(self):
        try:
            return self.parameters
        except NameError:
            return self.get_parameters()

    @classmethod
    def get_parameters(cls) -> parameter_dict:
        params = {}
        for field in fields(get_annotations(cls.__init__).get("parameters")):
            if field.init:
                params[field.name] = field.type
        return params

    @classmethod
    def get_extract_parameter_class(cls):
        return get_annotations(cls.__init__).get("parameters", None)

    def get_cache_file(self):
        """Get cache dir as environment variable DATA_DIR or app dir"""
        Path(self.data_dir).mkdir(exist_ok=True)
        return f"{self.data_dir}/{self.__class__.__name__}.json"

    @classmethod
    def store_params(cls, params: dict):
        Path(cls.data_dir).mkdir(exist_ok=True)
        with shelve.open(cls.params_file) as f:
            current = f.get(cls.dag_name, {})
            current.update(params)
            f[cls.dag_name] = current

    @classmethod
    def get_stored_params(cls):
        """Get previous parameters used for future usage"""
        p = {}
        try:
            p = shelve.open(cls.params_file)
        finally:
            return p.get(cls.dag_name, {})

    @abstractmethod
    def get_data_from_service(self, date_: str) -> DaysMetrics:
        """Get all data from that extract"""

    @staticmethod
    def pop_existing_days(
        existing_data: DaysMetrics, pop_data: DaysMetrics
    ) -> DaysMetrics:
        """Remove days from pop_data if that day already exists in existing_data"""
        output_pop_data = pop_data.copy()
        for day in pop_data.keys():
            if day in existing_data:
                output_pop_data.pop(day)
        return output_pop_data

    def to_json(self, filename: str, data: DaysMetrics) -> None:
        f = Path(filename)
        if f.exists():
            try:
                current_data = json.loads(f.read_text())
            except json.decoder.JSONDecodeError:
                logger.warning(f"{f.as_posix()} is corrupt, removing")
                f.unlink(missing_ok=True)
            else:
                self.pop_existing_days(current_data, data)
                data = always_merger.merge(current_data, data)
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def from_json(filename: Optional[str], date_=None) -> Union[DaysMetrics, None]:
        if not Path(filename).exists():
            return None
        with open(filename) as f:
            try:
                j = json.load(f)
            except json.decoder.JSONDecodeError as e:
                now = datetime.datetime.now().replace(microsecond=0).isoformat("_")
                backup_file = f"{filename}_{now}"
                logger.warning(
                    f"Unable to read from cached JSON, most likely corrupt with following error {e.msg} saving backup to {backup_file}"
                )
                Path(backup_file).write_text(f.read())
                return None
        if not date_:
            return j
        else:
            return {date_: j[date_]} if date_ in j else None

    def get_data(self, date_: str | datetime.date) -> DaysMetrics:
        """This is the main method supposed to be used"""
        if date_ is datetime.date:
            date_ = date_.strftime("%Y-%m-%d")
        cache_file = self.get_cache_file()
        self.__class__.store_params(
            self.parameters.__dict__
        )  # save last working params
        logger.debug(f"attempt get cache data from {cache_file}")
        if (j := self.from_json(cache_file, date_)) is not None:
            logger.debug(f"found cached {len(j)} bytes")
            return j
        logger.debug(f"getting data for {date_}")
        j = self.get_data_from_service(date_)
        self.to_json(cache_file, j)
        return j

    def to_df(self, input_data: Optional[dict] = None) -> pd.DataFrame:
        """
        Creates dataframe where list of values get processes.
        Or a single value in list get as value.
        Field name is concatenated by activity and unit.
        """
        if not input_data:
            input_data = self.from_json(self.get_cache_file())
        df = pd.DataFrame()
        if not input_data:
            return df
        for date_, activities_data in input_data.items():
            row_data = {}
            for metric_name, activity_data in activities_data.items():
                unit = activity_data["unit"]
                value = activity_data["value"]
                field_name = f"{metric_name}_{unit}"
                if isinstance(value, list):
                    if value and len(value) > 1:
                        for processor_name, func in self.list_value_processors.items():
                            try:
                                processed_data = func(value)
                            except TypeError:
                                continue
                            row_data.update(
                                {
                                    "date": date_,
                                    f"{field_name}_{processor_name}": processed_data,
                                }
                            )
                    else:
                        row_data.update({"date": date_, field_name: value[0]})
                else:
                    row_data.update({"date": date_, field_name: value})
            df = pd.concat([df, pd.DataFrame.from_records([row_data])])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.set_index(df["date"])
        return df
