import datetime

import pandas as pd
import pandera as pa
import numpy as np
from loguru import logger
from collections.abc import Iterable
from abc import ABC, abstractmethod
from my_health_stats.extract.base import BaseExtract

from my_health_stats.orchestrator.generic import register_dag_name


class TransformError(Exception):
    """Error related to transformation of data"""


class BaseTransform(ABC):
    input_schema: pa.DataFrameSchema = NotImplemented
    dag_name: str | Iterable = NotImplemented
    df: pd.DataFrame = None

    def __init__(self, *extract_classes: list[BaseExtract]):
        """Extract classes as arguments and merges"""
        self.df = pd.concat([getattr(_, 'to_df')() for _ in extract_classes])

    @classmethod
    def __init_subclass__(cls, **kwargs):
        for _ in (cls.input_schema, cls.df, cls.dag_name):
            if _ is NotImplemented:
                raise NotImplementedError(
                    f"Assure class variables being declared in subclass"
                )
        register_dag_name(cls)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.df!r})"

    def __str__(self):
        return f"{self.__class__.__name__} with size {self.df.shape}"

    @abstractmethod
    def process_pipeline(self, from_: datetime.date, to_: datetime.date) -> pd.DataFrame:
        """Shall process and return dataframe with data"""

    def validate(self):
        self.add_missing_columns(list(self.input_schema.columns.keys()))
        self.input_schema.validate(self.df)

    def index_as_dt(self):
        """Assure index being parsed as datetime and orders"""
        self.df.index = pd.to_datetime(self.df.index)
        self.df.sort_index(inplace=True)
        return self

    def aggregate_combined_dataframes(self, drop_col="date"):
        """Ensure consistent columns after combining dataframes"""
        strategy = {_: "first" for _ in self.df.columns}
        self.df = self.df.groupby(self.df.index).agg(strategy)
        self.df.sort_index()
        if drop_col:
            self.df.drop(drop_col, axis=1, inplace=True)
        return self

    def add_missing_columns(self, columns: str | Iterable, default_value=0.0):
        columns = [columns] if isinstance(columns, str) else columns
        for c in columns:
            if c not in self.df.columns:
                logger.debug(f"adding missing column {c} to df")
                self.df[c] = default_value
        return self

    def filter_period(self, from_: datetime.date, to_: datetime.date, filter_by='index'):
        """Filter period between dates"""
        f, t = from_, to_
        self.df = self.df.query(f"{filter_by} > @f & {filter_by} < @t")
        return self

    def add_missing_values(
        self,
        nan_value=0.0,
        cols=(
            "bloodpressurediastolic_mmHg",
            "bloodpressuresystolic_mmHg",
            "bodymass_kg",
        ),
    ):
        """Filling missing values use linear backward"""
        for _ in cols:
            self.df[_].replace(nan_value, np.NaN, inplace=True)
            self.df[_].interpolate(
                method="linear", limit_direction="backward", inplace=True
            )
        return self

    def resample_sum(self, resolution="W"):
        """Resampling resolution"""
        self.df = self.df.resample(resolution).sum()
