import datetime

import pandas as pd
import numpy as np
from loguru import logger
from collections.abc import Iterable


class TransformError(Exception):
    """Error related to transformation of data"""


class DataframeTransformer:
    def __init__(self, df: pd.DataFrame = None):
        self.df = df
        self.columns_required = ('walking_distance_meters',)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.df!r})'

    def __str__(self):
        return f'{self.__class__.__name__} with size {self.df.shape}'

    def process(self, from_: datetime.date, to_: datetime.date):
        return (
                self.index_as_dt().
                aggregate_combined_dataframes().
                add_missing_columns(self.columns_required).
                filter_period(from_, to_).
                add_col_avg_speed_running_trip().
                add_apple_garmin_distances().
                add_missing_values()
                )

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

    def add_missing_columns(self, columns: str | Iterable, default_value=0):
        columns = [columns] if isinstance(columns, str) else columns
        for c in columns:
            if c not in self.df.columns:
                logger.debug(f'adding missing column {c} to df')
                self.df[c] = default_value
        return self

    def filter_period(self, from_: datetime.date, to_: datetime.date):
        """Filter period between dates"""
        f, t = datetime.date(2020, 1, 1), datetime.date(2023, 1, 1)
        self.df = self.df.query('date > @f & date < @t')
        return self

    def add_col_avg_speed_running_trip(
        self, trip_distance_meters=6000, margin_percentage=8
    ):
        """Adding extra column for running speed based on e.g. 6Km runs"""
        diff_meters = (margin_percentage / 100) * trip_distance_meters
        try:
            self.df["avg_speed_running_trip"] = self.df.apply(
                lambda row: (row.running_duration_seconds / 60)
                / (row.running_distance_meters / 1000)
                if row.running_distance_meters
                and row.running_distance_meters - diff_meters
                <= row.running_distance_meters
                <= row.running_distance_meters + diff_meters
                else np.NaN,
                axis=1,
            )
        except AttributeError:
            raise TransformError
        return self

    def add_apple_garmin_distances(self):
        """Adding columns combining distances from both Garmin and Apple"""
        self.df["running_distance_garmin"] = self.df.running_distance_meters.apply(
            lambda x: x / 1000 if not pd.isna(x) else 0
        )
        self.df["walking_distance_garmin"] = self.df.walking_distance_meters.apply(
            lambda x: x / 1000 if not pd.isna(x) else 0
        )
        self.df[
            "walking_running_distance_applehealth"
        ] = self.df.distancewalkingrunning_km_sum.apply(
            lambda x: x if not pd.isna(x) else 0
        )

        self.df["walking_km"] = self.df.apply(
            lambda row: row.walking_running_distance_applehealth
            - row.running_distance_garmin
            if row.walking_running_distance_applehealth
            >= row.walking_distance_garmin + row.running_distance_garmin
            else row.walking_running_distance_applehealth,
            axis=1,
        )
        self.df["running_km"] = self.df.apply(
            lambda row: row.running_distance_garmin, axis=1
        )
        self.df['total_distance_km'] = self.df.apply(lambda row: row.running_km + row.walking_km, axis=1)
        return self

    def add_missing_values(self, nan_value=0.0, cols=('bloodpressurediastolic_mmHg', 'bloodpressuresystolic_mmHg', 'bodymass_kg')):
        """Filling missing values use linear backward"""
        for _ in cols:
            self.df[_].replace(nan_value, np.NaN, inplace=True)
            self.df[_].interpolate(method='linear', limit_direction='backward', inplace=True)
        return self

    def resample_sum(self, resolution='W'):
        """Resampling resolution"""
        self.df = self.df.resample(resolution).sum()
