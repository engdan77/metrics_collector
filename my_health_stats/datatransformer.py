import pandas as pd
import numpy as np


class DataframeTransformer:
    def __init__(self, df: pd.DataFrame = None):
        self.df = None

    def index_as_dt(self):
        """Assure index being parsed as datetime and orders"""
        self.df.index = pd.to_datetime(self.df.index)
        self.df.sort_index(inplace=True)
        return self

    def aggregate_combined_dataframes(self, dt_col='date', cols=('date', 'bodymass_kg_mean', 'stepcount_count_sum', 'distancewalkingrunning_km_sum', 'bodymass_kg', 'bloodpressuresystolic_mmHg', 'bloodpressurediastolic_mmHg', 'walking_distance_meters', 'running_distance_meters', 'avg_speed_running_trip')):
        """Ensure consistent columns after combining dataframes"""
        strategy = {_: 'first' for _ in cols}
        self.df = self.df.groupby(self.df.index).agg(strategy)
        self.df.sort_index()
        self.df.drop(dt_col, axis=1, inplace=True)

    def add_col_avg_speed_running_trip(
        self, trip_distance_meters=6000, margin_percentage=8
    ):
        """Adding extra column for running speed based on e.g. 6Km runs"""
        diff_meters = (margin_percentage / 100) * trip_distance_meters
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
        return self
