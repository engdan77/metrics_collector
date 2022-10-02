import datetime

import numpy as np
import pandas as pd
import pandera as pa

from metrics_collector.extract.apple import AppleHealthExtract
from metrics_collector.extract.garmin import GarminExtract
from metrics_collector.transform.base import BaseTransform, TransformError


class GarminAppleTransform(BaseTransform):
    input_schema = pa.DataFrameSchema(
        {
            "distancewalkingrunning_km_sum": pa.Column(float, nullable=True),
            "bodymass_kg": pa.Column(float, nullable=True),
            "bloodpressuresystolic_mmHg": pa.Column(float, nullable=True),
            "bloodpressurediastolic_mmHg": pa.Column(float, nullable=True),
            "distancewalkingrunning_km": pa.Column(float, nullable=True),
            "running_distance_meters": pa.Column(float, nullable=True),
            "running_duration_seconds": pa.Column(float, nullable=True),
            "walking_distance_meters": pa.Column(float, nullable=True),
        },
    )
    dag_name = 'garmin_and_apple'

    def process_pipeline(self, from_: datetime.date, to_: datetime.date) -> pd.DataFrame:
        (
            self.index_as_dt()
            .aggregate_combined_dataframes()
            .filter_period(from_, to_)
            .add_col_avg_speed_running_trip()
            .add_apple_garmin_distances()
            .add_missing_values()
        )
        return self.df

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
        except (AttributeError, ValueError) as e:
            raise TransformError(e)
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
        self.df["total_distance_km"] = self.df.apply(
            lambda row: row.running_km + row.walking_km, axis=1
        )
        return self
