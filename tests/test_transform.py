from datetime import datetime, date

import pandas as pd
import pytest

from metrics_collector.transform import BaseTransform
import pandera as pa
from .test_extract import extract_obj


class FooTransform(BaseTransform):
    input_schema = pa.DataFrameSchema(
        {
            "running": pa.Column(float, nullable=True),
            "walking": pa.Column(float, nullable=True),
        },
    )
    dag_name = "foo"

    def process_pipeline(
        self, from_: datetime.date, to_: datetime.date
    ) -> pd.DataFrame:
        (self.index_as_dt().aggregate_combined_dataframes().filter_period(from_, to_))
        return self.df


@pytest.fixture
def transform_obj(extract_obj):
    # extract_objs = test_extract.extract_obj()
    return FooTransform(extract_obj)


def test_transform_obj(transform_obj):
    assert transform_obj.dag_name == "foo", "Wrong dag name"
    assert transform_obj.df.shape == (2, 3), "Shape of data wrong"


def test_process_pipeline(transform_obj):
    from_, to_ = date(2022, 1, 2), date(2022, 1, 5)
    transform_obj.process_pipeline(from_, to_)
    df = transform_obj.df
    assert df.shape == (1, 2), "Unable to filter out specific date"
    assert df.index.name == "date", "Index columns should be date"
    assert set(df.columns) == {"running_meter", "walking_meter"}
