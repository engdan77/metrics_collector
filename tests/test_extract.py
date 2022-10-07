import json
import tempfile
from pathlib import Path

import pytest

from metrics_collector.extract.base import (
    BaseExtractParameters,
    BaseExtract,
    DaysMetrics,
)
from dataclasses import dataclass

mock_days_metrics = {
    "2022-01-01": {
        "running": {"value": 300, "unit": "meter"},
        "walking": {"value": 150, "unit": "meter"},
    },
    "2022-01-03": {
        "running": {"value": 120, "unit": "meter"},
        "walking": {"value": 110, "unit": "meter"},
    },
}


@dataclass
class SampleExtractParameters(BaseExtractParameters):
    uri_for_sample_service: str


class SampleExtract(BaseExtract):
    dag_name = "foo"

    def __init__(self, parameters: BaseExtractParameters):
        self.parameters = parameters

    def get_data_from_service(self, date_: str) -> DaysMetrics:
        mock_data: dict = mock_days_metrics
        return mock_data[date_]


@pytest.fixture
def extract_obj():
    params = SampleExtractParameters(uri_for_sample_service="foo://my_service")
    extract_obj = SampleExtract(params)
    return extract_obj


def test_instantiate_extract_class(extract_obj):
    assert isinstance(extract_obj, BaseExtract)


def test_extract_obj_has_properties(extract_obj):
    assert extract_obj.dag_name == "foo"
    assert isinstance(extract_obj.parameters, BaseExtractParameters)


def test_extract_obj_has_cache_dir(extract_obj):
    f = extract_obj.get_cache_file()
    assert Path(f).parent.exists() is True


def test_extract_obj_returns_data(extract_obj):
    data = extract_obj.get_data_from_service("2022-01-01")
    assert data == {'running': {'value': 300, 'unit': 'meter'}, 'walking': {'value': 150, 'unit': 'meter'}}


def test_extract_obj_to_json(extract_obj):
    with tempfile.NamedTemporaryFile() as f:
        extract_obj.to_json(f.name, mock_days_metrics)
        data_in_file = json.loads(open(f.name).read())
    assert data_in_file == mock_days_metrics


def test_extract_obj_to_df(extract_obj):
    df = extract_obj.to_df()