import pytest
from metrics_collector.extract.base import BaseExtractParameters
from dataclasses import dataclass

@pytest.fixture()
def sample_extract_parameters_class():
    @dataclass
    class SampleExtractParameters(BaseExtractParameters):
        uri_for_sample_service: str
    return SampleExtractParameters


def sample_extract_days_metrics():
    return {
              "2022-01-01": {
                "running": {
                  "value": 300,
                  "unit": "meter"
                },
                "walking": {
                  "value": 150,
                  "unit": "meter"
                }
              }
            }


