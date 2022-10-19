from collections import defaultdict
from dataclasses import dataclass, field
from tempfile import NamedTemporaryFile
from typing import Callable, Annotated
from zipfile import ZipFile
from loguru import logger
from apple_health import HealthData
from metrics_collector.extract.base import (
    DaysMetrics,
    BaseExtract,
    BaseExtractParameters,
)
from metrics_collector.storage.uriloader import uri_loader


@dataclass
class AppleHealthExtractParameters(BaseExtractParameters):
    apple_uri_health_data: str
    uri_loader: Callable[[Annotated[str, "uri_string"]], bytes] = field(
        init=False, default=uri_loader, repr=False
    )


class AppleHealthExtract(BaseExtract):

    dag_name = "garmin_and_apple"

    def __init__(self, parameters: AppleHealthExtractParameters):
        self.parameters = parameters
        self.records = None
        self.xml = None
        self.activity_prefix = "HKQuantityTypeIdentifier"
        self.activities = [
            "HKQuantityTypeIdentifierBloodPressureDiastolic",
            "HKQuantityTypeIdentifierBloodPressureSystolic",
            "HKQuantityTypeIdentifierBodyFatPercentage",
            "HKQuantityTypeIdentifierBodyMass",
            "HKQuantityTypeIdentifierBodyMassIndex",
            "HKQuantityTypeIdentifierDistanceWalkingRunning",
            "HKQuantityTypeIdentifierStepCount",
        ]
        self.parsed_complete = False

    def parse_records(self):
        with NamedTemporaryFile() as f:
            logger.debug(f"writing zipped apple xml to {f.name}")
            f.write(
                self.parameters.uri_loader(self.parameters.apple_uri_health_data)
            )  # load data from service
            self.xml = self._extract_xml(f.name)
        logger.debug(f"found xml {int(len(self.xml) / 1000)} KB")
        self.records = self._get_health_data_from_xml(self.xml)
        self.parsed_complete = True

    def _extract_xml(self, input_zip):
        with ZipFile(input_zip) as myzip:
            logger.debug(myzip.namelist())
            with myzip.open("apple_health_export/export.xml") as myfile:
                return myfile.read()

    def _get_health_data_from_xml(self, input_xml: bytes):
        with NamedTemporaryFile() as fp:
            fp.write(input_xml)
            # TODO: fina HealthData class
            health_data = HealthData.read(fp.name)
        return health_data

    def _get_all_types(self, input_records: HealthData, year=2022):
        name_types = set()
        for r in input_records.records:
            if r.start.year == year:
                name_types.add(r.name)
        return name_types

    def _get_one_each_type(self, input_records):
        result = {}
        for r in input_records.records:
            result[r.name] = r
        return result

    def get_data_from_service(self, date_) -> DaysMetrics:
        if not self.parsed_complete:
            self.parse_records()
        input_records = self.records
        result = defaultdict(lambda: defaultdict(dict))
        for r in input_records.records:
            d = r.start.strftime("%Y-%m-%d")
            if r.name in self.activities:
                if r.start == date_:
                    continue
                metric_name = r.name.replace(self.activity_prefix, "").lower()
                result[d][metric_name]["unit"] = r.unit
                values = result[d][metric_name].get("value", [])
                values.append(r.value)
                result[d][metric_name]["value"] = values
        return dict(result)
