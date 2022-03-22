import json
from collections import defaultdict
from tempfile import NamedTemporaryFile
from zipfile import ZipFile

from apple_health import HealthData
from loguru import logger


class AppleHealth:
    def __init__(self, zip_file: str):
        self.zip_file = zip_file
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

        self.xml = self.extract_xml(self.zip_file)
        logger.debug(f"found xml {int(len(self.xml) / 1000)} KB")
        self.records = self.get_health_data_from_xml(self.xml)
        self.result = self.get_daily_records(self.records)

    def extract_xml(self, input_zip):
        with ZipFile(input_zip) as myzip:
            logger.debug(myzip.namelist())
            with myzip.open("apple_health_export/export.xml") as myfile:
                return myfile.read()

    def get_health_data_from_xml(self, input_xml: bytes):
        with NamedTemporaryFile() as fp:
            fp.write(input_xml)
            health_data = HealthData.read(fp.name)
        return health_data

    def get_all_types(self, input_records: HealthData, year=2022):
        name_types = set()
        for r in input_records.records:
            if r.start.year == year:
                name_types.add(r.name)
        return name_types

    def get_one_each_type(self, input_records):
        result = {}
        for r in input_records.records:
            result[r.name] = r
        return result

    def get_daily_records(self, input_records: HealthData):
        result = defaultdict(lambda: defaultdict(dict))
        for r in input_records.records:
            if r.name in self.activities:
                dt = r.start.strftime("%Y-%m-%d")
                activity_name = r.name.replace(self.activity_prefix, "")
                result[dt][activity_name]["unit"] = r.unit
                values = result[dt][activity_name].get("value", [])
                values.append(r.value)
                result[dt][activity_name]["value"] = values
        return result

    def to_json(self, filename: str):
        with open(filename, "w") as f:
            logger.debug(f'writing to {filename}')
            json.dump(self.result, f, indent=2)
