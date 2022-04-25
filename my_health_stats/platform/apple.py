from collections import defaultdict
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from loguru import logger
from apple_health import HealthData
from my_health_stats.platform.base import DaysActivities, BaseService


class AppleHealthPlatform(BaseService):

    def __init__(self, zip_file: str):
        self.records = None
        self.xml = None
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
        self.parsed_complete = False

    def parse_records(self):
        self.xml = self._extract_xml(self.zip_file)
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

    def get_data_from_service(self, date_) -> DaysActivities:
        if not self.parsed_complete:
            self.parse_records()
        input_records = self.records
        result = defaultdict(lambda: defaultdict(dict))
        for r in input_records.records:
            d = r.start.strftime("%Y-%m-%d")
            if r.name in self.activities:
                if r.start == date_:
                    continue
                activity_name = r.name.replace(self.activity_prefix, "").lower()
                result[d][activity_name]["unit"] = r.unit
                values = result[d][activity_name].get("value", [])
                values.append(r.value)
                result[d][activity_name]["value"] = values
        return dict(result)

