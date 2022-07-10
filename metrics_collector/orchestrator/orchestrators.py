import os
from pathlib import Path

from metrics_collector.extract.apple import AppleHealthExtract
from metrics_collector.extract.garmin import GarminExtract
from metrics_collector.load.base import BaseLoadGraph
from metrics_collector.load.graph import GarminAppleLoadGraph
from metrics_collector.orchestrator.generic import Orchestrator
from metrics_collector.transform.transformers import GarminAppleTransform
from typing import Type


# class GarminAppleOrchestrator(Orchestrator):
#     def __init__(
#         self,
#         apple_health_path="../data/export.zip",
#         garmin_user=os.getenv("USERNAME"),
#         garmin_password=os.getenv("PASSWORD"),
#     ):
#         self.garmin_password = garmin_password
#         self.garmin_user = garmin_user
#         self.apple_health_path = apple_health_path
#
#     def extract_and_transform(self) -> GarminAppleTransform:
#         apple_bytes = Path(self.apple_health_path).read_bytes()
#         ah = AppleHealthExtract(apple_bytes)
#         g = GarminExtract(self.garmin_user, self.garmin_password)
#         return GarminAppleTransform(ah, g)
#
#     def get_graph_class(self) -> Type[BaseLoadGraph]:
#         return GarminAppleLoadGraph
