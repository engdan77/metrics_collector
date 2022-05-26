import os
from pathlib import Path

from my_health_stats.extract.apple import AppleHealthExtract
from my_health_stats.extract.garmin import GarminExtract
from my_health_stats.load.base import BaseLoadGraph
from my_health_stats.load.graph import GarminAppleLoadGraph
from my_health_stats.orchestrator.generic import Orchestrator
from my_health_stats.transform.transformers import GarminAppleTransform
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
