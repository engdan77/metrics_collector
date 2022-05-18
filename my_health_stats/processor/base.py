import datetime
from abc import ABC, abstractmethod
from my_health_stats.load.base import BaseLoadGraph
from my_health_stats.transform.base import BaseTransform
from typing import Type


class BasePipeline(ABC):

    @abstractmethod
    def extract_and_transform(self) -> BaseTransform:
        """Code for extracting e.g. using parameters in __init__"""
        ...

    @abstractmethod
    def get_graph_class(self) -> Type[BaseLoadGraph]:
        """Return a simple class"""
        ...

    def execute(self, start_date=datetime.date, end_date=datetime.date) -> BaseLoadGraph:
        """This returns an object to be used to generate graphs in various formats"""
        transform_object: BaseTransform = self.extract_and_transform()
        graph_class = self.get_graph_class()
        graph_loader = graph_class(transform_object, start_date, end_date)
        return graph_loader


