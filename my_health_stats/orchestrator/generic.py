import datetime
from abc import ABC, abstractmethod
from collections import defaultdict

# from my_health_stats.load.base import BaseLoadGraph
# from my_health_stats.transform.base import BaseTransform
from typing import Type, Annotated, Iterable


def register_dag_name(cls):
    c = cls.dag_name
    names = (c,) if isinstance(c, str) else c
    for _ in names:
        Orchestrator.registered_etl_entities[_].append(cls)


class Orchestrator:

    registered_etl_entities: defaultdict[
        Annotated[str, "dag name"], list[Annotated[Type, "classes"]]
    ] = defaultdict(list)

    def extract_and_transform(self):
        """Code for extracting e.g. using parameters in __init__"""
        ...

    def get_graph_class(self):
        """Return a simple class"""
        ...

    def execute(self, start_date=datetime.date, end_date=datetime.date):
        """This returns an object to be used to generate graphs in various formats"""
        # transform_object: BaseTransform = self.extract_and_transform()
        # graph_class = self.get_graph_class()
        # graph_loader = graph_class(transform_object, start_date, end_date)
        return graph_loader
