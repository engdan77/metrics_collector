from __future__ import annotations  # required to avoid circular imports for typing purposes
import datetime
from collections import defaultdict
from typing import Type, Annotated, Iterable, Callable
import my_health_stats
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from my_health_stats.extract.base import parameter_dict, BaseExtract  # only when typing
    from my_health_stats.transform.base import BaseTransform
    from my_health_stats.load.base import BaseLoadGraph

class ClassType(str, Enum):
    extract = auto()
    transform = auto()
    load = auto()


def register_dag_name(cls):
    c = cls.dag_name
    names = (c,) if isinstance(c, str) else c
    for _ in names:
        Orchestrator.registered_etl_entities[_].append(cls)


class Orchestrator:

    registered_etl_entities: defaultdict[
        Annotated[str, "dag name"], list[Annotated[Type, "classes"]]
    ] = defaultdict(list)

    def __init__(self):
        self.type = {ClassType.extract: my_health_stats.extract.base.BaseExtract,
                     ClassType.transform: my_health_stats.transform.base.BaseTransform,
                     ClassType.load: my_health_stats.load.base.BaseLoadGraph}

    def get_extract_parameters(self) -> dict[BaseExtract, parameter_dict]:
        result = defaultdict(dict)
        for dag_name in self.registered_etl_entities.keys():
            extract_classes = self.get_registered_classes(dag_name, ClassType.extract)
            for cls in extract_classes:
                result[dag_name][cls] = cls.get_parameters()
        return result

    def get_graph_methods(self, dag_name: str) -> Iterable[Annotated[Callable, "Class methods generating graphs"]]:
        extract_classes = self.get_registered_classes(dag_name, ClassType.load)
        for cls in extract_classes:
            return cls.get_all_graph_methods()

    def get_registered_classes(self, dag_name, class_type: ClassType, only_first=False) -> list[BaseExtract | BaseTransform | BaseLoadGraph] | BaseExtract | BaseTransform | BaseLoadGraph:
        classes = [cls for cls in self.registered_etl_entities.get(dag_name, []) if cls.__base__ is self.type.get(class_type, None)]
        return next(iter(classes)) if only_first else classes

    def run_dag(self, dag_name):
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
