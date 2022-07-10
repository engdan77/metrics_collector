from __future__ import annotations  # required to avoid circular imports for typing purposes
import datetime
from collections import defaultdict
from typing import Type, Annotated, Iterable, Callable, Union
import metrics_collector
from enum import Enum, auto
from typing import TYPE_CHECKING
from loguru import logger

if TYPE_CHECKING:
    from metrics_collector.extract.base import parameter_dict, BaseExtract, BaseExtractParameters  # only when typing
    from metrics_collector.transform.base import BaseTransform
    from metrics_collector.load.base import BaseLoadGraph


class ClassType(str, Enum):
    extract = auto()
    transform = auto()
    load = auto()


Params = dict[Annotated[str, "param name"], Annotated[str, "value"]]


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
        self.type = {ClassType.extract: metrics_collector.extract.base.BaseExtract,
                     ClassType.transform: metrics_collector.transform.base.BaseTransform,
                     ClassType.load: metrics_collector.load.base.BaseLoadGraph}

    def get_dag_names(self) -> list:
        return list(self.registered_etl_entities.keys())

    def get_extract_parameters(self) -> dict[Annotated[str, 'dag_name'], dict[str, parameter_dict]]:
        result = defaultdict(dict)
        for dag_name in self.registered_etl_entities.keys():
            extract_classes = self.get_registered_classes(dag_name, ClassType.extract)
            for cls in extract_classes:
                result[dag_name][cls] = cls.get_parameters()
        return result

    def get_graph_names(self, dag_name: str) -> list[str]:
        extract_classes = self.get_registered_classes(dag_name, ClassType.load)
        methods = []
        for cls in extract_classes:
            for m in cls.get_all_graph_methods(cls):
                methods.append(m.__name__)
        return methods

    def get_graph_methods(self, dag_name: str) -> Iterable[Annotated[Callable, "Class methods generating graphs"]]:
        extract_classes = self.get_registered_classes(dag_name, ClassType.load)
        methods = []
        for cls in extract_classes:
            methods.append(cls.get_all_graph_methods())
        return methods

    def get_registered_classes(self, dag_name, class_type: ClassType, only_first=False) -> list[Type[BaseExtract] | Type[BaseTransform] | Type[BaseLoadGraph]] | Type[BaseExtract] | Type[BaseTransform] | Type[BaseLoadGraph]:
        classes = [cls for cls in self.registered_etl_entities.get(dag_name, []) if cls.__base__ is self.type.get(class_type, None)]
        return next(iter(classes)) if only_first else classes

    @staticmethod
    def dict_to_extract_params_object(params: Params, extract_class: BaseExtract) -> BaseExtractParameters:
        cls = extract_class.get_extract_parameter_class()
        declare_params = [k for k, v in cls.__dataclass_fields__.items() if v.init]
        # only pass params that are valid for the class
        obj = cls(**{key: params[key] for key in declare_params})
        return obj

    def get_extract_args_def(self, dag_name):
        """Used to e.g. pass to UI to request params from use"""
        args = self.get_extract_parameters()
        extract_classes = self.get_registered_classes(dag_name, ClassType.extract)
        for extract_class in extract_classes:
            yield args[dag_name][extract_class]

    def get_extract_objects(self, dag_name, extract_params: dict):
        """Main entrypoint for getting extract objects used to get transformer object"""
        # create extract objects
        args = self.get_extract_parameters()
        extract_classes = self.get_registered_classes(dag_name, ClassType.extract)
        extract_objects = []
        for extract_class in extract_classes:
            extract_args = args[dag_name][extract_class]
            logger.debug(f"get arguments for {extract_class=} which is {extract_args}")
            p = self.dict_to_extract_params_object(extract_params, extract_class)
            extract_object = extract_class(p)  # add args
            extract_objects.append(extract_object)
        return extract_objects

    def get_transform_object(self, dag_name: str, extract_objects: list) -> BaseTransform:
        """Main entrypoint for getting transform object used to load graph from"""
        # create transform object
        transformer_class = self.get_registered_classes(dag_name, ClassType.transform, only_first=True)
        transformer_object = transformer_class(*extract_objects)
        return transformer_object


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
