import collections
import datetime
from abc import ABC, abstractmethod
from enum import auto
from typing import Callable, Iterable, Annotated

from metrics_collector.orchestrator.generic import register_dag_name
from metrics_collector.transform.base import BaseTransform
from fastapi_utils.enums import StrEnum


class GraphFormat(StrEnum):
    """Different formats to be returned and shall be kept standard extension to allow mime-type conversion"""
    html = auto()
    png = auto()


class BaseLoadGraph(ABC):
    graph_formats = {GraphFormat.png: 'to_png',
                     GraphFormat.html: 'to_html'}

    dag_name: str | Iterable = NotImplemented

    def __init__(self, transformer: BaseTransform, from_: datetime.date, to_: datetime.date):
        self.transformer = transformer
        self.transformer.validate()
        self.df = self.transformer.process_pipeline(from_, to_)

    def __init_subclass__(cls, **kwargs):
        if cls.dag_name is NotImplemented:
            raise NotImplemented("the dag_name is required for extract, transform and load subclasses")
        register_dag_name(cls)

    @abstractmethod
    def to_html(self, graph_method: Callable) -> str:
        """Convert result of graph method into html"""

    @abstractmethod
    def to_png(self, graph_method: Callable) -> bytes:
        """Convert result of graph method into png bytes"""

    @abstractmethod
    def get_all_graph_methods(self) -> Iterable[Annotated[Callable, "Class methods generating graphs"]]:
        """Return list with methods generating graphs"""

    def get_all_graphs(self, graph_format: GraphFormat):
        format_method = getattr(self, self.graph_formats[graph_format])
        for graph_method in self.get_all_graph_methods():
            yield format_method(graph_method)
