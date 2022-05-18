import datetime
from abc import ABC, abstractmethod
from typing import Callable, Iterable, Annotated

from my_health_stats.load.graph import GraphFormat
from my_health_stats.transform.base import BaseTransform


class BaseLoadGraph(ABC):
    graph_formats = {GraphFormat.png: 'to_png',
                     GraphFormat.html: 'to_html'}

    def __init__(self, pipeline: BaseTransform, from_: datetime.date, to_: datetime.date):
        self.pipeline = pipeline
        self.pipeline.validate()
        self.df = self.pipeline.process_pipeline(from_, to_)

    @abstractmethod
    def to_html(self, graph_method: Callable) -> str:
        """Convert result of graph method into html"""

    @abstractmethod
    def to_png(self, graph_method: Callable) -> bytes:
        """Convert result of graph method into png bytes"""

    @property
    @abstractmethod
    def get_all_graph_methods(self) -> Iterable[Annotated[Callable, "Class methods generating graphs"]]:
        """Return list with methods generating graphs"""

    def get_all_graphs(self, graph_format: GraphFormat):
        format_method = getattr(self, self.graph_formats[graph_format])
        for graph_method in self.get_all_graph_methods:
            yield format_method(graph_method)
