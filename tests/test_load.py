import datetime
from typing import Iterable, Annotated, Callable

import plotly
import plotly.graph_objects as go
import pytest
from metrics_collector.load.base import GraphFormat

from metrics_collector.load.base import BaseLoadGraph
from .test_extract import extract_obj
from .test_transform import transform_obj


class FooLoadGraph(BaseLoadGraph):
    dag_name = "foo"

    def get_all_graph_methods(
        self,
    ) -> Iterable[Annotated[Callable, "Class methods generating graphs"]]:
        return (self.graph_foo,)

    def to_html(self, graph_method: Callable) -> str:
        return graph_method().to_html(include_plotlyjs="require", full_html=False)

    def to_png(self, graph_method: Callable) -> bytes:
        return graph_method().to_image(format="png")

    def graph_foo(self) -> plotly.graph_objects.Figure:
        df = self.df
        fig = go.Figure()
        fig.add_trace(
            go.Bar(x=df.index, y=df.running, name="Running distance", yaxis="y")
        )
        return fig


@pytest.fixture
def load_graph_obj(transform_obj):
    obj = FooLoadGraph(
        transform_obj, datetime.date(2022, 1, 1), datetime.date(2022, 1, 10)
    )
    return obj


def test_graph_obj(load_graph_obj):
    assert load_graph_obj.dag_name == "foo"
    assert load_graph_obj.df.shape == (1, 4), "Wrong size of dataframe"


@pytest.mark.skip(reason="until research why fail in ubuntu")
def test_get_graphs_html(load_graph_obj):
    html = next(load_graph_obj.get_all_graphs(GraphFormat.html))
    assert 'class="plotly-graph-div"' in html, "No valid HTML graph"


@pytest.mark.skip(reason="until research why fail in ubuntu")
def test_get_graphs_png(load_graph_obj):
    png_bytes = next(load_graph_obj.get_all_graphs(GraphFormat.png))
    assert isinstance(png_bytes, bytes), "Expected PNG bytes"
    assert len(png_bytes) > 10_000, "Expected byte larger than 10KB"
