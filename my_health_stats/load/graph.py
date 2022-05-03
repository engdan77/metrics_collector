import datetime
from abc import ABC, abstractmethod
from my_health_stats.transform.base import BaseTransform
import plotly.graph_objects as go
import plotly.express as px
from typing import Callable, Annotated, Iterable
from enum import Enum, auto


class GraphFormat(str, Enum):
    html = auto()
    png = auto()


class BaseLoadGraph(ABC):
    graph_formats = {GraphFormat.png: 'to_png',
                     GraphFormat.html: 'to_html'}

    def __init__(self, pipeline: BaseTransform, from_: datetime.date, to_: datetime.date):
        self.pipeline = pipeline
        self.pipeline.validate()
        self.df = self.pipeline.process_pipeline(from_, to_)
        # html = fig.to_html(include_plotlyjs="require", full_html=False)

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


class GarminAppleLoadGraph(BaseLoadGraph):

    @property
    def get_all_graph_methods(self) -> Iterable[Annotated[Callable, "Class methods generating graphs"]]:
        return (self.graph_monthly_run_count_pace,
                self.graph_weekly_distance,
                self.graph_weekly_weight,
                self.graph_weekly_blood_pressure)

    def to_html(self, graph_method: Callable) -> str:
        return graph_method().to_html(include_plotlyjs="require", full_html=False)

    def to_png(self, graph_method: Callable) -> bytes:
        return graph_method().to_image(format="png")

    def graph_monthly_run_count_pace(self):
        df = self.df.copy()
        # Require preparing data
        df = df.resample('M').agg({'avg_speed_running_trip': 'max', 'running_distance_meters': 'count'})
        df.rename(columns={"running_distance_meters": "number_of_runs"}, inplace=True)
        df.astype(float)
        df['number_of_runs'].interpolate(method='linear', limit_direction='backward', axis=0, inplace=True)
        df = df.ffill().bfill()
        # Build graph
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df.index, y=df.number_of_runs, name="Number of runs", yaxis='y'))
        fig.add_trace(
            go.Scatter(x=df.index, y=df.avg_speed_running_trip, name="Max minutes/Km", yaxis="y2"))
        fig.update_layout(xaxis=dict(domain=[0.5, 0.5]),
                          yaxis=dict(
                              title="Number of runs",
                              titlefont=dict(color="#1f77b4"),
                              tickfont=dict(color="#1f77b4")),
                          yaxis2=dict(title="Average speed", overlaying="y", side="right", position=1,
                                      autorange="reversed"))
        fig.update_layout(title_text="Monthly run counts and max pace", width=900)
        fig.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01), legend_title_text=None)
        return fig
        
    def graph_weekly_distance(self):
        df = self.df.resample('W').sum()
        df['walking_running_km_mean'] = df.total_distance_km.mean()
        b = px.bar(df, y=['walking_km', 'running_km'], title="Weekly distance", height=500)
        b.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01), legend_title_text=None)
        l = px.line(df, y="walking_running_km_mean")
        b.add_traces(l.data)
    
    def graph_weekly_blood_pressure(self):
        df = self.df.resample('W').max()
        both = ["bloodpressuresystolic_mmHg", "bloodpressurediastolic_mmHg"]
        for _ in both:
            df[_] = df[_].rolling(3).mean()
        l = px.line(df, y=both, title="Weekly blood pressure", height=500)
        l.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01), legend_title_text=None)
        
    def graph_weekly_weight(self):
        df = self.df.resample('W').max()
        df['bodymass_kg'] = df['bodymass_kg'].rolling(5).mean()
        l = px.line(df, y='bodymass_kg', title="Weekly average weight", height=500)
        l.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01), legend_title_text=None)