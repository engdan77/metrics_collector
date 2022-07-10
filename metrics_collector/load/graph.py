import plotly.graph_objs

import plotly.graph_objects as go
import plotly.express as px
from typing import Callable, Annotated, Iterable

from metrics_collector.load.base import BaseLoadGraph


class GarminAppleLoadGraph(BaseLoadGraph):
    dag_name = 'garmin_and_apple'

    def get_all_graph_methods(self) -> Iterable[Annotated[Callable, "Class methods generating graphs"]]:
        return (self.graph_monthly_run_count_pace,
                self.graph_weekly_distance,
                self.graph_weekly_weight,
                self.graph_weekly_blood_pressure)

    def to_html(self, graph_method: Callable) -> str:
        return graph_method().to_html(include_plotlyjs="require", full_html=False)

    def to_png(self, graph_method: Callable) -> bytes:
        return graph_method().to_image(format="png")

    def graph_monthly_run_count_pace(self) -> plotly.graph_objects.Figure:
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
        
    def graph_weekly_distance(self) -> plotly.graph_objs.Figure:
        df = self.df.resample('W').sum()
        df['walking_running_km_mean'] = df.total_distance_km.mean()
        fig = px.bar(df, y=['walking_km', 'running_km'], title="Weekly distance", height=500)
        fig.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01), legend_title_text=None)
        line = px.line(df, y="walking_running_km_mean")
        fig.add_traces(line.data)
        return fig
    
    def graph_weekly_blood_pressure(self) -> plotly.graph_objs.Figure:
        df = self.df.resample('W').max()
        both = ["bloodpressuresystolic_mmHg", "bloodpressurediastolic_mmHg"]
        for _ in both:
            df[_] = df[_].rolling(3).mean()
        fig = px.line(df, y=both, title="Weekly blood pressure", height=500)
        fig.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01), legend_title_text=None)
        return fig
        
    def graph_weekly_weight(self) -> plotly.graph_objs.Figure:
        df = self.df.resample('W').max()
        df['bodymass_kg'] = df['bodymass_kg'].rolling(5).mean()
        fig = px.line(df, y='bodymass_kg', title="Weekly average weight", height=500)
        fig.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01), legend_title_text=None)
        return fig
