# Metrics Collector

## Features

- Abstracts complexity from an existing time-based series analysis project (e.g. using Jyputer notebook) into a service that provides ... 
  - ... a User friendly web interface for input data and as result plotting charts
  - ... a RESTful-API for easier integrating your work to external services, such as a dashboard hosted anywhere
  - ... a Scheduler to allow easily setup recurring events such as sending charts to specific email
  - ... a common JSON-based cache storage to allow a local copy of all input data exchanged allowed to be used in other future contexts
- Extends easily by using class inheritance

## Motivation and background

Now I've recently become more and more fascinated by good software design by following design principles such as SOLID. And reading and learn the different design patterns for solving problems have got myself challenge myself thinking harder what might be found applicable for building a easily extensible and maintainable Python software project .. so what project might that be ..?

Thankfully (but not unusual) I did come across a daily problem of mine but I imagine being something others might also have good reasons to solve, and at the time of writing this I had not yet come across any other project to address those.

So this is when the "**Metric Collector**" was first invented, which is an application to be highly customisable for collecting metric from different data points, deal with the processing and dynamically expose certain basic user interfaces such as RESTful API, user-friendly web interface and also a scheduler to allow e.g. sending these time-based graphs.

And the code been written in such way, and thankfully to Pythons dynamic nature automatically make this into a more seamless experience thanks to the only work required to extend additional services is to inherit from a very few different base-classes.

This also gave me a chance to familiarize myself with GitHub actions allow automatic the CI/CD pipeline into the PyPI.

....

## Installation

....

## Usage

....

## Tutorial - implement your own service

For the most cases one may prefer to work within a Juypter to test get data, clean it and make chart of these.
So what this project aim for is to supply a few abstract base classes to you can conform you existing code without need to think too much about the code later on that allow you to easily access data.

### Extract step

This first step is a prerequisite to have any data to analyse.

####  Parameters - inherit from BaseExtractParameters

This is a dataclass that should be a container for all "fields" required to extract such data.
Thanks for inheritance the abstracted services will allow interfaces to ask those from user.

```python
from metrics_collector.extract.base import BaseExtractParameters
from dataclasses import dataclass

@dataclass
# An example how it may look for a Foo service
class FooExtractParameters(BaseExtractParameters):
    uri_for_foo_service: str
```

#### Extracting data - inherit from BaseExtract

It is required for your service to inherit from **BaseExtract** that has as main purpose to return dictionary that follows **DaysMetrics** based on a date (iso-format) as parameter.
An example of such **DaysMetrics** is

```json
{
  "2022-01-01": {
    "running": {
      "value": 300,
      "unit": "meter"
    },
    "walking": {
      "value": 150,
      "unit": "meter"
    }
  }
}
```

With we can create a concrete class that should have a dag_name that is a single identity across its belong Transform and Load/Graph classes we'll later get to.
The base class will also let you know of the required **get_data_from_service(date_)** method that you'd place the logic that'd use your ExtractParameters.
Machinery will also assure that data being cached for future use.

```python
from metrics_collector.extract.base import BaseExtract, DaysMetrics
# from ... import FooExtractParameters

class FooExtract(BaseExtract):

    dag_name = 'foo'

    def __init__(self, parameters: FooExtractParameters):
        self.parameters = parameters

    def get_data_from_service(self, date_: str) -> DaysMetrics:
        ...
        day_data: dict = gather_data(date_)
        return day_data

```

### Transform step

This step is the next in your pipeline to align the data to your needs.

#### Transforming data - inherit from BaseTransform

So we have here a different base class to inherit from and a required **process_pipeline(from_, to_)** method that going to be invoked by the internal machinery and should return the `self.df`.
This class will also use "pandera" (pa) for validating the schema of dataframe we're going to turn the data from Extract into.
This class also required the **dag_name** same as for above Extract, and that is the only thing you need to worry about.

Under the hood the orchestration layer will inject the corresponding Extract classes (one or many) into this and with that assure this instantiated object will have a self.df that will be validated before the concrete process_pipeline will be inoked.
Another purpose is also to allow extending the BaseTransform with most common set of functions one typically need and a few available are

- aggregate_combined_dataframes()
- filter_period()
- add_missing_values()

For approach this in a "pipeline" manner I've used `return self` to allows you to chain your operations.


```python
from metrics_collector.transform.base import BaseTransform
import pandera as pa
import datetime
import pandas as pd

class FooTransform(BaseTransform):
    input_schema = pa.DataFrameSchema(
        {
            "running": pa.Column(float, nullable=True),
            "walking": pa.Column(float, nullable=True)
        },
    )
    dag_name = 'garmin_and_apple'

    def process_pipeline(self, from_: datetime.date, to_: datetime.date) -> pd.DataFrame:
        """This is a required class method that returns a dataframe of transformed data"""
        (
            self.index_as_dt()
            .aggregate_combined_dataframes()
            .filter_period(from_, to_)
            .my_custom_transformer_method()
            .add_apple_garmin_distances()
            .add_missing_values()
        )
        return self.df
    
    def my_custom_transformer_method(self, some_param: str):
        """This is my custom method part of the pipeline"""
        self.df["my_additional_column"] = self.df.apply(lambda row: do_magic(row))
        return self
```

### Load/Graph data

This is the final step in the pipeline where the term Load a bit abused but just to signal this is the last step in an ETL pipeline.

#### Make graphs of data - Inherit from BaseLoadGraph

The same here goes as the previous step that the concrete class requires a dag_name variable used to identify itself belong to the extract and transform classes.
So you'll be focusing on creating your custom methods as part of this class.
Those class methods required are **get_all_graph_methods()** that should return an iterable of methods/functions whose purpose to return an object that shall have the **to_html()** and **to_png()** methods.
Whether the to_png or to_html used depend on the user e.g. using the web-UI with a more interactive experience, or sending reports as e.g. Email where a static PNG image found a better fit.


```python
from typing import Callable, Iterable, Annotated
from metrics_collector.load.base import BaseLoadGraph
import plotly
import plotly.express as px


class GarminAppleLoadGraph(BaseLoadGraph):
    dag_name = 'garmin_and_apple'

    def get_all_graph_methods(self) -> Iterable[Annotated[Callable, "Class methods generating graphs"]]:
        return self.my_walking_graph, self.my_running_graph

    def to_html(self, graph_method: Callable) -> str:
        """Based in this example where we use Figure we'll automatically have a to_html method"""
        return graph_method().to_html(include_plotlyjs="require", full_html=False)

    def to_png(self, graph_method: Callable) -> bytes:
        return graph_method().to_image(format="png")

    def my_walking_graph(self) -> plotly.graph_objs.Figure:
        """This is my custom method that will return a Figure"""
        df = self.df.resample('W').sum()  # now magically self.df is available from previous class
        fig = px.bar(df, y=...)
        ...
        return fig
    
    def my_running_graph(self) -> plotly.graph_objs.Figure:
        """My other running graph"""
        ...

```

So with this you should now have cleaned up and refactored your code from Jupyter notebook following protocols.
And as a bonus an abstract layer is now able to treat this as one-of-many services and presents interfaces and functionalities you didn't previously have for e.g. publishing as a dashboard or scheduling reports.

## Software design

So the main idea behind is primarily to abstract the logic that chenges the most and with a few different abstract classes supposed to represent each phase most commonly referred as Extract -> Transform -> Load .. so in short summary you would need to implement the following abstract classes

BaseExtract
BaseTransform
BaseLoad

And implement their abstract methods which with help from code-completion of your IDE will make it relatively easy.
On startup of this application we'll use the class inheritance and and their "dag_name" as being the mutual key that binds these steps into what I might be abuse the term related to DAG to expose them as option usable from the different user interfaces.

....

Facade pattern (Orchestrator)
Strategy pattern (passing progress bar per reference)
Template method pattern



## Credits

.....