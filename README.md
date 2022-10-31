# Metrics Collector

## Features

- Abstracts complexity from an existing time-based series analysis project (e.g. using Jyputer notebook) that dynamically builds into a service that provides ... 
  - ... a user friendly web interface for input data and as result plotting charts
  - ... a REST API for easier integrating your work to external services, such as a dashboard hosted anywhere
  - ... a scheduler to allow easily setup recurring events such as sending charts to specific email
  - ... a common JSON-based cache storage to allow a local copy of all input data exchanged allowed to be used in other future contexts
- Extends easily by using class inheritance

## Motivation and background

Main goal with this project was to develop an application that easily allowed me to collect and present time-based metrics from different services, could be anything such as market prices of house, health data such as how distance run etc.

At the same time allow me excercise writing maintainable and extensible Python code following good design principles.

So this is when the "**Metrics Collector**" was first developed, which is an application to be highly customisable for collecting metric from different data points, deal with the processing and dynamically expose certain basic user interfaces such as REST API, a user-friendly web interface and also a scheduler to allow e.g. sending these time-based graphs.

And the code been written in such way, and thankfully to Pythons dynamic nature automatically make this into a more seamless experience due to the fact that the only work required to extend additional services is to inherit from a very few different base-classes.

This also gave me a chance to familiarize myself with GitHub actions allow automatic the CI/CD pipeline into the PyPI.

<img src="https://raw.githubusercontent.com/engdan77/project_images/master/pics/hld_concepts.png" alt="hld_concepts" style="zoom: 67%;" />

## Installation

### From source

```shell
$ python3 -m venv venv
$ source venv/bin/activate
$ pip3 install git+https://github.com/engdan77/metrics_collector.git
```



## Usage

### From source

Start it from within the terminal by `metrics_collector` with the optional `--port` and `--data-dir` 

![cli_metrics_collector](https://raw.githubusercontent.com/engdan77/project_images/master/pics/cli_metrics_collector.png)

Use your browser to connect to http://127.0.0.1:5050 or the port that you can change with the --port argument.

## Tutorial - implement your own service

For the most cases one may prefer to work within a [Juypter notebook](https://jupyter.org) to test get data, clean it and make chart of these.
So what this project aim for is to supply a few abstract base classes to you can conform you existing code without need to think too much about the code later on that allow you to easily access data.

### <u>Extract step</u>

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

### <u>Transform step</u>

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
    dag_name = 'foo'

    def process_pipeline(self, from_: datetime.date, to_: datetime.date) -> pd.DataFrame:
        """This is a required class method that returns a dataframe of transformed data"""
        (
            self.index_as_dt()
            .aggregate_combined_dataframes()
            .filter_period(from_, to_)
            .my_custom_transformer_method()
            .add_missing_values()
        )
        return self.df
    
    def my_custom_transformer_method(self, some_param: str):
        """This is my custom method part of the pipeline"""
        self.df["my_additional_column"] = self.df.apply(lambda row: do_magic(row))
        return self
```

### <u>Load/Graph data</u>

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


class FooLoadGraph(BaseLoadGraph):
    dag_name = 'foo'

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

### <u>Orchestrator</u>

Now this is the last piece not that a normal user won't need to think about but rather the existing built-in interfaces does use for interfaces such as REST-api and Web User Interface, and this orchestrator class following [facade pattern](https://en.wikipedia.org/wiki/Facade_pattern) allowing easier usage in the future, and borrows some ideas from [template method pattern](https://en.wikipedia.org/wiki/Template_method_pattern) in the sense we outline the process in advance and allow concrete classes to fill in the steps required by a new service.

To give a simpler example this is how one instantiate the Orchestrator-class and have it extract, transform and finally return graphs.

```python
# instantiate orchestrator
o = Orchestrator()

# get dag_name and period
dag_name, from_, to_ = ('foo', '2022-03-01', '2022-03-10')  

# OPTIONAL: dynamically get required parameters
extract_params_definition = o.get_extract_params_def(dag_name)

# required extract params based on abstract class registered for dag_name
extract_params = {'uri_for_foo_service': 'ftp://foo:bar@127.0.0.1:10021/export.zip'}

# OPTIONAL: method allow get previous cached data
extract_params = o.get_stored_params(dag_name)

# EXTRACT: required with extract_params as dict
extract_objects = o.get_extract_objects(dag_name, extract_params)

# OPTIONAL: callback function presenting progress between 0.0 to 1.0, following strategy pattern
pb = my_progress_bar()

# processing those dates
o.process_dates(extract_objects, from_, to_, progress_bar=pb)

# TRANSFORM: important to be used next step
transform_object = o.get_transform_object(dag_name, extract_objects)

# LOAD: used to get graph results
for graph_data in o.get_all_graphs(from_, to_, dag_name, transform_object, 'png'):
  # custom handler for handling e.g. png or html
  do_something_with_graph_data(graph_data)

```







## Software design

Overall

So as mentioned earlier, the main components of this design could be described as

- Backend
  - Orchestrator - the class that ties the ETL steps together (Facade)
  - Cache - this part assures there is no need to retrieve data already existing
  - Scheduler - this part responsible to perform scheduled metrics e.g. send as email
- Frontend
  - Web UI - this allow the user to execute ETL based on available services
  - REST API - this expose the available metrics and makes it easier integrate with other platforms

### Abstracted backend backend logic

So the main idea behind is primarily to abstract that common logic related to such time-based analsysis projects into a few different re-usable classes and supposed to represent each phase most commonly referred as **Extract** -> **Transform** -> **Load** .. and following a [template method pattern](https://en.wikipedia.org/wiki/Template_method_pattern) approach for the following ...

Base**E**xtract ... responsible for extract raw data from its source
Base**T**ransform ... transform and cleaning data
Base**L**oad** ... generate graphs 

** *The term "Load" found slight abused since typically this phase is for storing data while in our case represents generating graphs*

.. and implementing their abstract methods which with help from code-completion of your IDE will make it relatively easy for one to add other services and additional graphs as one wish. On startup of this application we'll use the class inheritance and their "dag_name" as being the mutual key that binds these steps into what I might be abuse the term related to DAG to expose them as option usable from the different user interfaces.

Conceptually how these classes relates to each others and the steps being responsible for could be simplified and illustrated such as below diagram and we follow [strategy pattern](https://en.wikipedia.org/wiki/Strategy_pattern) allowing us to selecting algorithms in runtime e.g. "transform" initializes using an "extract" object... and "load graph" initialized with "transform".

#### High Level Design

```mermaid
%%{init: {'theme':'dark'}}%%
flowchart
subgraph Conceptual design for class interfaces
    direction BT
    subgraph BP["Orchestrator abstraction"]
        et["class Orchestrator()\n+get_extract_params_def(dag_name)\n+get_extract_objects(dag_name, extract_params)\n+process_dates(extract_objects, from_, to_, progress_bar=pb)\n+get_transform_object(dag_name, extract_objects)\n+get_all_graphs(from_, to_, dag_name, transform_object, 'html')\n"]
    end
        subgraph ETL
            direction LR
                subgraph Extract_Data
                direction BT
                    gd["concrete class\ndag_name\n+get_data_from_service()"] --> BE["abstract BaseExtract()"]
                end
                subgraph Transform_Data
                direction BT
                    pp["concrete class\ndag_name\n+process_pipeline(from, to)\n+input_schema"] --> BT["abstract BaseTransform()"]
                end
                subgraph Graph_Data
                direction BT
                    ga["concrete class\n+dag_name\nget_all_graph_methods(graph_format)\n+to_html()\n+to_png()"] --> BL["abstract BaseLoadGaph()"]
                end
            end
        subgraph FE["Web/REST"]
            x
        end
    end
    
Extract_Data ==> Transform_Data ==> Graph_Data
ETL ==> BP
BP ==> FE
```

### Scheduler and Orchestrator

How the classes responsible for keeping track of those scheduled ETL tasks and Orchestrator executing those, how they relate to each others could be illustrated as the below class diagram.

```mermaid
%%{init: {'theme':'dark'}}%%
classDiagram
    BaseAction <|-- EmailAction
    BaseAction <|-- CacheAction
    BaseAction o-- Orchestration

    BaseScheduleParams <|-- ScheduleParams
    Enum <|-- ActionType

    EmailAction o-- ActionType
    CacheAction o-- ActionType

    ScheduleConfig o-- ScheduleParams
    ScheduleConfig o-- ActionType
    ScheduleConfig o-- BaseAction


    class Orchestration {
    +get_extract_objects(extract_params)
    +process_dates(...,from ,to)
    +get_transform_object(...)
    +get_all_graphs(..., 'png')
    }

    class ScheduleConfig {
    <<dataclass>>
    +dag_name: str
    +from_: str
    +to_: str
    +extract_params: dict
    +schedule_params: ScheduleParams
    +action_type: ActionType
    +action_data: BaseAction
    __post_init__()
    }

    class ActionType {
    <<Enum>>
        +Email
        +Cache
    }

    class BaseAction {
        <<abstract>>
        -orchestration
        +action_type()
        +run()
    }

    class EmailAction {
        +to_email: str
        +subject: str
        +body: str
        +action_type()
        +run()
    }

    class CacheAction {
        +action_type()
        +run()
    }

    class BaseScheduleParams {
        <<abstract>>
        +__format__()
    }

    class ScheduleParams {
        +year: int | str
        +month: int | str
        +day: int | str
        +day_of_week: int | str
        +hour: int | str
        +minute: int | str
        +__format()__
    }

```



### Frontend

For the frontend I use two primary frameworks that makes this easy for us to expose to end-users, for the Web UI I use [PyWebIO](https://pywebio.readthedocs.io/en/latest/) together using [uvicorn](https://www.uvicorn.org) and [FastAPI](https://fastapi.tiangolo.com) to expose this as web service and use some techniques allow us to dynamically create [REST API](https://en.wikipedia.org/wiki/Representational_state_transfer) routes based on services available.



## Credits

Would primarily like to give shoutout to follow projects that have made this one possible .. 

- [Pandas](https://pandas.pydata.org)
- [Pandera](https://pandera.readthedocs.io/en/stable/)
- [Plotly](https://plotly.com)
- [FastAPI](https://fastapi.tiangolo.com)
- [UviCorn](https://www.uvicorn.org)
- [PyWebIO](https://pywebio.readthedocs.io)
- [APScheduler](https://apscheduler.readthedocs.io)
- [apprise](https://github.com/caronc/apprise)
- [Typer](http://typer.tiangolo.com/)
- [makefun](https://smarie.github.io/python-makefun/)