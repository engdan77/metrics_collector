from typing import Iterable, Type, Annotated, Protocol
import pywebio.input
from pywebio.input import input, FLOAT, radio
from pywebio.output import put_html, put_processbar, set_processbar, put_text, clear
from metrics_collector.orchestrator.generic import Orchestrator, ClassType, ProgressBar
from loguru import logger

Params = dict[Annotated[str, "param name"], Annotated[str, "value"]]


class WebProgressBar(ProgressBar):
    def __init__(self):
        self.current = 0
        self._name = 'download_bar'
        put_processbar(self._name)

    def update(self, progress: Annotated[float, "Between 0 and 1"]) -> None:
        set_processbar(self._name, progress)


def space_shifter(text: str) -> str:
    if '_' in text:
        return text.replace('_', ' ').capitalize()
    else:
        return text.replace(' ', '_').lower()


def ui_get_params(param_defintion: dict[Annotated[str, "parm name"], Type]) -> Params:
    """Take dict and get data using Web UI"""
    args = {}
    for label, type_ in param_defintion.items():
        match type_.__name__:
            case 'str' | 'int':
                args[label] = pywebio.input.input(label)
    return args


def ui_get_service_and_interval(o):
    # get which dag
    args = o.get_extract_services_and_parameters()  # Used to get which services are registered
    logger.debug(args)
    dag_name = space_shifter(radio("Chose what to extract", options=[space_shifter(str(arg)) for arg in args.keys()]))
    logger.debug(f'{dag_name=}')
    from_ = input('From date', type='date')
    to_ = input('To date', type='date')
    return dag_name, from_, to_


def main_ui():
    o = Orchestrator()

    dag_name, from_, to_ = ui_get_service_and_interval(o)

    # request params as dict from ui
    extract_params = {}
    for args_def in o.get_extract_args_def(dag_name):
        args_ = ui_get_params(args_def)  # This could in theory be changed with a different method
        extract_params.update(args_)
    extract_objects = o.get_extract_objects(dag_name, extract_params)

    put_text('Processing data from services')
    pb = WebProgressBar()
    o.process_dates(extract_objects, from_, to_, progress_bar=pb)
    set_processbar('download_bar', 1)   # To assure it shows 100%
    put_text('Massaging data and rendering charts')

    transform_object = o.get_transform_object(dag_name, extract_objects)  # Important to be used next step

    clear()
    for graph_data in o.get_all_graphs(from_, to_, dag_name, transform_object, 'html'):  # Used to get graph results
        put_html(graph_data)
