import datetime
from typing import Iterable, Type, Annotated

import pywebio.input
from pywebio.input import input, FLOAT, radio
from pywebio.output import put_html, put_processbar, set_processbar, put_text, clear

from metrics_collector.extract.base import BaseExtractParameters, BaseExtract
from metrics_collector.load.base import BaseLoadGraph
from metrics_collector.orchestrator.generic import Orchestrator, ClassType
from loguru import logger
from metrics_collector.utils import get_days_between

from metrics_collector.transform import BaseTransform

Params = dict[Annotated[str, "param name"], Annotated[str, "value"]]


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


# def dict_to_extract_params_object(params: Params, extract_class: BaseExtract) -> BaseExtractParameters:
#     cls = extract_class.get_extract_parameter_class()
#     obj = cls(**params)
#     return obj


def main_ui():
    o = Orchestrator()

    # get which dag
    args = o.get_extract_parameters()
    logger.debug(args)
    dag_name = space_shifter(radio("Chose what to extract", options=[space_shifter(str(arg)) for arg in args.keys()]))
    logger.debug(f'{dag_name=}')
    from_ = input('From date', type='date')
    to_ = input('To date', type='date')

    # request params as dict from ui
    extract_params = {}
    for args_def in o.get_extract_args_def(dag_name):
        args_ = ui_get_params(args_def)  # This could in theory be changed with a different method
        extract_params.update(args_)
    extract_objects = o.get_extract_objects(dag_name, extract_params)

    # TODO: get_data for all extract objects
    dates = list(get_days_between(from_, to_))
    tot = len(list(dates)) * len(extract_objects)
    put_text('Processing data from services')
    put_processbar('download_bar')
    for idx_extract, extract_object in enumerate(extract_objects, start=1):
        for idx_date, date in enumerate(dates):
            current_count = idx_date * idx_extract
            logger.info(f'downloading {current_count}/{tot} [{extract_object}]')
            extract_object.get_data(date)  # This could also be changed to different context
            set_processbar('download_bar', current_count / tot)
    set_processbar('download_bar', 1)
    put_text('Massaging data and rendering charts')

    transform_object = o.get_transform_object(dag_name, extract_objects)  # Important to be used next step

    clear()
    for graph_data in o.get_all_graphs(from_, to_, dag_name, transform_object, 'html'):  # Used to get graph results
        put_html(graph_data)
