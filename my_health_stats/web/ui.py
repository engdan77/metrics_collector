import datetime
from typing import Iterable, Type, Annotated

from pywebio.input import input, FLOAT, radio
from pywebio.output import put_text

from my_health_stats.extract.base import BaseExtractParameters
from my_health_stats.load.base import BaseLoadGraph
from my_health_stats.orchestrator.generic import Orchestrator, ClassType
from loguru import logger

from my_health_stats.transform import BaseTransform


def space_shifter(text: str) -> str:
    if '_' in text:
        return text.replace('_', ' ').capitalize()
    else:
        return text.replace(' ', '_').lower()


def ui_get_arguments(param_defintion: dict[Annotated[str, "parm name"], Type]) -> BaseExtractParameters:
    ...
    # TODO: prompt and return


def main_ui():
    o = Orchestrator()
    args = o.get_extract_parameters()
    logger.debug(args)
    dag_name = space_shifter(radio("Chose what to extract", options=[space_shifter(str(arg)) for arg in args.keys()]))
    logger.debug(f'{dag_name=}')
    extract_classes = o.get_registered_classes(dag_name, ClassType.extract)

    # create extract objects
    for extract_class in extract_classes:
        get_args = args[dag_name][extract_class]
        logger.debug(f"get arguments for {extract_class=} which is {get_args}")
        ...
        extract_object = extract_class(...)  # add args

    transfomer_class: BaseTransform = o.get_registered_classes(dag_name, ClassType.transform, only_first=True)
    t = transfomer_class(*extract_objects)
    load_class: BaseLoadGraph = o.get_registered_classes(dag_name, ClassType.load, only_first=True)
    from_ = datetime.date(2022, 1, 1)
    to_ = datetime.date(2022, 1, 3)
    l = load_class(t, from_, to_)
    graphs = o.get_graph_methods(dag_name)
    logger.debug(graphs)

    # height = input("Input your height(cm)：", type=FLOAT)
    # weight = input("Input your weight(kg)：", type=FLOAT)
    #
    # BMI = weight / (height / 100) ** 2
    #
    # top_status = [(16, 'Severely underweight'), (18.5, 'Underweight'),
    #               (25, 'Normal'), (30, 'Overweight'),
    #               (35, 'Moderately obese'), (float('inf'), 'Severely obese')]
    #
    # for top, status in top_status:
    #     if BMI <= top:
    #         put_text('Your BMI: %.1f. Category: %s' % (BMI, status))
    #         break