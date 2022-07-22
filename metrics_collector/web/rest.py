"""Module for dynamically create a REST API based on classes registered"""
import itertools

from fastapi_utils.enums import StrEnum
from fastapi import Response
from loguru import logger
from makefun import create_function
from fastapi import APIRouter, Request, HTTPException
from metrics_collector.orchestrator.generic import Orchestrator
import datetime

graph_router = APIRouter()
o = Orchestrator()
DagName = StrEnum('DagName', o.get_dag_names())


def rest_get_extract_params(query_params, dag_name, orchestrator):
    extract_params = {}
    for args_def in orchestrator.get_extract_params_def(dag_name):
        ...
        extract_params.update({})
    return extract_params


def graph(**args):
    media_type = "image/png"  # Possibly add other options in future to allow other media types
    logger.debug(f'{args=}')
    dag_name = args['request'].scope['path'].split('/').pop()
    logger.debug(f'{dag_name=}')
    any_params_none = any([_ is None for _ in args.values()])
    from_ = args['from_date']
    to_ = args['to_date']
    graph_name = args['graph']

    required_params = set(itertools.chain.from_iterable([_.keys() for _ in o.get_extract_params_def(dag_name)]))
    extract_params = o.get_stored_params(dag_name)
    if any_params_none and set(extract_params.keys()) == required_params:
        extract_params = o.get_stored_params(dag_name)
    elif any_params_none:
        return HTTPException(400, 'missing parameters')
    else:
        extract_params = args

    extract_objects = o.get_extract_objects(dag_name, extract_params)
    o.process_dates(extract_objects, from_, to_, progress_bar=None)
    logger.debug('completed processing dates')
    transform_object = o.get_transform_object(dag_name, extract_objects)  # Important to be used next step
    logger.debug('processing and rendering graph')
    graph_result = o.get_graph(graph_name, from_, to_, dag_name, transform_object, 'png')
    return Response(content=graph_result, media_type=media_type)


# Some magic to dynamically create API endpoints
for dag_name in o.get_dag_names():
    dag_args = o.get_extract_services_and_parameters().get(dag_name)
    all_args = []

    graph_names = o.get_graph_names(dag_name)  # get all graph names options
    GraphEnum = StrEnum('GraphEnum', graph_names)
    all_args.append(f'graph: GraphEnum')

    for arg_name, default_date in (('from_date', 'today() - datetime.timedelta(days=1)'), ('to_date', 'today()')):
        all_args.append(f'{arg_name}: datetime.date = datetime.date.{default_date}')

    for cls, args in dag_args.items():  # add args for extract class
        for arg, type_ in args.items():
            all_args.append(f'{arg}: {type_.__name__} | None=None')
    all_args.append(f'request: Request = None')

    gen_func = create_function(f'{dag_name}({", ".join(all_args)})', graph)
    graph_router.add_api_route(f'/{dag_name}', gen_func)

