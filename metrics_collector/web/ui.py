import datetime
import itertools
import json
import re
from functools import partial
from typing import Type, Annotated
import pywebio.input
from parsedatetime import Calendar
from pywebio.input import input, radio, select, input_group
from pywebio.output import put_html, put_processbar, set_processbar, put_text, clear, put_table, put_buttons
from metrics_collector.orchestrator.generic import Orchestrator, ProgressBar
from loguru import logger

from metrics_collector.utils import normalize_date
from metrics_collector.scheduler.api import MyScheduler, EmailAction, scheduler_config_file, get_scheduler_config, \
    save_scheduler_config, ActionType, BaseAction, BaseScheduleParams, ScheduleConfig
from metrics_collector.scheduler import ScheduleParams

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

    period = {'from': None, 'to': None}

    def check_form(form_data):
        now = datetime.date.today()
        if form_data['date'] and form_data['text']:
            return ('date', 'Chose one of the options')
        if form_data['date'] and normalize_date(form_data['date']) <= now and not form_data['text']:
            return None
        if form_data['text']:
            c = Calendar()
            if c.parse(form_data['text'])[1] and normalize_date(form_data['text']) <= now:
                return None
            else:
                return ('text', 'Not a valid date by text')
        return ('date', 'Chose a date')

    for t in ('from', 'to'):
        data = input_group(f"{t.capitalize()} date:", [
            input(f'Calendar...', type='date', name='date'),
            input(f'...or text expression', name='text', type='text')
        ], validate=check_form)
        period[t] = data['date'] or data['text']
    return dag_name, period['from'], period['to']


def ui_get_extract_params(dag_name, orchestrator):
    # request params as dict from ui
    extract_params = {}
    for args_def in orchestrator.get_extract_params_def(dag_name):
        args_ = ui_get_params(args_def)  # This could in theory be changed with a different method
        extract_params.update(args_)
    return extract_params


def get_extract_params(dag_name, orchestrator):
    required_params = set(itertools.chain.from_iterable([_.keys() for _ in orchestrator.get_extract_params_def(dag_name)]))
    extract_params = orchestrator.get_stored_params(dag_name)
    if not set(extract_params.keys()) == required_params:
        extract_params = ui_get_extract_params(dag_name, orchestrator)
    else:
        for k, v in extract_params.items():
            put_text(f'{k}: {v}')
        if not {'Yes': True, 'No': False}.get(select(f'Would you like to use these existing params?', ['Yes', 'No']),
                                              None):
            extract_params = ui_get_extract_params(dag_name, orchestrator)
    return extract_params


def ui_get_schedule_options() -> ScheduleParams:
    """Define scheduling options"""

    def check_form(data):
        success, message = MyScheduler.verify_job(data)
        if not success:
            return 'year', f'{message}, check http://shorturl.at/bjOP0'

    fields = [input(_, type='text', name=_) for _ in ('year', 'month', 'day', 'day_of_week', 'hour', 'minute')]
    form: ScheduleParams = input_group('Schedule', fields, validate=check_form)
    logger.info(f'valid params {form}')
    return form


def ui_add_schedule():
    """This is UI to get input and add scheduled job"""
    o = Orchestrator()
    dag_name, from_, to_ = ui_get_service_and_interval(o)
    extract_params = get_extract_params(dag_name, o)  # determine if params already stored
    action_type, action_properties = ui_get_action_options()
    schedule_params = ui_get_schedule_options()
    schedule_config = ScheduleConfig(dag_name, from_, to_, extract_params, schedule_params, action_type, action_properties)
    save_scheduler_config(schedule_config)
    clear()
    put_text('Scheduler configuration updated, restarting schedules ...')


def ui_remove_schedule():
    """This is UI for removing existing scheduled job"""
    def delete_row(choice, row):
        logger.info(f'deleting item {row} from schedules')
        config = get_scheduler_config()
        config.pop(row)
        f = scheduler_config_file()
        f.write_text(json.dumps(config, indent=4))
        clear()
        put_text(f'Removed {row} from scheduled jobs')

    table_rows = [('#', 'Service', 'From', 'To', 'Action', 'Schedule', '')]
    # TODO: Add scheduled job - when
    for i, item in enumerate(get_scheduler_config()):
        dag_name, from_, to_, _extract_param, schedule_param, action_type, action_data = item.values()
        table_rows.append((i, dag_name, from_, to_, action_type, f'{to_base_class(schedule_param, BaseScheduleParams)}', f'{to_base_class(action_data, BaseAction)}', put_buttons(['delete'], onclick=partial(delete_row, row=i))))
    put_table(table_rows)


def to_base_class(input_data: dict, base_class=BaseAction):
    obj = None
    for cls in base_class.__subclasses__():
        try:
            obj = cls(**input_data)
        except TypeError:
            continue
    return obj


def ui_get_email_properties() -> EmailAction:
    def check_form(data_):
        if not re.match(r'^[\w\-.]+@([\w-]+\.)+[\w-]{2,4}$', data_['to_email']):
            return 'to_email', 'Not a valid email'

    data = input_group("Email properties", [
        input('To email', name='to_email'),
        input('Subject', name='subject'),
        input('Body', name='body')
    ], validate=check_form)
    return EmailAction(**data)


def ui_get_action_options() -> tuple[ActionType, BaseAction]:
    """Present the options for scheduler"""
    clear()
    selection = select('What action', ['Email', 'Cache'])
    action_type = getattr(ActionType, selection)
    action_properties = ui_get_email_properties() if action_type == ActionType.Email else None
    return action_type, action_properties


def ui_show():
    """This is the main UI for get input and plot graphs to the screen"""
    o = Orchestrator()
    dag_name, from_, to_ = ui_get_service_and_interval(o)
    extract_params = get_extract_params(dag_name, o)  # determine if params already stored
    extract_objects = o.get_extract_objects(dag_name, extract_params)  # required with extract_params as dict

    put_text('Processing data from services')
    pb = WebProgressBar()
    o.process_dates(extract_objects, from_, to_, progress_bar=pb)
    set_processbar('download_bar', 1)   # To assure it shows 100%
    put_text('Massaging data and rendering charts')

    transform_object = o.get_transform_object(dag_name, extract_objects)  # Important to be used next step

    clear()
    for graph_data in o.get_all_graphs(from_, to_, dag_name, transform_object, 'html'):  # Used to get graph results
        put_html(graph_data)
