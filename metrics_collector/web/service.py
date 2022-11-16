from pathlib import Path
from typing import Iterable

import uvicorn
from loguru import logger
import asyncio
from uvicorn_loguru_integration import run_uvicorn_loguru
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pywebio.platform.fastapi import asgi_app

from metrics_collector.scheduler.api import AsyncService
from metrics_collector.web.rest import graph_router
from metrics_collector.web.ui import ui_show, ui_add_schedule, ui_remove_schedule

app = FastAPI()
app.include_router(graph_router, prefix='/graph')


@app.get('/')
def main_route():
    return RedirectResponse('/static')


class WebServer:

    async_services_start_method = None
    extra_async_services = None

    def __init__(self, extra_async_services: Iterable[AsyncService] = None, port=5050):
        self.__class__.extra_async_services = extra_async_services
        self.mounts()
        self.config = uvicorn.Config(
            app=app, host="0.0.0.0", port=port, log_level="debug"
        )
        app.add_event_handler("startup", WebServer.startup_events)
        self.server = uvicorn.Server(run_uvicorn_loguru(self.config))
        asyncio.run(self.server.serve())

    @staticmethod
    def startup_events():
        """
        This is essential part for starting services after uvicorn event loop been created
        for those services to hook start within.

        :return:
        """
        for service in WebServer.extra_async_services:
            logger.debug(f"starting {service}")
            service.start()

    def mounts(self):
        app.mount("/action/show", asgi_app(ui_show))
        app.mount("/action/add_schedule", asgi_app(ui_add_schedule))
        app.mount('/action/remove_schedule', asgi_app(ui_remove_schedule))
        app.mount("/static", StaticFiles(directory=Path(__file__).parent / 'static', html=True), name="static")

    def start(self):
        self.server.serve()


if __name__ == "__main__":
    ...
