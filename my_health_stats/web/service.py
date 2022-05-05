import uvicorn
from loguru import logger
import asyncio
from uvicorn_loguru_integration import run_uvicorn_loguru
from fastapi import FastAPI
from pywebio.platform.fastapi import asgi_app
from pywebio.output import put_text
from my_health_stats.web.ui import main_ui
app = FastAPI()


def start_web():
    loop = asyncio.new_event_loop()

    # adding ui
    ui = asgi_app(main_ui)
    app.mount("/ui", ui)

    # main config
    config = uvicorn.Config(app=app, host='0.0.0.0', port=5050, log_level='info', loop='asyncio')
    server = uvicorn.Server(run_uvicorn_loguru(config))
    loop.run_until_complete(server.serve())


if __name__ == '__main__':
    start_web()
