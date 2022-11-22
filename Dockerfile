FROM python:3.10-slim

ENV DATA_DIR=/app/data
ENV IS_DOCKER=1
EXPOSE 5050/tcp

RUN apt-get update
RUN apt-get install --yes gcc python3-dev

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
COPY ./setup.py /app/setup.py
COPY ./MANIFEST.in /app/MANIFEST.in
COPY ./metrics_collector /app/metrics_collector
RUN mkdir /app/data

RUN python3 setup.py install
CMD ["metrics_collector"]

