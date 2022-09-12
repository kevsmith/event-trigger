FROM --platform=linux/amd64 python:3.8

RUN apt-get update && apt-get upgrade -y

RUN pip install --upgrade pip && pip install nats-py requests

COPY trigger.py /trigger.py

CMD python /trigger.py