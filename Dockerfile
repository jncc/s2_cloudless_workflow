FROM ubuntu:24.04 as base

RUN apt update
RUN apt install -y python3 python3-virtualenv pip

RUN virtualenv venv
RUN source /venv/bin/activate
RUN pip install s2cloudless

FROM base as workflow

RUN source /venv/bin/activate
RUN mkdir /workflow

COPY ./workflow /workflow
WORKDIR /workflow
RUN pip install -r requirements.txt

FROM workflow as dev
