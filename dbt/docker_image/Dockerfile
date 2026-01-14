FROM python:3.10-alpine

WORKDIR /opt
COPY requirements.txt .
RUN apk add --no-cache bash && pip install -r requirements.txt && mkdir dbt \
    && chmod -R 777 dbt
WORKDIR /opt/dbt
USER root