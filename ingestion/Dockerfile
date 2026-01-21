FROM python:3.11.14-alpine

WORKDIR /opt
COPY requirements.txt .
RUN apk add --no-cache bash \ 
    && apk add --no-cache cargo \
    && pip install --upgrade pip wheel setuptools \
    && pip install -r requirements.txt 

USER root