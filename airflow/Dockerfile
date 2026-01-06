FROM apache/airflow:2.10.5

USER root
RUN sudo apt-get update && sudo apt-get install -y iputils-ping

USER airflow

COPY requirements.txt /
RUN pip install --upgrade setuptools wheel && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

COPY package/gspread_extended/dist custom_package/gspread_extended/dist
RUN pip install custom_package/gspread_extended/dist/gspread_extended-0.1.0-py3-none-any.whl