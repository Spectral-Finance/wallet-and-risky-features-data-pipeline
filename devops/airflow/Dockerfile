FROM apache/airflow:2.2.2

WORKDIR /opt/airflow

USER airflow

COPY requirements-dev.txt requirements.txt
RUN python3 -m pip install -r requirements.txt

ADD config/airflow-variables.json .
ADD config/init.sh .
