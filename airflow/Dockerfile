FROM apache/airflow:3.1.1-python3.11

USER root

RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.19 \
    sqlmodel==0.0.22 \
    psycopg2-binary==2.9.9 \
    python-dateutil==2.9.0

RUN mkdir -p ~/airflow/dags

ENV AIRFLOW_HOME=/home/airflow/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_FOLDER=/home/airflow/airflow/dags
ENV AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True

RUN airflow db migrate