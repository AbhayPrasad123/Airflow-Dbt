FROM apache/airflow:3.3.0

USER root

RUN apt-get update && apt-get install -y git

USER airflow

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-databricks