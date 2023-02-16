FROM --platform=linux/amd64 apache/airflow:2.5.0-python3.10

LABEL team="SUT"

USER root
RUN apt -y update && apt install -y p7zip-full

USER airflow
RUN pip install \
    airflow-clickhouse-plugin==0.10.0 \
    clickhouse-cityhash==1.0.2.4 \
    lz4==4.3.2 \
    airflow-providers-clickhouse==0.0.1 \
    clickhouse-connect==0.5.11