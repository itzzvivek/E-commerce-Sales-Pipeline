FROM apache/airflow:3.0.6

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential libpq-dev gcc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


FROM apache/superset:latest
USER root
RUN pip install psycopg2-binary
USER superset

