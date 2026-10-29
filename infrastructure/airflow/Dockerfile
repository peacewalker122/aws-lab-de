FROM docker.io/apache/airflow:3.1.0-python3.12

USER root

COPY --chmod=755 infrastructure/airflow/entrypoint.sh /opt/aster/entrypoint.sh

USER airflow

RUN pip install --no-cache-dir \
    boto3==1.40.70 \
    duckdb==1.4.2 \
    opentelemetry-exporter-otlp-proto-http==1.45.1 \
    opentelemetry-sdk==1.45.1 \
    pyarrow==21.0.0 \
    "psycopg[binary]==3.2.12"

RUN python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"
