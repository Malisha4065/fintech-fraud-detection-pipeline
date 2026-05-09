FROM apache/airflow:2.7.3-python3.10

# The reconciliation DAG reads/writes Parquet files and connects to PostgreSQL.
RUN pip install --no-cache-dir \
    pandas==2.1.3 \
    pyarrow==14.0.1 \
    psycopg2-binary==2.9.9
