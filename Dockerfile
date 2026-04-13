FROM apache/airflow:2.7.1

USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    pandas \
    minio \
    psycopg2-binary \
    s3fs \
    pyarrow \
    delta-spark \
    pyspark