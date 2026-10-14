FROM apache/airflow:2.9.3-python3.11

USER root
# Java is required by PySpark; curl is used to fetch the S3A connector jars.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow
ENV HOME=/home/airflow \
    VENV_ROOT=/home/airflow/venvs

# 1) PySpark lives in the Airflow environment (runs as a local-mode Spark job).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 2) S3A connector so Spark can talk to LocalStack (Hadoop 3.3.4 matches Spark 3.5.x).
RUN SPARK_JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && curl -fsSL -o "${SPARK_JARS}/hadoop-aws-3.3.4.jar" \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o "${SPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar" \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 3) dbt and Soda get isolated virtualenvs to avoid dependency clashes with Airflow.
RUN python -m venv ${VENV_ROOT}/dbt \
 && ${VENV_ROOT}/dbt/bin/pip install --no-cache-dir "dbt-duckdb>=1.8,<1.9" "duckdb==1.1.3" \
 && ${VENV_ROOT}/dbt/bin/python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

RUN python -m venv ${VENV_ROOT}/soda \
 && ${VENV_ROOT}/soda/bin/pip install --no-cache-dir "soda-core-duckdb>=3.3,<3.4" "duckdb==1.1.3" \
 && ${VENV_ROOT}/soda/bin/python -c "import duckdb; duckdb.sql('INSTALL httpfs')"
