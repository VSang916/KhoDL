FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.2}

ENV PATH="/home/airflow/.local/bin:${PATH}"

COPY requirements.txt /tmp/requirements.txt

RUN pip install --user --no-cache-dir -r /tmp/requirements.txt

USER airflow

RUN pip install plyvel ckanapi

COPY airflow.cfg /opt/airflow/