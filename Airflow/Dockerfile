FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

# default credentials: login - airflow, password - airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME
#USER $AIRFLOW_UID

COPY dags dags
COPY raw raw
COPY credentials credentials

#RUN chmod u=rwx,g=rwx,o=rwx ${AIRFLOW_HOME}/raw

COPY scripts scripts
RUN chmod +x scripts
