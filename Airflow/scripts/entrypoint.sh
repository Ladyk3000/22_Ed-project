#!/usr/bin/env bash

airflow db upgrade
#airflow initdb
airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
airflow connections --add --conn_id=google_cloud_default --conn_type=google_cloud_platform --conn_extra='{ "extra__google_cloud_platform__key_path":"/opt/airflow/credentials/seventh-oven-347013-bc0e6ba03fe0.json", "extra__google_cloud_platform__project": "seventh-oven-347013", "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/drive"}'

airflow webserver
