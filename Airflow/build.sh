#!/usr/bin/env bash

docker build . -f Dockerfile -t airflow-etl

docker-compose -f docker-compose.yml up -d --build --force-recreate