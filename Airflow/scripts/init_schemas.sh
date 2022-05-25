#!/bin/bash

psql -h "${DB_HOST}" -U "${POSTGRES_USER}" -p "5432" -c "create schema if not exists raw" "postgres"
psql -h "${DB_HOST}" -U "${POSTGRES_USER}" -p "5432" -c "create schema if not exists datamart" "postgres"