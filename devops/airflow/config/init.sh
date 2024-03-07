#!/bin/bash -x

# Database
airflow db init

# User
airflow users create \
    --role "Admin" \
    --username "admin" \
    --password "admin" \
    --email "admin@airflow.com" \
    --firstname "ad" \
    --lastname "min"

# Variables
airflow variables import "airflow-variables.json"

# It's show time!
airflow webserver -p 8080 &
airflow scheduler &
tail -f /dev/null
