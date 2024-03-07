#!/bin/bash
AWS_ACCESS_KEY_ID=$(aws --profile default configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws --profile default configure get aws_secret_access_key)
AWS_DEFAULT_REGION=$(aws --profile default configure get region)
AIRFLOW_CONTAINER_ID=$(docker ps -aqf name=^/airflow$)

echo "Creating AWS connection in Airflow"

docker exec -it ${AIRFLOW_CONTAINER_ID} airflow connections delete aws_default || true
docker exec -it ${AIRFLOW_CONTAINER_ID}  airflow connections add \
    --conn-type "aws" \
    --conn-extra '{ "aws_access_key_id":"'${AWS_ACCESS_KEY_ID}'", "aws_secret_access_key": "'${AWS_SECRET_ACCESS_KEY}'", "region_name":"'${AWS_DEFAULT_REGION}'" }' \
    aws_default
