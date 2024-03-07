#!/bin/sh
set -e

IMAGE_NAME=$1
ENVIRONMENT=$2
DOMAIN_NAME=spectral
DOMAIN_OWNER=${DOMAIN_OWNER}

cd ../../../; # go to root of project

# login to ECR
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com;

# Build the docker image
docker build --platform=linux/amd64 --build-arg CODEARTIFACT_TOKEN=`aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} \
	--domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` \
	-t data-lakehouse/${IMAGE_NAME}-${ENVIRONMENT} -f devops/ecs/Dockerfile .;

# Tag the image
docker tag data-lakehouse/${IMAGE_NAME}-${ENVIRONMENT}:latest ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/${IMAGE_NAME}-${ENVIRONMENT}:latest;

# Push the image to ECR
docker push ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/${IMAGE_NAME}-${ENVIRONMENT}:latest;
