SHELL := /bin/bash
AWS_ACCOUNT_ID ?=
REPOSITORY_NAME ?= 
DOMAIN_NAME ?= 
DOMAIN_OWNER ?=
LIBRARY_NAME= 
CODEARTIFACT_USER = 
OS_NAME := `uname -s | tr A-Z a-z`
PROJECT_NAME=wallet-and-risky-features-data-pipeline
ENVIRONMENT=dev
IMAGE_NAME = ${PROJECT_NAME}
SDL_REPOSITORY_NAME = 
SDL_PACKAGE_NAME =
AWS_REGION ?= 

# Requires docker to be installed in your OS
login_ecs_docker_registry:
	aws ecr get-login-password --region us-east-2 | \
	docker login --username AWS --password-stdin ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com

build_image:
	docker build --platform=linux/amd64 --build-arg CODEARTIFACT_TOKEN=`aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} \
	--domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` \
	-t data-lakehouse/${IMAGE_NAME} -f devops/ecs/Dockerfile .

# Push the docker image to ECR
push_image_to_ecr:
	docker build --platform=linux/amd64 --build-arg CODEARTIFACT_TOKEN=`aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} \
	--domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` \
	-t data-lakehouse/${IMAGE_NAME} -f devops/ecs/Dockerfile . \
	&& docker tag data-lakehouse/${IMAGE_NAME}:latest ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/${IMAGE_NAME}-${ENVIRONMENT}:latest \
	&& docker push ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/${IMAGE_NAME}-${ENVIRONMENT}:latest

# Create a poetry virtual environment to run the project locally - Linux
# Requires poetry to be installed in your OS
create_local_env:
	CODEARTIFACT_AUTH_TOKEN=`aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} \
	--domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` && \
	CODEARTIFACT_REPOSITORY_URL=`aws codeartifact get-repository-endpoint --domain ${DOMAIN_NAME} \
		--domain-owner ${DOMAIN_OWNER} --repository ${REPOSITORY_NAME} --format pypi --query repositoryEndpoint --output text`

	poetry config repositories.${REPOSITORY_NAME} ${CODEARTIFACT_REPOSITORY_URL} \
	&& poetry config http-basic.${REPOSITORY_NAME} ${CODEARTIFACT_USER} `aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} --domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` \
	&& poetry source add spectral-data-repository --secondary https://${DOMAIN_NAME}-${DOMAIN_OWNER}.d.codeartifact.us-east-2.amazonaws.com/pypi/spectral-data-repository/simple/ \
	&& poetry install \
	&& poetry add spectral-data-lib==2.1.1 --source spectral-data-repository
	@echo "Virtual environment created and activated, run 'poetry shell' to activate it"

# Delete the poetry virtual environment to run the project locally
delete_local_env:
	rm -rf `poetry env info -p`
	@echo "Virtual environment deleted"

create_modules_file_and_upload_to_s3:
	zip -r modules.zip config config.py log_manager.py src -x "src/pipelines/raw/*" "src/pipelines/stage/transformations/*" "src/pipelines/analytics/transformations/*" \
	&& aws s3 cp modules.zip s3://data-lakehouse-${ENVIRONMENT}/configs/scripts/${PROJECT_NAME}/ \
	&& aws s3 cp src s3://data-lakehouse-${ENVIRONMENT}/configs/scripts/${PROJECT_NAME}/src/ --recursive --exclude '*' --include '*.sql' \
	&& aws s3 cp main.py s3://data-lakehouse-${ENVIRONMENT}/configs/scripts/${PROJECT_NAME}/

create_pyspark_deps_file_and_upload_to_s3:
	DOCKER_BUILDKIT=1 docker build --build-arg CODEARTIFACT_TOKEN=`aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} \
	--domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` \
	--output . -t data-lakehouse/${PROJECT_NAME}-emr -f devops/emr/Dockerfile . \
	&& aws s3 cp pyspark_deps.tar.gz s3://data-lakehouse-${ENVIRONMENT}/configs/scripts/${PROJECT_NAME}/

setup_airflow_locally:
	cd devops/airflow && docker-compose up -d && ./config/setup_aws_connection.sh

create_ecs_stack:
	cd devops/ecs/infrastructure && \
	terraform workspace select ${ENVIRONMENT} && \
	terraform apply

delete_ecs_stack:
	cd devops/ecs/infrastructure && \
	terraform workspace select ${ENVIRONMENT} && \
	terraform destroy

conf_poetry_aws_repository:
	poetry config repositories.${SDL_REPOSITORY_NAME} https://spectral-${AWS_ACCOUNT_ID}.d.codeartifact.${AWS_REGION}.amazonaws.com/pypi/${SDL_REPOSITORY_NAME}/
	poetry config http-basic.${SDL_REPOSITORY_NAME} ${CODEARTIFACT_USER} `aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} --domain-owner ${AWS_ACCOUNT_ID} --query authorizationToken --output text`

poetry_add_latest_spectral_data_lib:
	poetry source add spectral-data-repository --secondary \
		https://spectral-${AWS_ACCOUNT_ID}.d.codeartifact.${AWS_REGION}.amazonaws.com/pypi/${SDL_REPOSITORY_NAME}/simple/
	poetry add ${SDL_PACKAGE_NAME}==`aws codeartifact list-package-versions --domain ${DOMAIN_NAME} --repository ${SDL_REPOSITORY_NAME} \
		--package ${SDL_PACKAGE_NAME} --format pypi --status Published --sort-by PUBLISHED_TIME \
		| jq -r '.versions[0].version'` \
		 --source ${SDL_REPOSITORY_NAME}
