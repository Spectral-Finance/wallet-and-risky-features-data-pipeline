# 1. Base image
FROM python:3.10-slim

ARG APP_ENV='dev'
ARG SSH_TUNNEL='false'
ARG CODEARTIFACT_TOKEN

# 👇 python
ENV APP_DIR='/project' \
    APP_ENV=${APP_ENV} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.6.1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry'

# 👇
WORKDIR "$APP_DIR"

ENV PYTHONPATH "${PYTHONPATH}:/project"

# 👇 just necessary files.
COPY ./poetry.lock .
COPY ./pyproject.toml .
COPY ./config.py .
COPY ./main.py .
COPY ./src/ /project/src/
COPY ./config/ /project/config/

# 👇 Install python packages
RUN pip install "poetry==$POETRY_VERSION" && \
    poetry config virtualenvs.create false && \
    poetry install
