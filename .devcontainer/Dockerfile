FROM mcr.microsoft.com/devcontainers/python:dev-3.11-bullseye

ENV PYTHONUNBUFFERED=1

# Install system dependencies and Python tools
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN pip3 install --upgrade pip
# Install pre-commit and detect-secrets globally so they are available.
# pre-commit will manage its own specific version for the hook from your config.
RUN pip3 install --upgrade pre-commit detect-secrets
# [Optional] If your requirements rarely change, uncomment this section to add them to the image.
COPY requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
   && rm -rf /tmp/pip-tmp
