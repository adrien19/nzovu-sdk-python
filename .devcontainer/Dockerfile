FROM python:3.12.14-bookworm@sha256:dbbe4ceb97851e2e5fa83798b239811f871cb743b259ba3563737349f6bcfaa0

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000

RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client make \
    && rm -rf /var/lib/apt/lists/*

COPY .devcontainer/requirements.txt /tmp/sdk-tools.txt
RUN python -m venv /opt/poetry \
    && /opt/poetry/bin/pip install --no-cache-dir -r /tmp/sdk-tools.txt \
    && rm /tmp/sdk-tools.txt

ENV PATH="/opt/poetry/bin:${PATH}" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    POETRY_KEYRING_ENABLED=false

RUN groupadd --gid "$USER_GID" "$USERNAME" \
    && useradd --uid "$USER_UID" --gid "$USER_GID" --create-home --shell /bin/bash "$USERNAME"

USER $USERNAME
WORKDIR /workspace
