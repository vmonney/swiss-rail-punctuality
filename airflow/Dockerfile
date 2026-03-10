FROM apache/airflow:2.10.5

USER root
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /opt/airflow

# Install locked project dependencies (including dbt) reproducibly.
COPY pyproject.toml uv.lock /tmp/project/
RUN uv export --project /tmp/project --frozen --all-groups --no-editable --no-emit-project --no-hashes --format requirements-txt --output-file /tmp/requirements.lock \
    && uv pip install --system --only-binary :all: --requirement /tmp/requirements.lock \
    && rm -rf /tmp/project /tmp/requirements.lock

USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"
