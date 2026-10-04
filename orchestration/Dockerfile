FROM docker:29.8.1-cli@sha256:018edbc908e08fcc9dbf029c812c34251e9b4719e6f71ca0e5eae2a987d014ca AS docker-cli

FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

ARG AIRFLOW_VERSION=3.3.2
ARG AIRFLOW_CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY pyproject.toml uv.lock dbt-requirements.txt /tmp/orchestration/

RUN uv export --project /tmp/orchestration --frozen --no-dev --no-emit-project --no-hashes \
      -o /tmp/requirements.txt \
    && uv pip install --no-cache -r /tmp/requirements.txt --constraint "${AIRFLOW_CONSTRAINTS}"

# dbt gets its own venv so its pins never fight Airflow's constraints; Cosmos calls /opt/dbt-venv/bin/dbt
USER root
RUN mkdir /opt/dbt-venv && chown airflow:0 /opt/dbt-venv
USER airflow
RUN uv venv --python /usr/python/bin/python3.12 /opt/dbt-venv \
    && VIRTUAL_ENV=/opt/dbt-venv uv pip install --no-cache -r /tmp/orchestration/dbt-requirements.txt
