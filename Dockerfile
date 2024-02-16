# Builder Image

FROM python:3.9-bookworm AS builder

RUN pip install --no-cache-dir poetry==1.7.0

ENV POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_CACHE_DIR=/tmp/pypoetry

WORKDIR /app
COPY pyproject.toml poetry.lock dbt_project.yml packages.yml ./
RUN poetry install --no-root && poetry run dbt deps && rm -rf $POETRY_CACHE_DIR

# Runner Image

FROM python:3.9-slim-bookworm AS runner

ENV VIRTUAL_ENV=/app/.venv \
    DBT_PACKAGES=/app/dbt_packages \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY --from=builder ${DBT_PACKAGES} ${DBT_PACKAGES}

COPY . .

CMD ["dbt-rpc", "serve", "--profiles-dir", ".", "--host", "0.0.0.0", "--port", "8580"]
