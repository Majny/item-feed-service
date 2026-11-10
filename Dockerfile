FROM python:3.13-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DATA_DIR=/data

WORKDIR /app
# Pinned dependencies first (requirements.lock, generated from pyproject.toml
# with `uv pip compile`), so a code change does not reinstall them.
COPY requirements.lock ./
RUN pip install -r requirements.lock
COPY pyproject.toml ./
COPY feedservice ./feedservice
RUN pip install --no-deps .

# Run as an unprivileged user; /data is the shared volume for feeds and images.
RUN useradd --create-home --uid 1000 app && mkdir -p /data && chown app:app /data

# Test image: the app plus pytest and the test suite (docker compose run tests).
FROM base AS test
COPY requirements-test.lock ./
RUN pip install -r requirements-test.lock
COPY tests ./tests
COPY examples ./examples
USER app

# Runtime image (the default target): no test dependencies.
FROM base AS app
USER app
EXPOSE 8000
CMD ["uvicorn", "feedservice.api:app", "--host", "0.0.0.0", "--port", "8000"]
