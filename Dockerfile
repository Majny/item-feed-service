FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml ./
COPY feedservice ./feedservice
RUN pip install ".[test]"
COPY tests ./tests
COPY examples ./examples

# Run as an unprivileged user; /data is the shared volume for feeds and images.
RUN useradd --create-home --uid 1000 app && mkdir -p /data && chown app:app /data
USER app
ENV DATA_DIR=/data

EXPOSE 8000
CMD ["uvicorn", "feedservice.api:app", "--host", "0.0.0.0", "--port", "8000"]
