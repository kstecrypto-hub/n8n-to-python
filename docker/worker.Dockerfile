FROM python:3.12-slim

WORKDIR /app

COPY . /app
RUN pip install --no-cache-dir ".[dev]"

CMD ["python", "-m", "src.bee_ingestion.cli", "--help"]
