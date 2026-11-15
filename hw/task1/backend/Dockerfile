FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry==1.8.3

COPY pyproject.toml ./
RUN poetry config virtualenvs.create false \
    && poetry add requests \
    && poetry install --no-root --no-interaction --no-ansi

COPY app ./app
COPY gen_dashboard.py ./
COPY load_test.py ./

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]