FROM python:3.12

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir uv
RUN useradd --create-home --uid 1000 appuser

COPY pyproject.toml uv.lock .
RUN uv sync --frozen --no-install-project --only-group api

COPY api/src/*.py .

RUN chown -R appuser:appuser /app
USER appuser

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
