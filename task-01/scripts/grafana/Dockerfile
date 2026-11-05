FROM python:3.12

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir uv
RUN useradd --create-home --uid 1000 appuser

COPY pyproject.toml uv.lock .
RUN uv sync --frozen --no-install-project --only-group grafana_dashboard

COPY scripts/grafana/src/*.py .
COPY scripts/grafana/dashboards/*.json ./dashboards/

RUN chown -R appuser:appuser /app
USER appuser

CMD ["uv", "run", "python", "main.py"]
