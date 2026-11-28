FROM python:3.13-slim

RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir uv

COPY pyproject.toml /app/pyproject.toml
WORKDIR /app

RUN /venv/bin/uv sync --no-dev

COPY src /app/src
COPY verify.py /app/verify.py

# python -m hw.producer
ENV PYTHONPATH=/app/src
ENV PATH="/app/.venv/bin:${PATH}"

CMD ["python", "-m", "hw.producer"]
