FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update -y \
    && apt-get install -y wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "uv==0.13.6"

COPY requirements.txt .
RUN uv venv .venv --python 3.14 \
    && uv pip install -r requirements.txt \
    && mkdir -m 777 /app/.uv-cache

ENV UV_CACHE_DIR=/app/.uv-cache

COPY app/ .

# COPY tests/ ./tests

USER 65534:65534

ENTRYPOINT ["uv", "run", "main.py"]

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=5 \
    CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8080/health"]
