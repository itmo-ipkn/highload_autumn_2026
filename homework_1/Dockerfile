FROM python:3.15.2-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN mkdir -p /var/prometheus_client_data

RUN apt update \
    && apt install -y --no-install-recommends --no-install-suggests \
        curl \
    && apt clean \
    && apt autoremove \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt && pip cache purge

COPY /src /app

CMD ["python3", "-m", "fastapi", "run", "main.py"]
