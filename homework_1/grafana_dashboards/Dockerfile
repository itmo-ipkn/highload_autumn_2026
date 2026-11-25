FROM python:3.15.2-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt && pip cache purge

COPY ./ /app

CMD ["python3", "dashboard_api.py"]
