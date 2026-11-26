FROM python:3.13.7-slim
WORKDIR /app

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY netinventory ./netinventory
ENTRYPOINT ["python", "-m"]
