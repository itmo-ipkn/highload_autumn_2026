FROM python:3.12.11-slim-bookworm
WORKDIR /lab
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-u", "-m", "app.cli"]
