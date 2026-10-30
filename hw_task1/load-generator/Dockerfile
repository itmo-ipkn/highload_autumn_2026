FROM python:3.14-slim

WORKDIR /load-generator

RUN pip install --no-cache-dir httpx==0.28.1

COPY load.py .

CMD ["python", "-u", "load.py"]
