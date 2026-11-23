FROM python:3.14-slim

WORKDIR /generator

RUN pip install --no-cache-dir httpx==0.28.1

COPY generate.py .

CMD ["python", "generate.py"]