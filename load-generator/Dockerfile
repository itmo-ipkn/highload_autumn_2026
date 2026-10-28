FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY load_generator.py .

RUN addgroup -S app && adduser -S -G app app

USER app

CMD ["python", "-u", "/app/load_generator.py"]