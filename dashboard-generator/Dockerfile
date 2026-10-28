FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY generate_dashboard.py .

RUN addgroup -S app && adduser -S -G app app
USER app

CMD ["python", "/app/generate_dashboard.py"]