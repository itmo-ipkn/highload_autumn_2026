FROM python:3.11.9-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && useradd --uid 10001 --create-home app
COPY --chmod=0644 app/*.py verify/logic.py ./
USER 10001:10001
EXPOSE 9404
CMD ["python", "producer.py"]
