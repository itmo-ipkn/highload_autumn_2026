FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dashboard_generator.py load_test.py ./

# По умолчанию — генератор дашборда; для load-generator команда переопределяется в compose
CMD ["python", "dashboard_generator.py"]
