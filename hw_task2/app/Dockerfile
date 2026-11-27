FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
# exec-form CMD in compose => python is PID 1 and receives SIGTERM directly
EXPOSE 8000
