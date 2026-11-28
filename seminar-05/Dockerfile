FROM python:3.12.12-slim
WORKDIR /lab
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ app/
COPY proto/ proto/
COPY tools/ tools/
RUN python tools/generate.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "app/server.py", "backend"]
