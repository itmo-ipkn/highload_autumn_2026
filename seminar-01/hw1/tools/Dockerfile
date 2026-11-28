FROM python:3.13.5-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /tools
COPY --chmod=0644 requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10002 --create-home tools
COPY --chmod=0644 generate_dashboard.py load.py ./
USER 10002:10002
CMD ["python", "generate_dashboard.py"]
