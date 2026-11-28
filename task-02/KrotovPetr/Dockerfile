FROM python:3.12-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt
RUN useradd --uid 10001 --create-home app
COPY src ./src
COPY tests ./tests
USER app
CMD ["python", "-m", "src.verify"]
