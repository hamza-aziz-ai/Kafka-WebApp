FROM python:3.14-slim-trixie@sha256:cd89c30eda26ee911a4611b18680086e0ea42e6f5f8cbe653d07fa7958c9cec8
# Links the image to this repository when it is pushed to ghcr.io
LABEL org.opencontainers.image.source=https://github.com/hamza-aziz-ai/Kafka-WebApp

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN useradd --system app && chown -R app /app
USER app

EXPOSE 9090
HEALTHCHECK --interval=30s --timeout=5s CMD ["python3", "-c", "import socket; socket.create_connection(('localhost', 9090), 1)"]
CMD ["python3", "app.py"]
