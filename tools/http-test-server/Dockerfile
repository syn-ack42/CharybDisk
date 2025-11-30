FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py config.yaml ./

# Data mount points
VOLUME ["/data"]

ENV HTTP_TEST_CONFIG=/app/config.yaml
ENV HTTP_TEST_HOST=0.0.0.0
ENV HTTP_TEST_PORT=8080

EXPOSE 8080

CMD ["python", "server.py"]
