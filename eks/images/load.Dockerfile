# Distributed load generator: the same producer.py, containerized for the fleet.
FROM python:3.12-slim
RUN pip install --no-cache-dir confluent-kafka==2.6.0
COPY common/producer.py /app/producer.py
WORKDIR /app
ENTRYPOINT ["python3", "producer.py"]
