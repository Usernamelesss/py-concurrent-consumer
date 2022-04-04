FROM python:3.7-slim

RUN pip install confluent-kafka
RUN pip install retry

WORKDIR /app
COPY ./ /app

ENTRYPOINT ["python", "main.py"]
