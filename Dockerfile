FROM python:3.7-slim

RUN pip install kafka-python==2.0.2
RUN pip install retry==0.9.2

WORKDIR /app
COPY ./ /app

ENTRYPOINT ["python", "start_consumer.py"]
