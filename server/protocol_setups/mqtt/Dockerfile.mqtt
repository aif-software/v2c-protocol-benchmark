FROM python:3-alpine

WORKDIR /app
COPY mqtt/mqtt_logger.py mqtt/requirements.txt /app/
COPY influx_writer.py /app/
RUN pip install -r requirements.txt

VOLUME [ "/app/certs" ]

CMD ["python", "mqtt_logger.py"]