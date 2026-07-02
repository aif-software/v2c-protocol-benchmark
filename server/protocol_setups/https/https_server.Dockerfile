FROM python:3.12-alpine

WORKDIR /app
COPY https/https_server_v2.py https/https_requirements.txt /app/
COPY influx_writer.py /app/
RUN pip install --no-cache-dir -r https_requirements.txt

VOLUME [ "/app/certs" ]

CMD ["python", "https_server_v2.py"]