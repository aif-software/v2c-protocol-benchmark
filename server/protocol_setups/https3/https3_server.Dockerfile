FROM python:3.12-alpine

WORKDIR /app
COPY https3/https_server_quic.py https3/https_requirements.txt /app/
COPY influx_writer.py /app/
RUN pip install --no-cache-dir -r https_requirements.txt

VOLUME [ "/app/certs" ]

EXPOSE 4433/udp

CMD ["python", "https_server_quic.py"]