FROM python:3-alpine

WORKDIR /app
COPY coap/coap_server.py coap/coap_requirements.txt /app/
COPY influx_writer.py /app/
RUN apk add --no-cache build-base autoconf automake libtool pkgconfig openssl-dev libffi-dev
RUN pip install --no-cache-dir -r coap_requirements.txt

VOLUME [ "/app/certs" ]

CMD ["python", "coap_server.py"]