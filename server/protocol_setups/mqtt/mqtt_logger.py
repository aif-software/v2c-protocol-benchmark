import logging
import os
import time
import urllib3
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient
from time import time_ns
from influx_writer import LoggerContext, store_message, INFLUX_OPTIONS


urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)  # TEMPORARY DISABLE WARNINGS

_logger = logging.getLogger(__name__)
from dotenv import load_dotenv

global counter
counter = 0

load_dotenv()

try:
    _BROKER_URL = os.environ["BROKER_IP"]
    _BROKER_PORT = os.environ["BROKER_PORT"]
    _CERT_PATH = os.environ["CERT_PATH"]
    _CLIENT_ID = os.environ["CLIENT_ID"]
    _CLIENT_USERNAME = os.environ["CLIENT_USERNAME"]
    _CLIENT_PASSWORD = os.environ["CLIENT_PASSWORD"]
    _LOG_FILE_PATH = os.environ["LOG_FILE_PATH"]
    _LOGGING_LEVEL_STRING = os.environ["LOG_LEVEL"]
    _TSDB_URL = os.environ["TSDB_URL"]
    _TSDB_PORT = os.environ["TSDB_PORT"]
    _TSDB_PROTOCOL = os.environ["TSDB_PROTOCOL"]
    _TSDB_USERNAME = os.environ["TSDB_USERNAME"]
    _TSDB_PASSWORD = os.environ["TSDB_PASSWORD"]
    _TSDB_ORG = os.environ["TSDB_ORG"]
    QOS_LEVEL = int(os.environ["QOS_LEVEL"])

    if _LOGGING_LEVEL_STRING == "DEBUG":
        _LOGGING_LEVEL = logging.DEBUG
    elif _LOGGING_LEVEL_STRING == "INFO":
        _LOGGING_LEVEL = logging.INFO
    elif _LOGGING_LEVEL_STRING == "WARNING":
        _LOGGING_LEVEL = logging.WARNING
    else:
        raise ValueError(f"Unexpected logging level provided: {_LOGGING_LEVEL_STRING}")

except KeyError:
    raise RuntimeError("Missing one or more required environmental variables.")

_TSDB_BUCKET = f"mqtt_bucket{int(QOS_LEVEL)}"  # map qos level to bucket 1, 2, 3
print("Using bucket:", _TSDB_BUCKET)

is_connected = False

context = LoggerContext(bucket=_TSDB_BUCKET, log_file=_LOG_FILE_PATH)


def _fix_path(file_name: str) -> str:
    if _CERT_PATH.endswith("/"):
        return _CERT_PATH + file_name
    else:
        return _CERT_PATH + "/" + file_name


def _main():
    logging.basicConfig(level=_LOGGING_LEVEL)

    print("Running main")

    url = f"{_TSDB_PROTOCOL}://{_TSDB_URL}:{_TSDB_PORT}"
    print(f"Connecting to {url}")
    influx_client = InfluxDBClient(
        url=url,
        username=_TSDB_USERNAME,
        password=_TSDB_PASSWORD,
        org=_TSDB_ORG,
        verify_ssl=False,
        timeout=30_000,
    )

    write_api = influx_client.write_api(write_options=INFLUX_OPTIONS)

    def on_message_wrapper(client, userdata, message):
        logger_receive_time = time_ns()
        store_message(
            context, message.topic, message.payload, write_api, logger_receive_time
        )

    print("Connected to influxdb")
    client = mqtt.Client()

    ca_cert_path = _fix_path("ca.crt")

    client.tls_set(ca_cert_path)
    client.tls_insecure_set(True)
    client.username_pw_set(_CLIENT_USERNAME, password=_CLIENT_PASSWORD)

    client._on_connect = _on_connect

    client.on_message = (
        on_message_wrapper  # wrap callback to fit influx_writer store_message signature
    )
    # client._on_message = _on_message
    print(f"Connecting to broker {_BROKER_URL}:{_BROKER_PORT}")
    client.connect(_BROKER_URL, int(_BROKER_PORT))
    client.subscribe("#", qos=QOS_LEVEL)
    client.loop_start()

    print("Connected to mqtt")

    while not is_connected:
        print("Connecting...")
        time.sleep(5)
    while True:
        time.sleep(1)


def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        global is_connected
        is_connected = True
        print("Connected to broker")
    else:
        is_connected = False
        print("Failed to connected with result " + str(rc))


def _on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection.")


def _on_message(context: LoggerContext, write_api, client, userdata, message):
    global counter
    logger_receive_time = time_ns()
    print(counter)
    counter += 1
    try:
        print(f"Received message [{message.topic}] - {message.payload[:100]}")
        store_message(
            context, message.topic, message.payload, write_api, logger_receive_time
        )
    except Exception as e:
        _logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    _main()
