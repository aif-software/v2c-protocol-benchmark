import asyncio
import json
import logging
import os
import ssl
import aio_pika

import urllib3

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from time import time_ns
from influx_writer import LoggerContext, store_message, INFLUX_OPTIONS

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)  # TEMPORARY DISABLE WARNINGS

_logger = logging.getLogger(__name__)
from dotenv import load_dotenv

load_dotenv()

try:
    _BROKER_URL = os.environ["BROKER_IP"]
    _BROKER_PORT = int(os.environ["BROKER_PORT"])
    _LOG_FILE_PATH = os.environ["LOG_FILE_PATH"]
    _LOGGING_LEVEL_STRING = os.environ["LOG_LEVEL"]
    _TSDB_URL = os.environ["TSDB_URL"]
    _TSDB_PORT = os.environ["TSDB_PORT"]
    _TSDB_PROTOCOL = os.environ["TSDB_PROTOCOL"]
    _TSDB_USERNAME = os.environ["TSDB_USERNAME"]
    _TSDB_PASSWORD = os.environ["TSDB_PASSWORD"]
    _TSDB_ORG = os.environ["TSDB_ORG"]
    _CERTS_PATH = os.environ["CERTS_PATH"]
    _USER = os.environ["RABBITMQ_DEFAULT_USER"]
    _PASSWORD = os.environ["RABBITMQ_DEFAULT_PASS"]
    _QOS_LEVEL = os.environ["QOS_LEVEL"]
    qos = int(_QOS_LEVEL)

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

is_connected = False

_TSDB_BUCKET = f"amqp_bucket{qos}"  # map qos level to bucket 1, 2, 3
print("Using bucket:", _TSDB_BUCKET)

context = LoggerContext(bucket=_TSDB_BUCKET, log_file=_LOG_FILE_PATH)


async def _amqp_on_message(
    msg: aio_pika.IncomingMessage, influx_client, write_api, exchange
):
    AMQP_receive_time = time_ns()

    try:
        store_message(context, msg.routing_key, msg.body, write_api, AMQP_receive_time)
        if qos == 1:
            await msg.ack()
    except Exception as e:
        raise e


async def _main():
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

    print("Connected to influxdb")
    write_api = influx_client.write_api(write_options=INFLUX_OPTIONS)
    print(f"Connecting to broker {_BROKER_URL}:{_BROKER_PORT}")
    try:
        context = ssl.create_default_context()
        context.load_verify_locations(cafile=_CERTS_PATH)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED

        connection = await aio_pika.connect_robust(
            host=_BROKER_URL,
            port=_BROKER_PORT,
            login=_USER,
            password=_PASSWORD,
            ssl=True,
            ssl_context=context,
        )
        _on_connect(None, None, None, 0)
    except Exception as e:
        print(f"Failed to connect to broker: {e}")
        _on_connect(None, None, None, 1)
        raise

    queue_name = "test_queue"
    routing_key = "toyota.#"

    channel = await connection.channel(
        publisher_confirms=True
    )  # channel inside connection, communication happens inside the channel

    exchange = await channel.get_exchange(
        "amq.topic"
    )  # virtual router proxy, managing messages and which queues to send to
    queue = await channel.declare_queue(
        queue_name, durable=True
    )  # buffer storing messages until the receivers receive them.

    await queue.bind(exchange, routing_key)  # bind queue to exchange

    no_ack = qos == 0  # map no_ack to qos level

    await queue.consume(
        lambda m: _amqp_on_message(m, influx_client, write_api, exchange),
        no_ack=no_ack,
    )

    print("Connected to AMQP")

    while not is_connected:
        print("Connecting...")
        await asyncio.sleep(1)
    await asyncio.Future()


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


if __name__ == "__main__":
    asyncio.run(_main())
