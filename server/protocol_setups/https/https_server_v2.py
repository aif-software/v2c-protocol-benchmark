import asyncio
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.exceptions import ProtocolError
from h2.events import (
    DataReceived,
    RequestReceived,
    StreamEnded,
)
import ssl
from logging import DEBUG
import logging
import os
from influx_writer import LoggerContext, store_message, INFLUX_OPTIONS
from influxdb_client import InfluxDBClient
from time import time_ns


_logger = logging.getLogger(__name__)

from dotenv import load_dotenv

load_dotenv()
try:
    _CERT_PATH = os.environ["CERT_PATH"]
    _LOGGING_LEVEL_STRING = os.environ["LOG_LEVEL"]
    _LOG_FILE_PATH = os.environ["LOG_FILE_PATH"]
    _TSDB_PROTOCOL = os.environ["TSDB_PROTOCOL"]
    _TSDB_URL = os.environ["TSDB_URL"]
    _TSDB_USERNAME = os.environ["TSDB_USERNAME"]
    _TSDB_PASSWORD = os.environ["TSDB_PASSWORD"]
    _TSDB_PORT = os.environ["TSDB_PORT"]
    _TSDB_ORG = os.environ["TSDB_ORG"]
    _TSDB_BUCKET = os.environ["TSDB_BUCKET"]
    _TSDB_ORG = os.environ["TSDB_ORG"]

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


def _fix_path(file_name: str) -> str:
    if _CERT_PATH.endswith("/"):
        return _CERT_PATH + file_name
    else:
        return _CERT_PATH + "/" + file_name


server_cert_path = _fix_path("server.crt")
key_path = _fix_path("server.key")
ca_cert_path = _fix_path("ca.crt")


class H2Server(asyncio.Protocol):
    def __init__(self):
        config = H2Configuration(client_side=False, header_encoding="utf-8")
        self.conn = H2Connection(config=config)
        self.transport = None
        self.topics = {}
        self.buffers = {}

        self.url = f"{_TSDB_PROTOCOL}://{_TSDB_URL}:{_TSDB_PORT}"
        self.influx_client = InfluxDBClient(
            url=self.url,
            username=_TSDB_USERNAME,
            password=_TSDB_PASSWORD,
            org=_TSDB_ORG,
            verify_ssl=False,
            timeout=30_000,
        )
        self.write_api = self.influx_client.write_api(write_options=INFLUX_OPTIONS)
        self.context = LoggerContext(bucket=_TSDB_BUCKET, log_file=_LOG_FILE_PATH)

    def connection_made(self, transport):
        self.transport = transport
        self.conn.initiate_connection()
        self.transport.write(self.conn.data_to_send())

    def data_received(self, data):
        try:
            https_receive_time = time_ns()

            events = self.conn.receive_data(data)
        except ProtocolError as e:
            self.transport.write(self.conn.data_to_send())
            self.transport.close()
        else:
            self.transport.write(self.conn.data_to_send())
            for event in events:
                if isinstance(event, RequestReceived):
                    headers = event.headers
                    path = None
                    for name, value in headers:
                        if name == ":path":
                            path = value
                            break
                    # topic = path.lstrip("/")
                    self.topics[event.stream_id] = (path or "").lstrip("/")
                    self.buffers[event.stream_id] = bytearray()
                elif isinstance(event, DataReceived):
                    if event.data:
                        self.conn.acknowledge_received_data(
                            len(event.data), event.stream_id
                        )
                        self.buffers.setdefault(event.stream_id, bytearray()).extend(
                            event.data
                        )

                        print(
                            f"Data received on stream {event.stream_id}: {event.data}"
                        )

                elif isinstance(event, StreamEnded):
                    topic = self.topics.pop(event.stream_id, "")
                    body = bytes(self.buffers.pop(event.stream_id, b""))

                    try:
                        store_message(
                            self.context,
                            topic,
                            body,
                            self.write_api,
                            https_receive_time,
                        )
                    except Exception:
                        _logger.exception("Error in processing message")
                        raise Exception(status_code=500, detail="Internal Server Error")

                    response_headers = [
                        (":status", "200"),
                        ("content-type", "text/plain"),
                    ]
                    self.conn.send_headers(
                        event.stream_id, response_headers, end_stream=False
                    )
                    self.conn.send_data(event.stream_id, b"OK", end_stream=True)
                    self.transport.write(self.conn.data_to_send())


async def main():
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certfile=server_cert_path, keyfile=key_path)
    ssl_context.set_alpn_protocols(["h2"])

    loop = asyncio.get_running_loop()
    server = await loop.create_server(H2Server, "0.0.0.0", 8000, ssl=ssl_context)
    print("Server started")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
