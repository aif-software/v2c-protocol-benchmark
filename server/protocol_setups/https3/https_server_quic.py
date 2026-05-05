import asyncio
import aioquic.asyncio
from aioquic.quic.configuration import QuicConfiguration
import aioquic
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived
from influxdb_client import InfluxDBClient, WriteOptions

import logging
import os
import sys
from time import time_ns
import aioquic.asyncio
import aioquic
from influx_writer import LoggerContext, store_message, INFLUX_OPTIONS

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
)
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


_QOS_LEVEL = 2
is_connected = False

server_cert_path = _fix_path("server.crt")
key_path = _fix_path("server.key")
ca_cert_path = _fix_path("ca.crt")


class QuicServer(aioquic.asyncio.QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.h3 = H3Connection(self._quic, enable_webtransport=False)
        self.buffers = {}
        self.stream_id = None
        self.topics = {}

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
        self.logger_context = LoggerContext(
            bucket=_TSDB_BUCKET, log_file=_LOG_FILE_PATH
        )

    def quic_event_received(self, event):
        for e in self.h3.handle_event(event):
            if isinstance(e, HeadersReceived):
                self.buffers.setdefault(e.stream_id, bytearray())
                for k, v in e.headers:
                    if k == b":path":
                        topic = v.decode().lstrip("/")
                        self.topics[e.stream_id] = topic
                        break
            elif isinstance(e, DataReceived):
                buf = self.buffers.setdefault(e.stream_id, bytearray())
                if e.data:
                    buf.extend(e.data)

                if e.stream_ended:
                    https_receive_time = time_ns()

                    body = bytes(self.buffers.pop(e.stream_id, b""))
                    current_topic = self.topics.pop(e.stream_id, "")

                    store_message(
                        self.logger_context,
                        current_topic,
                        body,
                        self.write_api,
                        https_receive_time,
                    )

                    self.h3.send_headers(
                        stream_id=e.stream_id,
                        headers=[
                            (b":status", b"200"),
                            (b"content-type", b"text/plain"),
                            (b"content-length", b"2"),
                        ],
                        end_stream=False,
                    )
                    self.h3.send_data(
                        stream_id=e.stream_id,
                        data=b"ok",
                        end_stream=True,
                    )
                    self.transmit()


async def main():
    configuration = QuicConfiguration(
        is_client=False,
        alpn_protocols=["h3"],
    )

    configuration.load_cert_chain(server_cert_path, key_path)

    loop = asyncio.get_event_loop()

    server = await aioquic.asyncio.serve(
        host="0.0.0.0",
        port=4433,
        configuration=configuration,
        create_protocol=QuicServer,
    )
    print("HTTP/3 server is running on")
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
