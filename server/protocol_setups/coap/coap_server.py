from logging import DEBUG
import logging
import os
import asyncio
from influx_writer import LoggerContext, store_message, INFLUX_OPTIONS
import aiocoap.resource as resource
from influxdb_client import InfluxDBClient
import aiocoap
from time import time_ns
from aiocoap import credentials, error
from aiocoap.numbers.constants import TransportTuning

import yaml

_logger = logging.getLogger(__name__)
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aiocoap").setLevel(DEBUG)

from dotenv import load_dotenv

load_dotenv()
try:
    _LOGGING_LEVEL_STRING = os.environ["LOG_LEVEL"]
    _LOG_FILE_PATH = os.environ["LOG_FILE_PATH"]
    _TSDB_PROTOCOL = os.environ["TSDB_PROTOCOL"]
    _TSDB_URL = os.environ["TSDB_URL"]
    _TSDB_USERNAME = os.environ["TSDB_USERNAME"]
    _TSDB_PASSWORD = os.environ["TSDB_PASSWORD"]
    _TSDB_PORT = os.environ["TSDB_PORT"]
    _TSDB_ORG = os.environ["TSDB_ORG"]
    _TSDB_ORG = os.environ["TSDB_ORG"]
    _QOS_LEVEL = os.environ["QOS_LEVEL"]
    _TSDB_BUCKET = f"coap_bucket{int(_QOS_LEVEL)}"  # map qos level to bucket 1, 2, 3
    print("Using bucket:", _TSDB_BUCKET)


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

context = LoggerContext(bucket=_TSDB_BUCKET, log_file=_LOG_FILE_PATH)


class CANLogger(resource.Resource):
    def __init__(self, client, write_api):
        super().__init__()
        self.client = client
        self.write_api = write_api
        self.received = 0
        self.transport_tuning = TransportTuning()
        self.transport_tuning.MAX_LATENCY = 20.0

        print(("Exchange lifetime set to:", self.transport_tuning.EXCHANGE_LIFETIME))

    async def render_post(self, request):
        coap_receive_time = time_ns()
        payload = bytes(request.payload)

        """
        asyncio.create_task(
            asyncio.to_thread(
                store_message, context, None, payload, self.write_api, coap_receive_time
            )
        )"""

        store_message(context, None, payload, self.write_api, coap_receive_time)

        return aiocoap.Message(
            code=aiocoap.Code.CREATED,
            payload=b"OK",
            transport_tuning=self.transport_tuning,
        )


async def main():
    root = resource.Site()

    root.add_resource(
        [".well-known", "core"], resource.WKCResource(root.get_resources_as_linkheader)
    )

    influx_client = InfluxDBClient(
        url=url,
        username=_TSDB_USERNAME,
        password=_TSDB_PASSWORD,
        org=_TSDB_ORG,
        verify_ssl=False,
        timeout=30_000,
    )

    write_api = influx_client.write_api(write_options=INFLUX_OPTIONS)

    can_logger = CANLogger(influx_client, write_api)
    root.add_resource(["can"], can_logger)
    root.add_resource([], can_logger)

    allowed_endpoints = [
        "PCM_CRUISE",
        "GAS_PEDAL",
        "BRAKE",
        "SPEED",
        "PCM_CRUISE_2",
        "SEATS_DOORS",
        "UI_SETTING",
        "KINEMATICS",
        "PCM_CRUISE_SM",
        "VIN_PART_1",
        "VIN_PART_2",
        "VIN_PART_3",
    ]

    for endpoint in allowed_endpoints:
        root.add_resource([endpoint], can_logger)
        print(f"Registered endpoint: /{endpoint}")

    psk_id = os.getenv("COAPS_PSK_IDENTITY").strip()
    psk_key = os.getenv("COAPS_PSK_KEY").strip()

    if psk_id and psk_key:
        server_credentials = credentials.CredentialsMap()

        server_credentials.load_from_dict(
            {
                "coaps://*": {
                    "dtls": {
                        "psk": psk_key.encode(),
                        "client-identity": psk_id.encode(),
                    }
                }
            }
        )

        bind_host = os.environ.get("COAPS_BIND_ADDR") or os.environ.get("POD_IP")
        transports = ["tinydtls_server", "udp6"]

        await aiocoap.Context.create_server_context(
            root,
            bind=(bind_host, 5683),
            transports=["udp6"],
        )

        await aiocoap.Context.create_server_context(
            root,
            bind=(bind_host, None),
            server_credentials=server_credentials,
            transports=transports,
        )
        print("Started server context:", bind_host, 5684)

    else:
        print("Setting up DTLS failed.")

    # await aiocoap.Context.create_server_context(root)

    # Run forever

    await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    url = f"{_TSDB_PROTOCOL}://{_TSDB_URL}:{_TSDB_PORT}"
    print(f"Connecting to {url}")

    print("Connected to influxdb")

    asyncio.run(main())
