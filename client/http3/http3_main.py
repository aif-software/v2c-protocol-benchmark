import argparse
import json
import ssl
import asyncio
from aioquic.quic.configuration import QuicConfiguration
from aioquic.asyncio.client import connect
from http3_publisher import HTTP3Sender

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.client_shared import start_client
from common.config import BENCHMARK_CONFIG_PATH, PROJECT_ROOT

config: dict = json.load(open(BENCHMARK_CONFIG_PATH))


async def main(output, setting):
    broker: str = config["client_settings"]["server_address"]
    port: int = config["client_settings"]["server_port"]
    cert_path = str(PROJECT_ROOT / config["client_settings"]["certs_path"])

    quic_config = QuicConfiguration(
        is_client=True, alpn_protocols=["h3"], verify_mode=ssl.CERT_NONE
    )
    quic_config.load_verify_locations(cafile=cert_path)

    async with connect(
        broker,
        port,
        configuration=quic_config,
        create_protocol=lambda *args, **kwargs: HTTP3Sender(
            *args,
            config=config,
            **kwargs,
        ),
    ) as http3sender:
        print("Connected to HTTP/3 server.")

        try:
            await asyncio.wait_for(
                start_client(
                    http3sender.publish_can_data_structured,
                    output=output,
                    qos=0,
                    setting=setting,
                ),
                timeout=config["client_settings"]["duration"],
            )

        except asyncio.TimeoutError:
            print("Timeout reached, stopping client...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--qos", type=int, default=0, help="Quality of Service level")
    parser.add_argument("--output", type=str, default="client_data.txt")
    parser.add_argument("--setting", type=str, default="simulation")
    args = parser.parse_args()
    asyncio.run(main(output=args.output, setting=args.setting))
