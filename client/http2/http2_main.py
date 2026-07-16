import argparse
import json
from http2_publisher import HTTPSender
import asyncio
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.client_shared import start_client
from common.config import BENCHMARK_CONFIG_PATH, PROJECT_ROOT

config: dict = json.load(open(BENCHMARK_CONFIG_PATH))


async def main(output, setting):
    cert_path = str(PROJECT_ROOT / config["client_settings"]["certs_path"])

    httpSender = HTTPSender(config, cert_path)

    try:
        await asyncio.wait_for(
            start_client(
                callback=httpSender.publish_https_structured,
                output=output,
                qos=0,
                setting=setting,
            ),
            timeout=config["client_settings"]["duration"],
        )

    except asyncio.TimeoutError:
        print("Timeout reached, stopping client...")
    finally:
        await httpSender.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--qos", type=int, default=0, help="Quality of Service level")
    parser.add_argument("--output", type=str, default="client_data.txt")
    parser.add_argument("--setting", type=str, default="simulation")
    args = parser.parse_args()
    asyncio.run(main(output=args.output, setting=args.setting))
