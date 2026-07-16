import json
import sys
import asyncio
import argparse
import warnings
from pathlib import Path

import aiocoap
from coap_publisher import COAPSender

warnings.filterwarnings("ignore", category=DeprecationWarning)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.client_shared import start_client
from common.config import BENCHMARK_CONFIG_PATH

config: dict = json.load(open(BENCHMARK_CONFIG_PATH))


async def main(qos: int, output: str, setting: str):
    transports = ["udp6"]
    coap_context = await aiocoap.Context.create_client_context(transports=transports)

    coapSender = COAPSender(qos=qos, config=config, coap_context=coap_context)
    await coapSender.initialize()

    try:
        await asyncio.wait_for(
            start_client(
                callback=coapSender.post_coap_structured,
                output=output,
                qos=qos,
                setting=setting,
                coap_context=coap_context,
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
    asyncio.run(main(qos=args.qos, output=args.output, setting=args.setting))
