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
from common.dispatcher import Dispatcher
from common.config import BENCHMARK_CONFIG_PATH

config: dict = json.load(open(BENCHMARK_CONFIG_PATH))


async def main(qos: int, output: str, setting: str):
    window = int(config["client_settings"]["window"])
    workers = int(config["client_settings"]["workers"])
    queue_maxsize = int(config["client_settings"]["queue_maxsize"])

    transports = ["tinydtls"]
    coap_context = await aiocoap.Context.create_client_context(transports=transports)

    coapSender = COAPSender(qos=qos, config=config, coap_context=coap_context)
    await coapSender.connect()

    dispatcher = Dispatcher(
        coapSender.post_coap_structured,
        window=window,
        workers=workers,
        queue_maxsize=queue_maxsize,
        log_file=output,
        coap_context=coap_context,
    )

    try:
        await asyncio.wait_for(
            start_client(dispatcher, output, qos=qos, mode="coap", setting=setting),
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
