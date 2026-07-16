import json
import asyncio
import argparse
from mqtt_publisher import MQTTAsyncSenderAioMQTT
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.client_shared import start_client
from common.config import BENCHMARK_CONFIG_PATH, PROJECT_ROOT
from common.dispatcherv2 import Dispatcher

config: dict = json.load(open(BENCHMARK_CONFIG_PATH))


async def main(qos, output, setting):
    window = config["client_settings"]["window"]
    workers = config["client_settings"]["workers"]
    queue_maxsize = config["client_settings"]["queue_maxsize"]

    cert_path = str(PROJECT_ROOT / config["client_settings"]["certs_path"])
    mqttSender = MQTTAsyncSenderAioMQTT(qos=qos, config=config, cert_path=cert_path)
    await mqttSender.connect()

    dispatcher = Dispatcher(
        mqttSender.publish_mqtt_structured,
        window=window,
        workers=workers,
        queue_maxsize=queue_maxsize,
        log_file=output,
    )

    try:
        await asyncio.wait_for(
            start_client(dispatcher, output, qos=qos, setting=setting),
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
