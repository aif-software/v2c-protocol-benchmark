import asyncio
import can
import cantools
import json
import logging
import time
import cantools.database
import sys
from pathlib import Path
import sys

from time import time_ns

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.config import BENCHMARK_CONFIG_PATH

config: dict = json.load(open(BENCHMARK_CONFIG_PATH))


####


def init_can_connection():
    database = config["can"]["database"]
    interface = config["can"]["interface"]
    channel = config["can"]["channel"]
    delay = config["can"]["delay"]
    filter_set = set(config["can"]["filter_set"])
    db = cantools.database.load_file(database)
    bus = can.Bus(channel=channel, interface=interface)

    return db, bus


def create_message_entry(
    message: can.Message,
    db: cantools.database.can.Database,
    filter_set: set,
) -> dict:
    try:
        db_message = db.get_message_by_frame_id(message.arbitration_id)
        decoded = db.decode_message(message.arbitration_id, message.data)
        logger.debug(f"{db_message.name}: {decoded}")

        try:
            return {
                "name": db_message.name,
                "timestamp": message.timestamp,
                "id": message.arbitration_id,
                "data": json.dumps(decoded),
                "raw": "0x" + message.data.hex(),
            }
        except TypeError:
            print(f"TypeError: {decoded} for message {message}")
            return {
                "name": db_message.name,
                "timestamp": message.timestamp,
                "id": message.arbitration_id,
                "data": f"{decoded}",
                "raw": "0x" + message.data.hex(),
            }
    except KeyError:
        return {
            "name": "Unknown",
            "timestamp": message.timestamp,
            "id": message.arbitration_id,
            "data": message.data.hex(),
            "raw": "0x" + message.data.hex(),
        }


# For reading real can connection
def read(db, bus, delay=0):
    time.sleep(delay)
    message = bus.recv()

    return create_message_entry(message, db, set())


async def async_read_json():
    channel = config["simulation"]["channel"]
    try:
        with open(channel, "r") as file:
            messages = json.loads(file.read())

        previous_ts = None
        loop = asyncio.get_running_loop()
        base_monotonic = loop.time()
        base_ts = None

        for message in messages:
            if message.get("name") == "Unknown":
                continue

            msg_ts = message.get("timestamp")
            if msg_ts is None:
                yield message, time_ns()
                continue

            try:
                msg_ts = float(msg_ts)
            except Exception:
                yield message, time_ns()
                continue

            if previous_ts is None:
                # first msg: establish schedule origin
                previous_ts = msg_ts
                base_ts = msg_ts
                yield message, time_ns()
                continue

            # schedule by absolute time to avoid drift
            target = base_monotonic + (msg_ts - base_ts)
            delay = max(0.0, target - loop.time())

            print(f"Sleeping for {delay:.6f} seconds to simulate original timing")
            await asyncio.sleep(delay)

            yield message, time_ns()
            previous_ts = msg_ts
        print("All messages handled. Simulation ending...")

    except FileNotFoundError:
        print(f"Error: {channel} not found.")
