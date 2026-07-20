"""
The purpose of this module is to create centralized handling of different
protocol implementations and combine them with the dispatcher. Basically
all protocols existing and to be implemented should call this class by
passing a callback which handles the network part of the implementation.
"""

import ast
import asyncio
import json
from time import time_ns
import time

import can_reader
import itertools
from uuid import uuid4

from common.dispatcherv2 import Dispatcher
from common.config import BENCHMARK_CONFIG_PATH

RUN_ID = uuid4().hex
MESSAGE_ID = itertools.count(1)

config: dict = json.load(open(BENCHMARK_CONFIG_PATH))


# NOTE: Dispatcher was moved under shared client so that is it not required to be
# created in protocol implementations. This is better also because the shared client
# was in the end the module responsible for handling the usage of the dispatcher.
async def start_client(callback, output, qos, coap_context=None, setting="simulation"):

    window = config["client_settings"]["window"]
    workers = config["client_settings"]["workers"]
    queue_maxsize = config["client_settings"]["queue_maxsize"]

    dispatcher = Dispatcher(
        sender=callback,
        window=window,
        workers=workers,
        queue_maxsize=queue_maxsize,
        log_file=output,
        coap_context=coap_context,
    )

    try:
        await dispatcher.start()

        if setting == "simulation":
            async for msg, can_received_time in can_reader.async_read_json():
                await handle_message(
                    msg=msg,
                    can_received_time=can_received_time,
                    dispatcher=dispatcher,
                    qos=qos,
                )
        elif setting == "can":
            db, bus = can_reader.init_can_connection()
            while True:
                msg = await asyncio.to_thread(can_reader.read, db=db, bus=bus)
                if msg is None:
                    continue
                await handle_message(
                    msg=msg,
                    can_received_time=time_ns(),
                    dispatcher=dispatcher,
                    qos=qos,
                )
        else:
            print('Invalid setting. Use either "simulation" or "can"')

    except asyncio.CancelledError:
        print("Code execution was interrupted")

        time_stopped = time.time()

        try:
            stopped, shutdowned_messages = await dispatcher.shutdown()

            print(f"Writing remaining data to {output}")
            with open(output, "a") as log_file:

                for s in shutdowned_messages:
                    log_file.write(json.dumps(s) + "\n")

                log_file.write("\n----- Final Stats -----\n")
                log_file.write(
                    f"Cancelled at: {time_stopped}, shutdown completed at: {stopped}\n"
                )
                log_file.write(f"Took {stopped - time_stopped} seconds to shutdown\n")
                log_file.write(
                    "Average sent rate:" + str(round(dispatcher.can_read_rate)) + "\n"
                )
        except Exception as e:
            print(f"Error while writing output: {e}")
        raise


async def handle_message(msg, can_received_time, dispatcher, qos):
    name = msg.get("name", "Unknown")
    if name == "Unknown":
        return
    ts = msg.get("timestamp", time.time())
    raw = msg.get("data", {})

    # can_received_time = msg.get("can_received_time")
    can_received_time = time_ns()

    pre_decode_time = time_ns()

    # Parse JSON-only payloads into a dict of signals
    if isinstance(raw, dict):
        signals = raw
    else:
        try:
            signals = json.loads(raw)
        except Exception:
            try:
                signals = ast.literal_eval(raw)
            except Exception:
                signals = {}
    post_decode_time = time_ns()

    msg_id = f"{RUN_ID}_{next(MESSAGE_ID)}"

    await dispatcher.submit(
        msg_id=msg_id,
        message_name=name,
        signal_name=name,
        data=signals,
        timestamp=ts,
        unit=None,
        qos=qos,
        latency_metrics={
            "can_received_time": can_received_time,
            "pre_decode_time": pre_decode_time,
            "post_decode_time": post_decode_time,
        },
    )
