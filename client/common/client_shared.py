import ast
import asyncio
import json
from time import time_ns
import time

import can_reader
import itertools
from uuid import uuid4

RUN_ID = uuid4().hex
MESSAGE_ID = itertools.count(1)


async def start_client(dispatcher, output, qos, mode="normal", setting="simulation"):
    try:
        log_file = open(output, "w")

        if mode == "normal":
            await dispatcher.start()
        elif mode == "coap":
            await dispatcher.start_coap()

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
                msg = can_reader.read(db=db, bus=bus)
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

        stopped, shutdowned_messages = await dispatcher.shutdown()

        print(f"Appending file {output}")
        log_file = open(output, "a")

        for s in shutdowned_messages:
            log_file.write(json.dumps(s) + "\n")
        log_file.flush()

        log_file.write("\n----- Final Stats -----\n")
        log_file.write(
            f"Cancelled at: {time_stopped}, shutdown completed at: {stopped}\n"
        )
        log_file.write(f"Took {stopped - time_stopped} seconds to shutdown\n")
        log_file.write(
            "Average sent rate:" + str(round(dispatcher.can_read_rate)) + "\n"
        )
        log_file.flush()

        raise


async def handle_message(msg, can_received_time, dispatcher, qos):
    name = msg.get("name", "Unknown")
    if name == "Unknown":
        return
    ts = msg.get("timestamp", time.time())
    raw = msg.get("data", {})
    # can_received_time = msg.get("can_received_time")
    can_received_time = time_ns()

    """
    name = msg.get("name", "Unknown")
    ts = msg.get("timestamp", time.time())
    raw = msg.get("data", {})
    """

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
