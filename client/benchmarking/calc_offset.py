import json
from pathlib import Path

import httpx
import asyncio
import time
from time import perf_counter_ns, time_ns

MAX_RTT_MS = 100.0


async def get_server_offset(client, url, current_offset_ms):
    res = await get_clock_offset(client, url)
    if not res:
        return None, current_offset_ms

    res["uncertainty_ms"] = (
        res["rtt_ms"] / 2.0
    )  # calculate uncertainty, which is the the maximum possible error margin.

    # only update if its below the limit, else ignore
    if res["rtt_ms"] <= MAX_RTT_MS:
        current_offset_ms = res["offset_ms"]

    return res, current_offset_ms


async def get_server_offset_probes(
    client, url, current_offset_ms, probes=10, probe_spacing=0.02
):
    best = None
    for _ in range(probes):
        res = await get_clock_offset(client, url)
        if res:
            res["uncertainty_ms"] = res["rtt_ms"] / 2.0

            if (best is None) or (res["rtt_ms"] < best["rtt_ms"]):
                best = res

        if probe_spacing:
            await asyncio.sleep(probe_spacing)

    if not best:
        return None, current_offset_ms

    if best["rtt_ms"] <= MAX_RTT_MS:
        current_offset_ms = best["offset_ms"]

    return best, current_offset_ms


async def get_clock_offset(client, url):
    client_start = time_ns()
    try:
        res = await client.get(url)
        client_receive = time_ns()

        server_receive = res.json().get("t_server")

        rtt_ns = client_receive - client_start

        offset_ns = server_receive - (client_start + (rtt_ns // 2))

        return {
            "client_start": client_start,
            "client_receive": client_receive,
            "offset_ms": offset_ns / 1_000_000,
            "rtt_ms": rtt_ns / 1_000_000,
        }
    except Exception as e:
        print(f"Request failed: {e}")
        return None


async def calculate_offset(
    url="",
    output_file="output.csv",
    interval=1,
    stop_event=None,
):
    results = []

    with open(output_file, "a") as f:
        f.write(
            "start_timestamp,stop_timestamp,offset_ms,rtt_ms,uncertainty_ms,used_offset_ms\n"
        )

        # f.write("offset_ms,rtt_ms,avg_offset_ms,avg_rtt_ms\n")

    current_offset_ms = None

    async with httpx.AsyncClient() as client:
        while not stop_event.is_set():
            res, current_offset_ms = await get_server_offset_probes(
                client, url, current_offset_ms
            )

            if res and current_offset_ms is not None:
                with open(output_file, "a") as f:
                    f.write(
                        f'{res["client_start"]},{res["client_receive"]},{res["offset_ms"]:.6f},{res["rtt_ms"]:.6f},{res["uncertainty_ms"]:.6f},{current_offset_ms:.6f}\n'
                    )
            await asyncio.sleep(interval)
    f.close()


def run_offset_calc(**kwargs):
    asyncio.run(calculate_offset(**kwargs))


if __name__ == "__main__":

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    config_path = Path(__file__).parent / "benchmark_config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    clock_offset_address = config["client_settings"]["clock_offset_address"]
    stop_event = asyncio.Event()
    run_offset_calc(
        url=f"{clock_offset_address.rstrip('/')}/sync",
        interval=0.5,
        output_file="test_offset.csv",
        stop_event=stop_event,
    )
