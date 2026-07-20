import json
import statistics
import influxdb_client
import csv
from bisect import bisect_right
import numpy as np
from pathlib import Path
import math, statistics
from dotenv import load_dotenv
import os

LATENCY_CALCULATION_CONFIG = [
    (
        "client_decode_time_ms",
        "client_post_decode_time_ms",
        "client_pre_decode_time_ms",
    ),
    (
        "client_publish_prep_time_ms",
        "client_publish_time_ms",
        "client_post_decode_time_ms",
    ),
    (
        "total_client_processing_time_ms",
        "client_publish_time_ms",
        "client_can_received_time_ms",
    ),
    (
        "network_latency_candump_to_logger_ms",
        "logger_receive_time_ms",
        "client_publish_time_ms",
    ),
    (
        "logger_message_format_time_ms",
        "logger_post_processing_time_ms",
        "logger_pre_processing_time_ms",
    ),
    (
        "logger_overall_processing_time_ms",
        "logger_influx_write_start_time_ms",
        "logger_receive_time_ms",
    ),
    (
        "overall_can_receive_to_influx_time_ms",
        "_time_ms",
        "client_can_received_time_ms",
    ),
]

FIELDS_TO_APPLY_OFFSET = {
    "logger_overall_processing_time_ms",
    "logger_influx_write_start_time_ms",
    "logger_receive_time_ms",
    "_time_ms",
}

"""
Small helper class to parse & manage clock offsync values
"""


class OffsetIndex:
    def __init__(self, start_ms, used_offset_ms):
        self.start_ms = start_ms
        self.used_offset_ms = used_offset_ms

    @classmethod
    def parse_offset_csv(cls, path):
        rows = []

        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                start_ms = float(row["start_timestamp"]) / 1_000_000.0

                offset_ms = float(
                    row["used_offset_ms"]
                    if row.get("used_offset_ms")
                    else row["offset_ms"]
                )

                rows.append((start_ms, offset_ms))

        rows.sort(key=lambda r: r[0])

        start_ms = [r[0] for r in rows]
        used_off = [r[1] for r in rows]

        return cls(start_ms, used_off)

    def latest_offset_ms(self, timestamp_ms):
        i = (
            bisect_right(self.start_ms, timestamp_ms) - 1
        )  # get index of latest used_offset that matches the given timestamp

        if i < 0:
            return None

        return self.used_offset_ms[i]


def write_raw_csv(output_file, result, append=False, write_header=True):
    ORDERED_KEEP_FIELDS = [
        "result",
        "table",
        "_start",
        "_stop",
        "_time",
        "_measurement",
        "counter",
        "timestamp",
        "client_can_received_time_ms",
        "client_post_decode_time_ms",
        "client_pre_decode_time_ms",
        "client_publish_time_ms",
        "logger_influx_write_start_time_ms",
        "logger_post_processing_time_ms",
        "logger_pre_processing_time_ms",
        "logger_receive_time_ms",
        "_time_ms",
        "overall_can_receive_to_influx_time_ms",
        "msg_id",
    ]
    KEEP_SET = set(ORDERED_KEEP_FIELDS)

    # get all rows first
    rows = []
    for table in result:
        for record in table.records:
            values = dict(record.values)

            if "value" not in values and "_value" in values:
                values["value"] = values["_value"]

            t = values.get("_time")
            if t is not None and hasattr(t, "timestamp"):
                values["_time_ms"] = int(t.timestamp() * 1000)

            rows.append(values)

    # header
    delta_headers = [name for (name, _, _) in LATENCY_CALCULATION_CONFIG]
    header = ORDERED_KEEP_FIELDS + ["signal_name", "signal_value"] + delta_headers

    mode = "a" if append else "w"
    with open(output_file, mode, newline="") as outfile:
        writer = csv.writer(outfile)
        if write_header:
            writer.writerow(header)

        for values in rows:
            base_row = [values.get(k) for k in ORDERED_KEEP_FIELDS]

            deltas = []
            for _, first_field, second_field in LATENCY_CALCULATION_CONFIG:
                first_val = values.get(first_field)
                second_val = values.get(second_field)
                if first_val is None or second_val is None:
                    deltas.append(None)
                else:
                    try:
                        deltas.append(first_val - second_val)
                    except TypeError:
                        deltas.append(None)

            writer.writerow(base_row + [None, None] + deltas)


def summarize_local_pub(path):
    values = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            value = obj.get("local_pub")
            if isinstance(value, (int, float)):
                values.append(float(value))

    with open(path, "a", encoding="utf-8") as f:
        f.write("local_publishing_time\n")
        f.write(f"Samples: {len(values)}\n")

        avg = statistics.mean(values)
        sd = statistics.stdev(values) if len(values) > 1 else 0.0
        mn = min(values)
        mx = max(values)
        med = statistics.median(values)
        p95 = np.percentile(values, 95)  # 95th percentile

        f.write(f"Average: {avg:.6f} ms\n")
        f.write(f"Std Dev: {sd:.6f} ms\n")
        f.write(f"Minimum: {mn:.6f} ms\n")
        f.write(f"Maximum: {mx:.6f} ms\n")
        f.write(f"Median : {med:.6f} ms\n")
        f.write(f"95thpercentile: {p95:.6f} ms\n")


def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def combine_deltas(combined, result, offset_index: OffsetIndex = None):
    for table in result:
        for record in table.records:
            values = dict(record.values)  # copy
            t = values.get("_time")
            if t is not None and hasattr(t, "timestamp"):
                values["_time_ms"] = int(t.timestamp() * 1000)

            if offset_index is not None:
                server_receive_ms = _to_float(values.get("client_publish_time_ms"))
                if server_receive_ms is not None:
                    offset_ms = offset_index.latest_offset_ms(server_receive_ms)
                    if offset_ms is not None:
                        for field in FIELDS_TO_APPLY_OFFSET:
                            value = _to_float(values.get(field))
                            if value is not None:
                                values[field] = value - offset_ms

            for delta_name, f1, f2 in LATENCY_CALCULATION_CONFIG:
                v1 = _to_float(values.get(f1))
                v2 = _to_float(values.get(f2))
                if v1 is not None and v2 is not None:
                    dv = v1 - v2
                    if not (isinstance(dv, float) and math.isnan(dv)):
                        combined[delta_name].append(dv)


def write_summary_from_combined(path, combined):
    def format_ms(x, decimals=6):
        return f"{x:.{decimals}f}"

    with open(path, "a") as summary:
        for name, data in combined.items():
            n = len(data)
            unit = "" if name in {"can_read_rate", "ack_rate"} else " ms"
            summary.write(f"{name}:\n")
            summary.write(f"Samples: {n}\n")
            if n == 0:
                summary.write("No data\n\n")
                continue
            avg = statistics.mean(data)
            med = statistics.median(data)
            mn = min(data)
            mx = max(data)
            sd = statistics.stdev(data) if n > 1 else 0.0
            p95 = np.percentile(data, 95)
            p90 = np.percentile(data, 90)
            p99 = np.percentile(data, 99)

            summary.write(f"Average: {format_ms(avg)}{unit}\n")
            summary.write(f"Std Dev: {format_ms(sd)}{unit}\n")
            summary.write(f"Minimum: {format_ms(mn)}{unit}\n")
            summary.write(f"Maximum: {format_ms(mx)}{unit}\n")
            summary.write(f"Median : {format_ms(med)}{unit}\n")
            summary.write(f"90thpercentile: {p90:.6f}{unit}\n")
            summary.write(f"95thpercentile: {p95:.6f}{unit}\n")
            summary.write(f"99thpercentile: {p99:.6f}{unit}\n\n")


def calculate_latency_chunked(
    bucket,
    org,
    start_iso,
    stop_iso,
    raw_output_file,
    summary_output_file,
    window_seconds=300,
    max_rows_per_chunk=None,
    sleep_between_seconds=0.0,
    offset_csv=None,
):
    import datetime as dt
    import time as _time

    def _parse_rfc3339(s: str):
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s)

    start_dt = _parse_rfc3339(start_iso)
    stop_dt = _parse_rfc3339(stop_iso)

    client = initialise_client()
    query_api = client.query_api()

    header_written = False
    cur = start_dt

    combined = {name: [] for (name, _, _) in LATENCY_CALCULATION_CONFIG}

    while cur < stop_dt:
        nxt = min(cur + dt.timedelta(seconds=window_seconds), stop_dt)
        while True:
            try:
                print(
                    f"Attempting to query from: {cur.isoformat()} to {nxt.isoformat()}"
                )

                query = f"""from(bucket: "{bucket}")
                |> range(start: {cur.isoformat()}, stop: {nxt.isoformat()})
                |> pivot(rowKey: ["_time", "_measurement"], columnKey: ["_field"], valueColumn: "_value")
                """

                if max_rows_per_chunk is not None:
                    query += f' |> sort(columns: ["_time","_measurement"]) |> limit(n: {max_rows_per_chunk})'
                print("query:", query)
                result = query_api.query(org=org, query=query)

                # CSV append
                write_raw_csv(
                    raw_output_file,
                    result,
                    append=header_written,
                    write_header=not header_written,
                )
                header_written = True

                # combine all delta values for summary
                offset_index = OffsetIndex.parse_offset_csv(offset_csv)
                combine_deltas(combined, result, offset_index)

                if sleep_between_seconds:
                    _time.sleep(sleep_between_seconds)

                cur = nxt
                break
            except Exception as e:
                print("Failed to query influxdb:", e, "retrying..")
                _time.sleep(5)

    print("Finished querying influxdb. ", raw_output_file)

    write_summary_from_combined(summary_output_file, combined)
    print("Finished writing summary: ", summary_output_file)


def initialise_client():

    config_path = Path(__file__).resolve().parents[1] / "config.env"
    load_dotenv(config_path)

    secrets_path = Path(__file__).resolve().parents[1] / "secrets.env"
    load_dotenv(secrets_path)

    influx_address = f"{os.getenv('TSDB_PROTOCOL')}://{os.getenv('TSDB_URL')}:{os.getenv('TSDB_PORT')}"

    client = influxdb_client.InfluxDBClient(
        url=influx_address,
        token=os.getenv("TSDB_TOKEN"),
        org=os.getenv("TSDB_ORG"),
        verify_ssl=False,
        timeout=50_000,
    )

    return client


def write_latency_details_to_csv(
    path, rows, target_delta="network_latency_candump_to_logger_ms"
):
    fieldnames = [
        "result",
        "table",
        "_start",
        "_stop",
        "_time",
        "_measurement",
        "counter",
        "timestamp",
        "signal_name",
        "signal_value",
        "delta_name",
        "v1_field",
        "v2_field",
        "v1",
        "v2",
        "dv",
    ]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            if row.get("delta_name") == target_delta:
                writer.writerow(row)


def calculate_latency_details_from_csv(
    csv_file,
    details_output_file,
    offset_csv=None,
):
    rows_out = []

    offset_index = OffsetIndex.parse_offset_csv(offset_csv) if offset_csv else None

    with open(csv_file, "r", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            values = dict(row)

            if offset_index is not None:
                server_receive_ms = _to_float(values.get("client_publish_time_ms"))
                if server_receive_ms is not None:
                    offset_ms = offset_index.latest_offset_ms(server_receive_ms)
                    if offset_ms is not None:
                        for field in FIELDS_TO_APPLY_OFFSET:
                            value = _to_float(values.get(field))
                            if value is not None:
                                values[field] = value - offset_ms

            for delta_name, f1, f2 in LATENCY_CALCULATION_CONFIG:
                v1 = _to_float(values.get(f1))
                v2 = _to_float(values.get(f2))

                if v1 is not None and v2 is not None:
                    dv = v1 - v2
                    if not (isinstance(dv, float) and math.isnan(dv)):
                        rows_out.append(
                            {
                                "result": row.get("result"),
                                "table": row.get("table"),
                                "_start": row.get("_start"),
                                "_stop": row.get("_stop"),
                                "_time": row.get("_time"),
                                "_measurement": row.get("_measurement"),
                                "counter": row.get("counter"),
                                "timestamp": row.get("timestamp"),
                                "signal_name": row.get("signal_name"),
                                "signal_value": row.get("signal_value"),
                                "delta_name": delta_name,
                                "v1_field": f1,
                                "v2_field": f2,
                                "v1": v1,
                                "v2": v2,
                                "dv": dv,
                            }
                        )

    write_latency_details_to_csv(details_output_file, rows_out)


def calculate_latency_from_csv(
    csv_file,
    summary_output_file,
    offset_csv=None,
):
    combined = {name: [] for (name, _, _) in LATENCY_CALCULATION_CONFIG}
    combined["can_read_rate"] = []
    combined["ack_rate"] = []

    offset_index = OffsetIndex.parse_offset_csv(offset_csv) if offset_csv else None

    with open(csv_file, "r", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            values = dict(row)

            if offset_index is not None:
                reference_time_ms = _to_float(values.get("client_publish_time_ms"))
                if reference_time_ms is not None:
                    offset_ms = offset_index.latest_offset_ms(reference_time_ms)
                    if offset_ms is not None:
                        for field in FIELDS_TO_APPLY_OFFSET:
                            value = _to_float(values.get(field))
                            if value is not None:
                                values[field] = value - offset_ms

            for delta_name, f1, f2 in LATENCY_CALCULATION_CONFIG:
                v1 = _to_float(values.get(f1))
                v2 = _to_float(values.get(f2))
                if v1 is not None and v2 is not None:
                    dv = v1 - v2
                    if not (isinstance(dv, float) and math.isnan(dv)):
                        combined[delta_name].append(dv)

    summary_dir = Path(summary_output_file).parent
    raw_summary_files = list(summary_dir.glob("*_summary.txt"))
    if len(raw_summary_files) == 1:
        with open(raw_summary_files[0], "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    values = json.loads(line)
                except json.JSONDecodeError:
                    continue

                can_read_rate = _to_float(values.get("can_read_rate"))
                ack_rate = _to_float(values.get("ack_rate"))

                if can_read_rate is not None:
                    combined["can_read_rate"].append(can_read_rate)
                if ack_rate is not None:
                    combined["ack_rate"].append(ack_rate)

    write_summary_from_combined(summary_output_file, combined)


def process_parent_directory(parent_dir: str):
    parent = Path(parent_dir)

    if not parent.is_dir():
        raise ValueError(f"{parent_dir} is not a valid directory")

    for child in parent.iterdir():
        if not child.is_dir():
            continue

        csv_files = list(child.glob("*.csv"))
        if len(csv_files) == 0:
            print(f"No CSV file found in {child}")
            continue
        if len(csv_files) > 1:
            print(f"Multiple CSV files in {child}, skipping")
            continue

        csv_file = csv_files[0]

        offset_files = list(child.glob("*offset.txt"))
        if len(offset_files) == 0:
            print(f"No offset file found in {child}")
            offset_file = None
        elif len(offset_files) > 1:
            print(f"Multiple offset files in {child}, using first")
            offset_file = offset_files[0]
        else:
            offset_file = offset_files[0]

        summary_file = child / "summary_stats.txt"
        details_file = f"{child}_latencies.csv"
        print(details_file)

        calculate_latency_from_csv(
            csv_file=str(csv_file),
            summary_output_file=str(summary_file),
            offset_csv=str(offset_file) if offset_file else None,
        )
