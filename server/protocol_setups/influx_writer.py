import json
import datetime
import logging
from time import time_ns
from typing import Tuple
from influxdb_client import Point, WritePrecision, WriteOptions
import re
import itertools

_logger = logging.getLogger(__name__)
global counter
counter = 1

FORCE_STRING_FIELDS = {"LOW_SPEED_LOCKOUT"}

INFLUX_OPTIONS = WriteOptions(
    batch_size=500,
    flush_interval=10_000,
    jitter_interval=2_000,
    retry_interval=5_000,
    max_retries=5,
    max_retry_delay=30_000,
    exponential_base=2,
)


class LoggerContext:
    def __init__(self, bucket: str, log_file: str = None):
        self.bucket = bucket
        self.log_file = log_file
        self.counter = itertools.count()


def _format_message(payload: bytes) -> Tuple[float, float, dict]:
    if not payload:
        _logger.error("Received empty payload")
        return None, None, {}

    try:
        payload_str = payload.decode("utf-8")  # Decode bytes to string
        payload_dict = json.loads(payload_str)

        timestamp = payload_dict.get("timestamp", None)
        data_value = payload_dict.get("data", None)
        latency_metrics = payload_dict.get("latency_metrics", {})
        topic = payload_dict.get("topic", None)
        msg_id = payload_dict.get("msg_id", None)

        return timestamp, data_value, latency_metrics, topic, msg_id
    except json.JSONDecodeError as e:
        _logger.error(f"Failed to decode JSON payload: {e} - Payload: {payload_str}")
        return None, None, {}, None


def store_message(
    context: LoggerContext, topic, payload, write_api, logger_receive_time
):
    try:
        pre_processing_time = time_ns()
        timestamp, message, latency_metrics, payload_topic, msg_id = _format_message(
            payload
        )
        post_processing_time = time_ns()
        """
        with open(context.log_file, "a") as f:
            f.write(f"{topic} {message}\n")
        """
        if payload_topic:
            topic = payload_topic

        topic_parts = re.split("[/.]", topic)

        current_time_utc = datetime.datetime.now(datetime.timezone.utc)

        influx_write_start_time = time_ns()
        unique_counter = next(context.counter)

        measurement = "/".join(topic_parts[0:2])

        shared_fields = {
            "logger_receive_time_ms": logger_receive_time / 1_000_000,
            "logger_pre_processing_time_ms": pre_processing_time / 1_000_000,
            "logger_post_processing_time_ms": post_processing_time / 1_000_000,
            "logger_influx_write_start_time_ms": influx_write_start_time / 1_000_000,
            "counter": unique_counter,
            "msg_id": msg_id
        }

        value_field_name = "/".join(topic_parts[2:]) if topic_parts[2:] else "value"

        for key, value in (latency_metrics or {}).items():
            if value is not None:
                shared_fields[f"client_{key}_ms"] = value / 1_000_000

        p = Point(measurement).time(current_time_utc, WritePrecision.NS)

        for shared_key, shared_value in shared_fields.items():
            p.field(shared_key, shared_value)

        if isinstance(message, dict):
            for key, value in message.items():
                field_name = f"{value_field_name}/{key}"

                if key in FORCE_STRING_FIELDS:
                    value = str(value)

                if value is not None:
                    p.field(field_name, value)

        else:  # not dict = store as single value field
            if message is not None:
                p.field(value_field_name, message)
        if unique_counter % 500 == 0:
            print(f"Received {unique_counter} messages")

    except Exception as e:
        print(f"Error processing message: {e}")
        _logger.error(f"Error processing message: {e}")
        return

    try:
        write_api.write(bucket=context.bucket, record=p)
    except Exception as e:
        print(f"InfluxDB write error: {e}")
        _logger.error(f"InfluxDB write error: {e}")
