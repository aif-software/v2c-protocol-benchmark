import json
from time import time_ns
import httpx
import ssl
import asyncio
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import HeadersReceived

class HTTP3Sender(QuicConnectionProtocol):
    def __init__(self, *args, config, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self.h3connection = H3Connection(self._quic, enable_webtransport=False)
        self.sent = 0
        self.received = 0
        self.futures = {}
        self.topic = config["client_settings"]["topic"]

    async def publish_can_data_structured(
        self,
        msg_id,
        message_name: str,
        signal_name: str,
        data: dict,
        timestamp: float,
        unit: str = None,
        qos: int = 0,
        latency_metrics=None,
    ):
        return await self.publish_https3_structured(
            msg_id, message_name, signal_name, data, timestamp, unit, qos, latency_metrics
        )

    async def publish_https3_structured(
        self,
        msg_id,
        message_name: str,
        subtopic: str,
        data: dict,
        timestamp: float,
        unit: str = None,
        qos: int = 0,
        latency_metrics=None,
    ):
        if latency_metrics is None:
            latency_metrics = {}

        latency_metrics["publish_time"] = time_ns()
        payload = {
            "msg_id": msg_id,
            "timestamp": timestamp,
            "data": data,
            "latency_metrics": latency_metrics,
        }
        return_codes = []

        try:
            payload_bytes = json.dumps(payload).encode("utf-8")
        except TypeError as e:
            print(f"Error encoding payload to JSON: {e}")
            return_codes.append("ERROR")
            return return_codes

        path = "/" + self.topic + "/" + subtopic
        path_bytes = path.encode()
        sid = self._quic.get_next_available_stream_id()
        authority = self.config["client_settings"]["server_address"].encode()

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.futures[sid] = future

        return_codes.append("SUBMITTED")
        self.h3connection.send_headers(
            sid,
            [
                (b":method", b"POST"),
                (b":scheme", b"https"),
                (b":authority", authority),
                (b":path", path_bytes),
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload_bytes)).encode("ascii")),
            ],
            end_stream=False,
        )

        self.h3connection.send_data(
            stream_id=sid,
            data=payload_bytes,
            end_stream=True,
        )

        self.transmit()

        # use future to wait for response
        try:
            await asyncio.wait_for(
                future, timeout=self.config["client_settings"].get("timeout")
            )  # if higher than 10, likely message not received by server
            return_codes.append("SUCCESSFUL")
        except asyncio.TimeoutError:
            self.futures.pop(sid, None)
            return_codes.append("UNKNOWN")

        return return_codes

    def quic_event_received(self, event):
        for event in self.h3connection.handle_event(event):
            if isinstance(event, HeadersReceived):
                status = None
                for name, value in event.headers:
                    if name == b":status":
                        status = int(value)
                        break
                if status == 200:
                    future = self.futures.pop(event.stream_id, None)
                    if future is not None and not future.done():
                        future.set_result(True)
