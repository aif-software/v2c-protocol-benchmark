import json
import socket
import ssl
from time import time_ns
import asyncio
import aiomqtt


class MQTTAsyncSenderAioMQTT:
    def __init__(self, qos, config, cert_path):
        self.config = config
        self.qos = qos
        self.cert_path = cert_path
        self.topic = "toyota"
        self.client = None
        self.context = ssl.create_default_context()
        self.context.load_verify_locations(cafile=cert_path)
        self.context.check_hostname = False

    async def connect(self):
        self.client = aiomqtt.Client(
            hostname=self.config["client_settings"]["server_address"],
            port=self.config["client_settings"]["server_port"],
            tls_context=self.context,
            keepalive=60,
            max_inflight_messages=int(self.config["client_settings"]["window"]),
            max_concurrent_outgoing_calls=int(self.config["client_settings"]["window"]),
            transport="tcp",
            socket_options=[
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            ],
        )

        await self.client.__aenter__()

    async def disconnect(self):
        if self.client:
            await self.client.__aexit__(None, None, None)
            self.client = None

    async def publish_mqtt_structured(
        self,
        msg_id,
        message_name: str,
        signal_name: str,
        data: dict,
        timestamp: float,
        unit: str = "",
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

        topic = f"{self.topic}/{message_name}/{signal_name}"
        payload_bytes = json.dumps(payload).encode("utf-8")

        return_codes = []
        return_codes.append("SUBMITTED")

        try:
            # qos 0: waits till publish completed
            # qos 1: waits till PUBACK is completed
            # qos 3: waits till PUBCOMP completed
            await asyncio.wait_for(
                self.client.publish(topic, payload_bytes, qos=qos),
                timeout=self.config["client_settings"]["timeout"],
            )
            return_codes.append("SUCCESSFUL")

        except asyncio.TimeoutError:
            return_codes.append("UNKNOWN")
        except Exception:
            return_codes.append("ERROR")

        return return_codes
