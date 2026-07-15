import asyncio
import json
import os
from pathlib import Path
from time import time_ns
import logging
import aiocoap
from aiocoap import NON, CON
from aiocoap.numbers.constants import TransportTuning
import warnings
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=DeprecationWarning)

logging.basicConfig(level=logging.ERROR)
logging.getLogger("aiocoap").setLevel(logging.ERROR)
logging.getLogger("aiocoap.message-manager").setLevel(logging.DEBUG)


class COAPSender:
    def __init__(self, qos, config, coap_context):
        self.qos = qos
        self.config = config
        self.coap_context = coap_context
        self.broker = None
        self.port = None
        self.topic = None
        self.transport_tuning = TransportTuning()

    async def initialize(self):
        try:
            secrets_path = Path(__file__).resolve().parents[2] / "secrets.env"
            load_dotenv(secrets_path)

            self.topic = self.config["client_settings"]["topic"]
            self.broker = self.config["client_settings"]["server_address"]
            self.port = self.config["client_settings"]["server_port"]
            print(
                f"CoAP client context initialized for server {self.broker}:{self.port}"
            )

            print("Initializing transport tuning...")
            self.transport_tuning = TransportTuning()
            self.transport_tuning.MAX_LATENCY = 20.0
        except Exception as e:
            print(f"Failed to initialize CoAP context: {e}")

    async def post_coap_structured(
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
        """
        Publishes a structured message to a configured post topic in the new format.

        :param signal_name: The subtopic to append to the base topic
        :param data: The data value to publish
        :param timestamp: Timestamp of the data
        :param unit: Optional unit of measurement
        :param qos: Quality of Service level
        """

        latency_metrics["publish_time"] = time_ns()
        coap_full_uri = f"coap://{self.broker}:{self.port}/can"

        payload = {
            "msg_id": msg_id,
            "timestamp": timestamp,
            "data": data,
            "topic": f"{self.topic}/{message_name}/{signal_name}",
            "latency_metrics": latency_metrics,
        }
        return_codes = []

        try:
            payload_bytes = json.dumps(payload).encode("utf-8")

        except TypeError as e:
            print(f"Error encoding payload to JSON: {e}")
            return_codes.append("ERROR")
            return return_codes

        ack = self.qos == 1
        mtype = CON if ack else NON

        request = aiocoap.Message(
            code=aiocoap.POST,
            uri=coap_full_uri,
            payload=payload_bytes,
            mtype=mtype,
            transport_tuning=self.transport_tuning,
        )
        return_codes.append("SUBMITTED")

        if ack:
            try:
                response = await asyncio.wait_for(
                    self.coap_context.request(request).response,
                    timeout=self.config["client_settings"].get("timeout"),
                )
                if response is not None and response.code.is_successful():
                    return_codes.append("SUCCESSFUL")
                else:
                    print("Failed to get CoAP response")
                    return_codes.append("ERROR")

            except asyncio.TimeoutError as e:
                print("CoAP publish timed out:", e)
                return_codes.append("UNKNOWN")
            except Exception as e:
                return_codes.append("UNKNOWN")

        else:
            response = self.coap_context.request(request)  # fire-and-forget / NON
        return return_codes

    async def shutdown(self):
        await self.coap_context.shutdown()
