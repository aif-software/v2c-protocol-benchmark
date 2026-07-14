import json
import os
from time import time_ns
import aio_pika
import asyncio
import ssl
from dotenv import load_dotenv
from pathlib import Path


class AMQPSender:
    def __init__(self, qos, config, cert_path):
        self.qos = qos
        self.config = config
        self.cert_path = cert_path
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        broker: str = self.config["client_settings"]["server_address"]
        port: int = self.config["client_settings"]["server_port"]

        secrets_path = Path(__file__).resolve().parents[2] / "secrets.env"
        load_dotenv(secrets_path)
        if self.connection is None:
            print(f"Connecting to address: {broker}:{port}")
            try:
                self.context = ssl.create_default_context()
                self.context.load_verify_locations(cafile=self.cert_path)
                self.context.check_hostname = False
                self.context.verify_mode = ssl.CERT_REQUIRED

                self.connection = await aio_pika.connect_robust(
                    host=broker,
                    port=port,
                    loop=asyncio.get_event_loop(),
                    login=os.getenv("RABBITMQ_DEFAULT_USER"),
                    password=os.getenv("RABBITMQ_DEFAULT_PASS"),
                    ssl=True,
                    ssl_context=self.context,
                )
                print(f"AMQP client context initialized for server {broker}:{port}")

                queue_name = "test_queue"

                routing_key = "test_queue"

                publisher_confirms = self.qos == 1  # qos 1 = true, 0 = false

                self.channel = await self.connection.channel(
                    publisher_confirms=publisher_confirms
                )

                # channel inside connection, communication happens inside the channel
                self.exchange = await self.channel.get_exchange(
                    "amq.topic"
                )  # virtual router proxy, managing messages and which queues to send to
                self.queue = await self.channel.declare_queue(
                    queue_name, auto_delete=False, durable=True
                )  # buffer storing messages until the receivers receive them.

                await self.queue.bind(
                    self.exchange, routing_key
                )  # bind queue to exchange

                return True

            except Exception as e:
                print(f"Failed to initialize amqp context: {e}")
                return False

    async def publish_amqp_structured(
        self,
        msg_id,
        message_name: str,
        signal_name: str,
        data: dict,
        timestamp: float,
        unit: str = None,
        qos: int = 0,
        latency_metrics={},
    ):
        """
        Publishes a structured message to a configured amqp topic in the new format.

        :param signal_name: The subtopic to append to the base topic
        :param data: The data value to publish
        :param timestamp: Timestamp of the data
        :param unit: Optional unit of measurement
        :param qos: Quality of Service level
        """
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

        routing_key = (
            self.config["client_settings"]["topic"]
            + "."
            + message_name
            + "."
            + signal_name
        )
        print(
            "Publishing: ",
            payload_bytes.decode("utf-8"),
            " to routing key: ",
            routing_key,
        )
        return_codes.append("SUBMITTED")

        if self.qos == 0:
            await self.exchange.publish(  # awaits only for publish, not for delivery, since publisher_confirms is False
                aio_pika.Message(
                    body=payload_bytes, delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=routing_key,
                mandatory=True,
            )
        elif self.qos == 1:
            try:
                await asyncio.wait_for(
                    self.exchange.publish(  # fully awaits for delivery, since publisher_confirms is True
                        aio_pika.Message(
                            body=payload_bytes,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=routing_key,
                        mandatory=True,
                    ),
                    timeout=self.config["client_settings"].get("timeout"),
                )
                return_codes.append("SUCCESSFUL")
            except asyncio.TimeoutError:
                return_codes.append("UNKNOWN")

        return return_codes
