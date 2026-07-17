import json
from time import time_ns
import httpx
import ssl
import asyncio


class HTTPSender:
    def __init__(self, config, cert_path):
        self.config = config
        self.cert_path = cert_path
        context = ssl.create_default_context()
        context.load_verify_locations(cafile=self.cert_path)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED
        limits = httpx.Limits(
            max_connections=self.config["client_settings"]["workers"],
            max_keepalive_connections=20,
        )
        self.client = httpx.AsyncClient(
            verify=context, timeout=httpx.Timeout(10.0), http2=True, limits=limits
        )

    async def publish_https_structured(
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
        return_codes = []

        path = self.config.get("https", {}).get("path", "")
        if path and not path.startswith("/"):
            path = "/" + path
        url = f"https://{self.config['client_settings']['server_address']}:{self.config['client_settings']['server_port']}{path}"

        try:
            payload_bytes = json.dumps(payload).encode("utf-8")
        except TypeError as e:
            print(f"Error encoding payload to JSON: {e}")
            return

        try:
            return_codes.append("SUBMITTED")
            resp = await self.client.post(
                f"{url}/{message_name}/{signal_name}",
                data=payload_bytes,
                timeout=self.config["client_settings"].get("timeout"),
            )
            if resp.status_code == 200:
                return_codes.append("SUCCESSFUL")

        except (httpx.TransportError, asyncio.TimeoutError) as exc:
            print(f"HTTPS publish failed (transport/timeout): {exc}")
            return_codes.append("UNKNOWN")
            pass

        return return_codes

    async def shutdown(self):
        try:
            await self.client.aclose()
        except Exception as e:
            print(f"Problem while closing http2 client: {e}")
