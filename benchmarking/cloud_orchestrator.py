import time
from kubernetes import config
from openshift.dynamic import DynamicClient
from kubernetes.dynamic.exceptions import ConflictError, ApiException, NotFoundError
import yaml
import json
from pathlib import Path


class Orchestrator:
    def __init__(self, namespace):
        config_path = Path(__file__).parent / "benchmark_config.json"
        with open(config_path, "r", encoding="utf-8") as f:
            self.settings_config = json.load(f)

        self.k8s_client = config.new_client_from_config()
        self.dyn_client = DynamicClient(self.k8s_client)
        self.namespace = namespace

        self.protocols = {
            "coap": {"server": ["coap-server"], "service": "coap-service"},
            "mqtt": {
                "server": ["mosquitto-broker", "mqttlogger"],
                "service": "mosquitto-service",
            },
            "amqp": {
                "server": ["rabbitmq-broker", "amqp-logger"],
                "service": "rabbitmq-service",
            },
            "http2": {"server": ["https-server"], "service": "https-service"},
            "http3": {"server": ["https3-server"], "service": "https3-service"},
        }

    def find_linked_pods(self, deployment_name):
        pods_linked = []
        v1_pods = self.dyn_client.resources.get(api_version="v1", kind="Pod")
        pods = v1_pods.get(namespace=self.namespace)

        for pod in pods.items:
            pod_name = pod.metadata.name
            if deployment_name in pod_name:
                pods_linked.append(pod)

        return pods_linked

    def find_linked_services(self, deployment_name):
        services_linked = []
        services = self.dyn_client.resources.get(api_version="v1", kind="Service")
        for service in services.get(namespace=self.namespace).items:
            service_name = service.metadata.name
            if deployment_name in service_name:
                services_linked.append(service)

        return services_linked

    def delete_deployment(self, deployment_name):
        try:
            pods_linked = self.find_linked_pods(deployment_name)
            services_linked = self.find_linked_services(deployment_name)
            services = self.dyn_client.resources.get(api_version="v1", kind="Service")
            deployments = self.dyn_client.resources.get(
                api_version="apps/v1", kind="Deployment"
            )
            deployments.delete(
                name=deployment_name,
                namespace=self.namespace,
                body={"propagationPolicy": "Foreground", "gracePeriodSeconds": 1},
            )

            print(f"Deleted Deployment: {deployment_name}")
        except ApiException as e:
            if e.status == 404:
                print(f"Deployment {deployment_name} not found, assuming deleted...")
                return True
            else:
                print(f"Error deleting deployment: {e}")
                return False
        v1_pods = self.dyn_client.resources.get(api_version="v1", kind="Pod")
        for pod in pods_linked:
            while True:
                try:
                    current_pod = v1_pods.get(
                        name=pod.metadata.name, namespace=self.namespace
                    )
                    print("pod status: ", current_pod.status.phase)
                    if (
                        current_pod.status.phase == "Terminated"
                        or current_pod.status.phase == "Succeeded"
                        or current_pod.status.phase == "Failed"
                    ):
                        break
                    time.sleep(8)
                except ApiException as e:
                    if e.status == 404:
                        print(f"Pod {pod.metadata.name} not found, assuming deleted...")
                        break
                    else:
                        print(f"Error fetching pod status: {e}")
                        time.sleep(5)

        print(f"All pods for deployment {deployment_name} have been terminated.")

        for service in services_linked:
            services.delete(
                name=service.metadata.name,
                namespace=self.namespace,
                body={"propagationPolicy": "Foreground", "gracePeriodSeconds": 1},
            )
            print(f"Deleted Service: {service.metadata.name}")
        return True

    def delete_service(self, service_name):
        services = self.dyn_client.resources.get(api_version="v1", kind="Service")

        try:
            services.delete(
                name=service_name,
                namespace=self.namespace,
                body={"propagationPolicy": "Foreground", "gracePeriodSeconds": 1},
            )
            print(f"Deleted Service: {service_name}")
        except NotFoundError as e:
            print(f"Service {service_name} not found, assuming deleted...")
        return True

    def deploy_protocol_setup(self, protocol: str, qos):
        deployment_manifest_paths = self.settings_config["yaml_paths"].get(protocol)
        PROJECT_ROOT = Path(__file__).resolve().parents[1]
        for deployment_manifest_path in deployment_manifest_paths:
            deployment_manifest_path = f"{PROJECT_ROOT}/{deployment_manifest_path}"

            print(f"Reading {deployment_manifest_path} for deployment...")
            with open(deployment_manifest_path, "r") as file:
                deployment_manifest = yaml.safe_load_all(file)
                for manifest in deployment_manifest:
                    if manifest is None:
                        continue
                    if manifest["kind"] == "Deployment":
                        for container in manifest["spec"]["template"]["spec"][
                            "containers"
                        ]:
                            if (
                                container["name"]
                                == self.protocols[protocol]["server"][-1]
                            ):
                                for env_var in container["env"]:
                                    if env_var["name"] == "QOS_LEVEL":
                                        print("Setting QOS env var to:", str(qos))
                                        env_var["value"] = str(qos)
                                    if env_var["name"] == "TSDB_BUCKET":
                                        print(
                                            "Setting bucket env var to:",
                                            f"{protocol}_bucket{qos}",
                                        )
                                        env_var["value"] = f"{protocol}_bucket{qos}"

                    try:
                        resource_api = self.dyn_client.resources.get(
                            api_version=manifest["apiVersion"], kind=manifest["kind"]
                        )
                        print(
                            f"Creating {manifest['kind']} named {manifest['metadata']['name']}..."
                        )
                        resource_api.create(body=manifest, namespace=self.namespace)
                        time.sleep(1)

                        if manifest["kind"] != "Deployment":
                            continue

                        while True:
                            deployments = self.dyn_client.resources.get(
                                api_version="apps/v1", kind="Deployment"
                            )
                            try:
                                current_deployment = deployments.get(
                                    name=manifest["metadata"]["name"],
                                    namespace=self.namespace,
                                )

                                val = getattr(
                                    current_deployment.status, "availableReplicas", 0
                                )

                                available = val if val is not None else 0
                                print("Available:", available)

                                if available >= 1:
                                    print(
                                        f"Deployment {manifest['metadata']['name']} is ready."
                                    )
                                    break

                                time.sleep(8)
                            except ApiException as e:
                                if e.status == 404:
                                    print(
                                        f"Deployment {manifest['metadata']['name']} not found, waiting..."
                                    )
                                    time.sleep(5)
                                else:
                                    print(f"Error fetching deployment status: {e}")
                                    time.sleep(5)

                    except ConflictError as e:
                        print(f"Already exists: {manifest['kind']}")
                        continue

        print("Deployment created successfully.")
        return True

    def delete_protocol_setup(self, protocol: str):
        current_protocol = self.protocols.get(protocol)
        if not current_protocol:
            print(f"Unknown protocol: {protocol}")
            return

        server = current_protocol.get("server")
        service = current_protocol.get("service")

        if isinstance(server, list):
            for s in server:
                self.delete_deployment(s)
        else:
            self.delete_deployment(server)

        if service:
            self.delete_service(service)

    def get_endpoint_pod_name(self, protocol: str):
        current_protocol = self.protocols.get(protocol)
        if not current_protocol:
            print(f"Unknown protocol: {protocol}")
            return None
        print(current_protocol)

        server = current_protocol.get("server")
        print(server)

        if isinstance(server, list):
            server = server[0]

        pods = self.find_linked_pods(server)
        print(pods)
        if pods:
            return pods[0].metadata.name
        return None


if __name__ == "__main__":
    pass
