## Structure

This repository contains the source code for the benchmark. It is structured as follows:

- **server** – contains all server-side deployment files  
- **client** – contains client files as well as the automated benchmark scripts  

### Setup Rahti

Using the project requires authorization to the Rahti servers with the use of OpenShift.

- Install the `oc` CLI tool:  
  https://docs.okd.io/4.18/cli_reference/openshift_cli/getting-started-cli.html  

- After `oc` has been installed, authorize to the Rahti servers:
  - Go to Rahti → Click on your username → Open settings  
  - Copy *Login Command*  
  - Paste the `oc login` command into the CLI  

You should now be authorized and can proceed with setting up the project.  
**Note:** this authorization must be repeated every 24 hours.

For setting up non-TCP connections in Rahti, follow:  
https://docs.csc.fi/cloud/rahti/networking/#using-loadbalancer-service-type-with-dedicated-ips  

### Benchmark setup

This project relies on a number of configuration values that must be provided before use. For example
you need to know the ip address you're going to use for exposing the ports on the OKD cluster. For CSC
this ip-address can be retrieved with service request to servicedesk@csc.fi. For other OKD providers the
setup can change so ask your provider how to set up LoadBalancer service.

#### Server-side setup

##### 1. Setup InfluxDB
- In InfluxDB [Deployment file](server/influx/influx.yaml), set the URL's value in the influx-route host value, as well as the port values
- Deploy the InfluxDB by running: 
  ```bash
  oc create -f server/influx/influx.yaml
  ```

- Go to the deployed influxDB, and complete the onboarding, and create the required buckets: 
  - amqp_bucket0
  - amqp_bucket1
  - coap_bucket0
  - coap_bucket1
  - http_bucket0
  - http3_bucket0
  - mqtt_bucket0
  - mqtt_bucket1
  - mqtt_bucket2

##### 2. Setup the clock offset calculator by running:
```bash
oc create -f clock_offset_calculator/offset.yaml 
  ```

##### 3. Protocol setups
- Create files config.env and secrets.env. Fill the files with the variables shown below.
```
# config.env

CERT_PATH=/certs
CLIENT_ID=randomCliendId
LOG_FILE_PATH=/tmp/log.txt
LOG_LEVEL=WARNING
TSDB_URL=
TSDB_PORT=443
TSDB_PROTOCOL=https
TSDB_ORG=
WORKER_COUNT=10
BROKER_IP=
BROKER_PORT=30686
```

```
# secrets.env

MQTT_USERNAME=
MQTT_PASSWORD=
COAPS_PSK_KEY=16 char identifier
COAPS_PSK_IDENTITY=
RABBITMQ_DEFAULT_USER=
RABBITMQ_DEFAULT_PASS=
TSDB_USERNAME=
TSDB_PASSWORD=
TSDB_TOKEN=
```

  - The usernames and passwords can be defined as anything for the secrets.env except the TSDB_TOKEN. an API Token with read-buckets permission is required. The instructions for fetching it is found at https://docs.influxdata.com/influxdb/v2/admin/tokens/create-token/

  - For the config.env BROKER_IP will be the ip-address you got from your OKD provider. TSDB_URL is basically whatever but it needs to be same as the route for influxdb in server/influx/influx.yaml. The TSDB_ORG is the organization you set during the influx initial setup.

- Deploy config to Rahti as ConfigMap:
```bash
oc create configmap app-config --from-env-file=config.env --dry-run=client -o yaml | oc apply -f -
```

- Deploy secrets.env as Secret to Rahti:
```bash
 oc create secret generic app-secrets --from-env-file=secrets.env --dry-run=client -o yaml | oc apply -f -
```

##### 4. Protocol-specific config

- Before being able to deploy the protocol to the cloud, the image must be made available. This can be done by deploying the protocols' dockerfile (found in server/protocol_setups/protocol) to any image repository (e.g., Rahti's (ImageStream)[https://docs.csc.fi/cloud/rahti/images/Using_Rahti_integrated_registry/], or DockerHub)
- The protocols' deployment YAML file (e.g., [logger.yaml](server/protocol_setups/mqtt/logger.yaml)) must then be modified to use that deployed image.
- Also in the protocols' deployment files, fill the values for
  - image
  - port values (Deployment & Service)
  - Namespace

#### Client-side

##### 1. Dependencies and environment
- Create and activate a new venv
- Install the dependencies by running 
  ```bash
  pip install -r client/can_feeder/setup_scripts/requirements.txt
  ```

##### 2. Setup the client config at [client/benchmarking/benchmark_config.json](client/benchmarking/benchmark_config.json)

##### 3. Generate the required self-signed certificates for the protocols by running:
  ```bash
  ( cd certs && ./generate_certs.sh )
```



##### 4. Push the certs and other secretive data as Secrets to the cloud to be used by the server-side components in Rahti by running:
  ```bash
  oc create secret generic certs   --from-file=ca.crt=certs/ca.crt   --from-file=server.crt=certs/server.crt   --from-file=server.key=certs/server.key   --dry-run=client -o yaml | oc apply -f - 
  ```

## Usage

- To run the benchmark, run the [client/benchmarking/benchmark_script.py](client/benchmarking/benchmark_script.py) script. 
- Pass the arguments:
  --protocol (mqtt, coap, amqp, http2, or http3)
  --qos (0,1,2)
  --setting ("simulation" or "can")
- Example: 
    ```bash
    python3 client/benchmarking/benchmark_script.py --protocol=mqtt --qos=1 --setting=simulation
    ```

- If you want to run all protocols, just leave out the `--protocol` and `--qos` arguments.
- Example:
    ```bash
    python3 client/benchmarking/benchmark_script.py
    ```
