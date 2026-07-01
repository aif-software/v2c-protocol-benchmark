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

- For setting up non-TCP connections in Rahti, follow:  
  https://docs.csc.fi/cloud/rahti/networking/#using-loadbalancer-service-type-with-dedicated-ips  

### Benchmark setup

This project relies on a number of configuration values that must be provided before use.

#### Server-side setup

##### 1. Setup InfluxDB
- In InfluxDB [Deployment file](server/influx/influx.yaml), set the URL's value in the influx-route host value, as well as the port values
- Deploy the InfluxDB by running: 
  ```bash
  oc create -f server/influx/influx.yaml
  ```

- Go to the deployed influxDB, and complete the onboarding, and create the required buckets. (The naming convention for buckets is protocol_bucketqos. for example, http_bucket0 or mqtt_bucket2).

##### 2. Setup the clock offset calculator by running:
```bash
oc create -f clock_offset_calculator/offset.yaml 
  ```

##### 3. Protocol setups
- Set the environmental variables in [config.env](config.env) and [secrets.env](secrets.env)
  - The username and passwords can be defined as anything. For the TSDB_TOKEN, an API Token with read-buckets permission is required. The instructions for fetching it is found at https://docs.influxdata.com/influxdb/v2/admin/tokens/create-token/


- Deploy config to Rahti as ConfigMap:
```bash
oc create configmap app-config --from-env-file=config.env --dry-run=client -o yaml | oc apply -f -
```

- Deploy secrets.env as Secret to Rahti:
```bash
 oc create secret generic app-secrets --from-env-file=secrets.env --dry-run=client -o yaml | oc apply -f -
```

##### 4. Protocol-specific config

- Before being able to deploy the protocol to the cloud, the image must be made available. This can be done by deploying the protocols' dockerfile (found in server/protocol_setups/protocol) to any image repository (e.g., Rahti's ImageStream, or DockerHub)
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
