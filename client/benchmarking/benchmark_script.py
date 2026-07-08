import datetime
import json
from pathlib import Path
import subprocess
import threading
import time
import argparse
import os
from latency_script import calculate_latency_chunked, summarize_local_pub
import signal
from cloud_orchestrator import Orchestrator
import warnings
from calc_offset import run_offset_calc
import traceback

# Ignore depracated packages dependency etc warnigns
warnings.filterwarnings("ignore", category=DeprecationWarning)


# Basically the "main" functionality of function
def run_benchmark(protocol, qos=0, setting="simulation"):

    # Set project root to be 2 folders down.
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    # Set config path to be in same folder
    # NOTE: Should this be moved to own folder?
    config_path = Path(__file__).parent / "benchmark_config.json"

    # Load config
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Get clock offset address. The /sync needs to be included in the address.
    clock_offset_address = config["client_settings"]["clock_offset_address"]

    # Create timestamps from this moment in format described in strftime().
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Get rahti project from config and creat orchestrator.
    rahti_project = config["server_settings"]["rahti_project"]
    orchestrator = Orchestrator(rahti_project)

    # Define what python environment to use and where to find client codes.
    python_exec = PROJECT_ROOT / f"{str(config["general"]["venv_path"])}/bin/python"
    client_root = PROJECT_ROOT / "client/can_feeder"

    # Set up protocols
    if protocol is None:
        protocol_order = {
            "http3": [0],
            "amqp": [0, 1],
            "http2": [0],
            "coap": [0, 1],
            "mqtt": [0, 1, 2],
        }
    else:
        protocol_order = {protocol: [qos]}

    # Go through the different protocols.
    for protocol, qos_list in protocol_order.items():
        for qos in qos_list:
            print(f"Starting benchmark for protocol={protocol} qos={qos}")
        try:
            results_dir = f"raw_benchmarking/results/{protocol}_{timestamp}_QOS_{qos}"
            config["client_settings"]["path"] = results_dir
            os.makedirs(results_dir, exist_ok=True)

            orchestrator.delete_protocol_setup(protocol)

            orchestrator.deploy_protocol_setup(protocol, qos)
            time.sleep(5)  # delay to give time for cloud deployment to startup

            bucket: str = f"{protocol}_bucket{qos}"
            output_file = f"{results_dir}/{protocol}_results_qos_{qos}.csv"
            file_name_base = f"{results_dir}/{protocol}_test_{int(time.time())}"

            summary_output_file = f"{file_name_base}_summary.txt"
            start_time = datetime.datetime.utcnow().isoformat() + "Z"

            # start concurrent hardware metrics logging
            # TODO: update args to kwargs for consistency.
            oc_stop = threading.Event()
            oc_thread = threading.Thread(
                target=get_hardware_metrics,
                args=(f"{results_dir}/hardware_metrics.txt", 5, oc_stop),
                daemon=True,
            )
            oc_thread.start()

            # start concurrent network metrics logging
            vn_stop = threading.Event()
            vn_log_path = f"{results_dir}/vnstat_continuous.txt"
            print(
                "Logging VnStat:", orchestrator.get_endpoint_pod_name(protocol=protocol)
            )
            vn_thread = threading.Thread(
                target=get_network_metrics_vnstat,
                kwargs=dict(
                    duration=5,
                    out_path=vn_log_path,
                    pod=orchestrator.get_endpoint_pod_name(protocol=protocol),
                    container="vnstat",
                    iface="eth0",
                    stop_event=vn_stop,
                ),
                daemon=True,
            )
            vn_thread.start()

            # start concurrent client for clock_offset_calculator.
            offset_stop = threading.Event()
            offset_thread = threading.Thread(
                target=run_offset_calc,
                kwargs=dict(
                    url=clock_offset_address,
                    output_file=f"{file_name_base}_offset.txt",
                    interval=1,
                    stop_event=offset_stop,
                ),
                daemon=True,
            )

            offset_thread.start()

            proc = subprocess.Popen(
                [
                    str(python_exec),
                    f"{str(client_root)}/{protocol}/{protocol}_main.py",
                    "--qos",
                    str(qos),
                    "--output",
                    f"{file_name_base}_summary.txt",
                    "--setting",
                    setting,
                ]
            )

            try:
                proc.wait()
            except KeyboardInterrupt:
                print("SIGINT, stop client gracefully")
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    print("Child didnt exit in time, forcing kill.")
                    proc.kill()

            # stop concurrent logging
            oc_stop.set()
            oc_thread.join(timeout=2)

            vn_stop.set()
            vn_thread.join(timeout=2)

            offset_stop.set()
            offset_thread.join(timeout=2)

            # download results from influx
            time.sleep(15)
            stop_time = datetime.datetime.utcnow().isoformat() + "Z"

            run_dir = f"{results_dir}/run"
            os.makedirs(run_dir, exist_ok=True)

            summary_file = os.path.join(run_dir, f"run_summary.txt")

            with open(summary_file, "a", buffering=1) as f:
                f.write(f"=== Benchmark run summary ===\n")
                f.write(f"Start time: {start_time}\n")
                f.write(f"Stop time: {stop_time}\n")
                f.write(f"Output file: {output_file}\n")
                f.write(f"Summary output file: {summary_output_file}\n")
                f.write("\n")

            calculate_latency_chunked(
                bucket=bucket,
                org=config["general"]["influx_org"],
                start_iso=start_time,
                stop_iso=stop_time,
                raw_output_file=output_file,
                summary_output_file=summary_output_file,
                # local_data=f"{file_name_base}_local_tests.txt",
                # qos=qos,
                window_seconds=30,
                max_rows_per_chunk=5000,
                sleep_between_seconds=1,
                offset_csv=f"{file_name_base}_offset.txt",
            )

            summarize_local_pub(path=summary_output_file)

            orchestrator.delete_protocol_setup(protocol)

            # NOTE: This was before after finally
            stop_time = datetime.datetime.utcnow().isoformat() + "Z"
            run_dir = f"{results_dir}/run"
            os.makedirs(run_dir, exist_ok=True)
            summary_file = os.path.join(run_dir, f"run_summary.txt")
            try:
                with open(summary_file, "a", buffering=1) as f:
                    f.write(f"=== Benchmark run summary ===\n")
                    f.write(f"Start time: {start_time}\n")
                    f.write(f"Stop time: {stop_time}\n")
                    f.write(f"Output file: {output_file}\n")
                    f.write(f"Summary output file: {summary_output_file}\n")
                    f.write("\n")
                time.sleep(10)

            except UnboundLocalError as e:
                print("Error with running the benchmark")

        except Exception as e:
            print(e)
            traceback.print_exc()


def get_hardware_metrics(out_path: str, interval_sec: int, stop_event: threading.Event):
    dirpath = os.path.dirname(out_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    with open(out_path, "a", buffering=1) as f:
        while not stop_event.is_set():
            ts = datetime.datetime.utcnow().isoformat() + "Z"
            res = subprocess.run(
                ["oc", "adm", "top", "pods"], capture_output=True, text=True
            )
            f.write(f"=== {ts} ===\n")
            if res.stdout:
                f.write(res.stdout)
            if res.stderr:
                f.write(res.stderr)
            f.write("\n")

            for _ in range(int(interval_sec * 10)):
                if stop_event.is_set():
                    break
                time.sleep(0.1)


def get_network_metrics_vnstat(
    duration, out_path, pod, container, iface, stop_event: threading.Event = None
):
    dirpath = os.path.dirname(out_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    with open(out_path, "a", buffering=1) as f:
        while not stop_event.is_set():
            ts = datetime.datetime.utcnow().isoformat() + "Z"
            f.write(f"=== {ts} ===\n")
            res = subprocess.run(
                [
                    "oc",
                    "exec",
                    pod,
                    "-c",
                    container,
                    "--",
                    "vnstat",
                    "-tr",
                    str(duration),
                    "-i",
                    iface,
                ],
                capture_output=True,
                text=True,
            )
            if res.stdout:
                f.write(res.stdout)
            if res.stderr:
                f.write(res.stderr)
            f.write("\n")

            for _ in range(int(duration * 10)):
                if stop_event.is_set():
                    break
                time.sleep(0.1)
            time.sleep(0.1)


def get_network_metrics(duration, out_path="network_metrics.txt", ip=0, iperf_port="0"):
    iperf_args = [
        "iperf3",
        "-c",
        ip,
        "-p",
        iperf_port,
        "-t",
        str(duration),
        "-i",
        "10",  # , "-b", "2M"
    ]

    dirpath = os.path.dirname(out_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    iperf_out = open(out_path, "w")

    try:
        process = subprocess.Popen(
            iperf_args, stdout=iperf_out, stderr=subprocess.STDOUT
        )
        return process

    except:
        iperf_out.close()
        print("iperf failed")
        raise


# NOTE: It would be good to think if we need the protocols to EVER run sequantially.
# Would that be good for the tests or not? IMO not because then the tests are not controlled
# and can be exposed to confounding factors like the position of the network base station,
# the level of the terrain, direction of movement and such. Other opinions neede on this matter.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--protocol",
        choices=[
            "mqtt",
            "amqp",
            "coap",
            "http2",
            "http3",
        ],
        default=None,
        help="Protocols",
    )
    parser.add_argument(
        "-q",
        "--qos",
        type=int,
        default=None,
        help="Quality of Service level",
    )
    parser.add_argument(
        "-s",
        "--setting",
        type=str,
        default="simulation",
    )
    args = parser.parse_args()

    protocol = args.protocol
    qos = args.qos
    setting = args.setting

    run_benchmark(
        protocol=protocol,
        qos=qos,
        setting=setting,
    )


if __name__ == "__main__":
    main()
