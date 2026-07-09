import datetime
import subprocess
import threading
import time
import argparse
import os
from latency_script import analyze_latency_data


def run_benchmark(protocol, iterations):
    duration = 60

    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_dir = f"raw_benchmarking/results/{protocol}_{timestamp}"
        os.makedirs(results_dir, exist_ok=True)
        for i in range(iterations):
            start_time = datetime.datetime.utcnow().isoformat() + "Z"

            # start concurrent network metrics logging

            iperf_out_path = f"{results_dir}/iperf_results_{i+1}.txt"
            iperf_process = get_network_metrics(duration, out_path=iperf_out_path)

            # start concurrent hardware metrics logging
            oc_stop = threading.Event()
            oc_thread = threading.Thread(
                target=get_hardware_metrics,
                args=(f"{results_dir}/hardware_metrics_{i+1}.txt", 15, oc_stop),
                daemon=True,
            )
            oc_thread.start()

            # start concurrent client

            subprocess.run(
                [
                    f"/home/ConnectiCar2.0/RaspberryPiScripts/venv/bin/python",
                    f"{protocol}_main.py",
                ]
            )

            time.sleep(duration)

            # stop concurrent logging
            oc_stop.set()
            oc_thread.join(timeout=2)

            if iperf_process and iperf_process.poll() is None:
                iperf_process.terminate()
                try:
                    iperf_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    iperf_process.kill()

            # download results from influx
            output_file = f"{results_dir}/{protocol}_results_{i+1}.csv"
            time.sleep(5)
            with open(output_file, "w") as outfile:
                subprocess.run(
                    [
                        "influx",
                        "query",
                        "--raw",
                        "--skip-verify",
                        f'from(bucket:"{protocol}_bucket") |> range(start: {start_time}) |> pivot(rowKey: ["_time", "counter"], columnKey: ["_field"], valueColumn: "_value")',
                    ],
                    stdout=outfile,
                )

            print(f"Results saved to {output_file}")

            # calculate latencies from results

            subprocess.run(
                [
                    "/home/ConnectiCar2.0/RaspberryPiScripts/venv/bin/python",
                    "benchmarking/latency_script.py",
                    output_file,
                    "--output",
                    f"{results_dir}/{protocol}_{int(time.time())}_{+1}.csv",
                ]
            )

            time.sleep(5)

        # create summary file of all results
        subprocess.run(
            [
                "/home/ConnectiCar2.0/RaspberryPiScripts/venv/bin/python",
                "benchmarking/summarize.py",
                results_dir,
                "--output",
                f"{results_dir}/{protocol}_test_{int(time.time())}_summary",
            ]
        )

    except Exception as e:
        print(e)


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


def get_network_metrics(
    duration, out_path="network_metrics.txt", ip="86.50.228.245", iperf_port="30685"
):
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol", choices=["mqtt", "amqp", "coap", "https"], required=True
    )
    args = parser.parse_args()
    protocol = args.protocol

    run_benchmark(protocol, 1)


if __name__ == "__main__":
    main()

