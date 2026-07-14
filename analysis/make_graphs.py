import argparse
from collections import defaultdict
import csv
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import gaussian_kde
import seaborn as sns
from matplotlib.lines import Line2D
from matplotlib.ticker import LogLocator, NullFormatter, ScalarFormatter


COLOR_MAP = {
    "MQTT QoS 0": "#2F3E63",
    "MQTT QoS 1": "#6A3D9A",
    "MQTT QoS 2": "#8F63B8",

    "AMQP Auto": "#0072B2",
    "AMQP Man": "#56B4E9",

    "CoAP NON": "#D55E00",
    "CoAP CON": "#E69F00",

    "HTTP/2": "#009E73",
    "HTTP/3": "#CC79A7",
}

STYLE_MAP = {
    "MQTT QoS 0": {"marker": "o", "linestyle": "-"},
    "MQTT QoS 1": {"marker": "s", "linestyle": "-."},
    "MQTT QoS 2": {"marker": "^", "linestyle": ":"},
    "AMQP Auto": {"marker": "D", "linestyle": "-"},
    "AMQP Man": {"marker": "P", "linestyle": "-."},
    "CoAP NON": {"marker": "X", "linestyle": "-"},
    "CoAP CON": {"marker": "v", "linestyle": "-."},
    "HTTP/2": {"marker": ">", "linestyle": "-"},
    "HTTP/3": {"marker": "<", "linestyle": "-"},
}



def build_label_from_filename(csv_file, qos_mapping):
    stem = csv_file.stem.lower()

    protocol = None
    for key in sorted(qos_mapping.keys(), key=len, reverse=True):
        if stem.startswith(key.lower()):
            protocol = key
            break

    if protocol is None:
        return stem

    qos_level = None
    parts = stem.split("_")

    if "qos" in parts:
        i = parts.index("qos")
        if i + 1 < len(parts):
            try:
                qos_level = int(parts[i + 1])
            except ValueError:
                pass

    qos_labels = qos_mapping.get(protocol, [])

    if qos_level is not None and qos_level < len(qos_labels):
        qos_name = qos_labels[qos_level]
        return f"{protocol} {qos_name}"

    return protocol


def plot_dv_curves_from_folder(
    folder_path,
    parent_dir,
    title,
    bucket_size_sec=1,
    name="",
):

    sns.set_theme(style="whitegrid")
    bucket_size_ms = bucket_size_sec * 1000
    csv_files = sorted(Path(folder_path).glob("*.csv"))

    if not csv_files:
        print(f"No csv files found")
        return

    qos_mapping = {
        "MQTT": ["QoS 0", "QoS 1", "QoS 2"],
        "AMQP": ["Auto", "Man"],
        "CoAP": ["NON", "CON"],
        "HTTP": [],
        "HTTP3": [],
    }

    fig, ax = plt.subplots()
    for csv_file in csv_files:
        rows = []

        with open(csv_file, "r", newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                v2 = row.get("v2")
                dv = row.get("dv")

                if not v2 or not dv:
                    continue

                rows.append((float(v2), float(dv)))

        if not rows:
            print(f"No valid rows in {csv_file.name}")
            continue

        rows.sort(key=lambda x: x[0])
        start_v2 = rows[0][0]

        buckets = defaultdict(list)

        for v2, dv in rows:
            relative_ms = v2 - start_v2
            bucket = int(relative_ms // bucket_size_ms)
            buckets[bucket].append(dv)

        x_values = []
        y_values = []

        for bucket in sorted(buckets.keys()):
            values = buckets[bucket]
            p99_latency = np.percentile(values, 99)

            bucket_time_sec = bucket * bucket_size_sec
            x_values.append(bucket_time_sec)
            y_values.append(p99_latency)

        label = build_label_from_filename(csv_file, qos_mapping)
        if label == "HTTP":
            label = "HTTP/2"
        if label == "HTTP3":
            label = "HTTP/3"

        print(f"{csv_file.name}: buckets={len(x_values)}, label={label}")
        style = STYLE_MAP.get(label, {"marker": "o", "linestyle": "-"})
        color = COLOR_MAP.get(label)

        sns.lineplot(
            x=x_values,
            y=y_values,
            label=label,
            alpha=0.8,
            ax=ax,
            linestyle=style["linestyle"],
            linewidth=1.3,
            color=color,
        )

    for spine in ax.spines.values():
        spine.set_color("black")

    ax.set_xlabel("time since start (seconds)")
    ax.set_ylabel("latency (ms)")
    ax.set_title(title)
    ax.legend()
    ax.set_ylim(0, 500)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    output = f"graphs/{parent_dir}_{name}.png"
    fig.savefig(output, dpi=200)
    print(output)

def point_on_curve(x_values, y_values, target_x):
    # gets the x-value closest to the target_x
    closest_index = np.abs(x_values - target_x).argmin()
    return x_values[closest_index], y_values[closest_index]


def calculate_probability_density_curve_from_folder(folder_path,
    parent_dir,
    title,
    bucket_size_sec=1,
    name="",):
    
    sns.set_theme(style="whitegrid")
    
    csv_files = sorted(Path(folder_path).glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return
    
    qos_mapping = {
        "MQTT": ["QoS 0", "QoS 1", "QoS 2"],
        "AMQP": ["Auto", "Man"],
        "CoAP": ["NON", "CON"],
        "HTTP": [],
        "HTTP3": [],
    }

    fig, ax = plt.subplots()

    for csv_file in csv_files:
        dv_values = []

        with open(csv_file, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                dv = row.get("dv")
                if not dv:
                    continue

                dv = float(dv)
                if dv <= 0:
                    continue

                dv_values.append(dv)

        if len(dv_values) < 2:
            print(f"Not enough valid dv values in {csv_file.name}")
            continue
    
        dv_values = np.array(dv_values)
        
        log_dv = np.log(dv_values)
        kde = gaussian_kde(log_dv)

        x_log = np.linspace(log_dv.min(), log_dv.max(), 1000)
        y = kde(x_log)
        x = np.exp(x_log)

        label = build_label_from_filename(csv_file, qos_mapping)
        if label == "HTTP":
            label = "HTTP/2"
        if label == "HTTP3":
            label = "HTTP/3"

        protocol = label
        style = STYLE_MAP.get(label, {"marker": "o", "linestyle": "-"})
        color = COLOR_MAP.get(protocol)

        sns.lineplot(x=x, y=y, label=label, alpha=0.8, ax=ax, linestyle=style["linestyle"], linewidth=1.3, markersize=6, color=color)
        print(
            f"{csv_file.name}: n={len(dv_values)}, "
            f"min={dv_values.min():.3f}, max={dv_values.max():.3f}, label={label}"
        )

        p50 = np.percentile(dv_values, 50)
        p95 = np.percentile(dv_values, 95)
        p99 = np.percentile(dv_values, 99)

        x50, y50 = point_on_curve(x, y, p50)
        x95, y95 = point_on_curve(x, y, p95)
        x99, y99 = point_on_curve(x, y, p99)

        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(ScalarFormatter())
        ax.ticklabel_format(style="plain", axis="x")
        ax.xaxis.set_minor_locator(LogLocator(base=10.0, subs=np.arange(2, 10) * 0.1))
        ax.xaxis.set_minor_formatter(NullFormatter())

        ax.tick_params(axis="x", which="major", length=6)
        ax.tick_params(axis="x", which="minor", length=3, bottom=True)

        for spine in ax.spines.values():
            spine.set_color("black")
        ax.set_xlabel("latency (ms)")
        ax.set_ylabel("probability density")
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        output = f"graphs/{parent_dir}_{name}_pdf.png"
        #ax.set_xlim(10, 1500)

        percentile_handles = [
        Line2D([0], [0], marker="o", color="white", markerfacecolor="gray",
            markeredgecolor="black", markersize=5, linestyle="None", label="p50"),
        Line2D([0], [0], marker="s", color="white", markerfacecolor="gray",
            markeredgecolor="black", markersize=5, linestyle="None", label="p95"),
        Line2D([0], [0], marker="^", color="white", markerfacecolor="gray",
            markeredgecolor="black", markersize=5, linestyle="None", label="p99"),
        ]

        protocol_handles, protocol_labels = ax.get_legend_handles_labels()

        all_handles = protocol_handles + percentile_handles
        all_labels = protocol_labels + ["p50", "p95", "p99"]

        ax.scatter([x50], [y50], color=color, s=25, marker="o", edgecolors="black", zorder=5)
        ax.scatter([x95], [y95], color=color, s=20, marker="s", edgecolors="black", zorder=5,)
        ax.scatter([x99], [y99], color=color, s=25, marker="^", edgecolors="black", zorder=5)
        protocol_legend = ax.legend(all_handles,all_labels, fontsize=8)
        ax.add_artist(protocol_legend)

        fig.savefig(output, dpi=200)


        print(output)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test")
    parser.add_argument("--title")

    p = parser.parse_args()
    test = p.test
    title = p.title

    calculate_probability_density_curve_from_folder(
        folder_path=f"PATH/TO/csv_files",
        parent_dir="toyota_brake",
        title=title,
        bucket_size_sec=10,
        name=f"{test}_bucketed_fixed",
    )
