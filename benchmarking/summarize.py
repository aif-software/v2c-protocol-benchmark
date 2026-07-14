import argparse
import csv
import re
from statistics import mean, median
from pathlib import Path

stat_re = re.compile(
    r"^(Average|Standard Deviation|Standard deviation|Minimum|Maximum|Median|Sample count)\s*:?\s*([\d.]+)"
)


def parse_file(path):
    metrics = {}
    sample_count = 0
    current = None

    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            # section header, e.g: metric_name:
            if line.endswith(":") and not line.startswith("="):
                current = line[:-1]
                if current not in metrics:
                    metrics[current] = {}
                continue

            if not current:
                continue

            match = stat_re.match(line)
            if not match:
                continue

            stat = match.group(1)
            val = match.group(2)

            if stat.lower().startswith("sample"):
                try:
                    sample_count += int(float(val))
                except ValueError:
                    pass
                continue

            try:
                if stat not in metrics[current]:
                    metrics[current][stat] = []
                    metrics[current][stat].append(float(val))
            except ValueError:
                pass

    return metrics, sample_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--output", default="summary.csv")
    args = parser.parse_args()

    all_metrics = {}
    files_used = []
    file_samples = []

    for inp in args.inputs:
        for path in Path(inp).rglob("*"):
            if path.is_file():
                files_used.append(str(path))
                file_metrics, samples = parse_file(path)
                file_samples.append(samples)

                for metric, stats in file_metrics.items():
                    if metric not in all_metrics:
                        all_metrics[metric] = {}

                    for stat, vals in stats.items():
                        if stat not in all_metrics[metric]:
                            all_metrics[metric][stat] = []
                        all_metrics[metric][stat].extend(vals)

    total_samples = sum(file_samples)

    with open(args.output, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["files_used"] + files_used)
        w.writerow(["sample_count"] + file_samples)
        w.writerow(["total_sample_count", total_samples])
        w.writerow([])
        w.writerow(["metric", "stat", "mean", "median"])
        for metric, stats in all_metrics.items():
            for stat, vals in stats.items():
                w.writerow([metric, stat, mean(vals), median(vals)])


if __name__ == "__main__":
    main()
