from pathlib import Path

CLIENT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]

BENCHMARK_CONFIG_PATH = PROJECT_ROOT / "benchmarking" / "benchmark_config.json"
