from pathlib import Path

CLIENT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = Path(__file__).resolve().parents[3]

BENCHMARK_CONFIG_PATH = CLIENT_ROOT / "benchmarking" / "benchmark_config.json"
