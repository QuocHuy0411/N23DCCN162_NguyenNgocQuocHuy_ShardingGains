from __future__ import annotations

import argparse
import sys

from coordinator.benchmark import run_benchmark
from coordinator.config import DEFAULT_ROWS, DEFAULT_RUNS, SCENARIOS
from coordinator.dataset_generator import generate_dataset
from coordinator.loader import init_database, load_all_scenarios
from coordinator.reporter import print_benchmark_table, save_results


def configure_console_encoding() -> None:
    """Ép stdout/stderr dùng UTF-8 để terminal in tiếng Việt ổn định hơn."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    """Tạo bộ phân tích lệnh CLI cho generate, init-db, load và benchmark."""
    parser = argparse.ArgumentParser(
        prog="python -m coordinator.main",
        description="Horizontal Scaling Efficiency: Sharding Gains",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="Generate user log CSV")
    generate_parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    generate_parser.add_argument("--force", action="store_true")

    subparsers.add_parser("init-db", help="Create benchmark tables on every node")

    load_parser = subparsers.add_parser("load", help="Load data into all scenarios")
    load_parser.add_argument(
        "--keep-chunks",
        action="store_true",
        help="Keep generated per-shard CSV chunks under data/load_chunks",
    )

    benchmark_parser = subparsers.add_parser("benchmark", help="Run benchmark")
    benchmark_parser.add_argument("--nodes", type=int, choices=SCENARIOS)
    benchmark_parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)

    return parser


def main() -> None:
    """Điểm vào CLI, gọi đúng workflow theo lệnh người dùng truyền vào."""
    configure_console_encoding()
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "generate":
        generate_dataset(rows=args.rows, force=args.force)
    elif args.command == "init-db":
        init_database()
    elif args.command == "load":
        load_all_scenarios(keep_chunks=args.keep_chunks)
    elif args.command == "benchmark":
        if args.runs <= 0:
            raise ValueError("--runs must be greater than 0")
        results = run_benchmark(nodes=args.nodes, runs=args.runs)
        print_benchmark_table(results, runs=args.runs)
        save_results(results)
    else:
        parser.error(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
