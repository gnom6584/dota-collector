from __future__ import annotations

import argparse

from .config import AppConfig
from .runners import (
    run_all,
    run_match_enricher,
    run_public_collector,
    run_storage_writer,
)


def main() -> None:
    """Точка входа для запуска воркеров сбора, обогащения и сохранения матчей."""
    parser = argparse.ArgumentParser(description="Match Collector Worker Process Manager")
    parser.add_argument(
        "--worker",
        "-w",
        choices=["all", "collector", "enricher", "writer"],
        default="all",
        help="Target worker to run: 'collector', 'enricher', 'writer', or 'all' (default: all).",
    )
    args = parser.parse_args()
    config = AppConfig()

    match args.worker:
        case "collector":
            run_public_collector(config)
        case "enricher":
            run_match_enricher(config)
        case "writer":
            run_storage_writer(config)
        case "all":
            run_all(config)


if __name__ == "__main__":
    main()
