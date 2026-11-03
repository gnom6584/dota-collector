from __future__ import annotations

import argparse
import logging
import multiprocessing
import signal
import sys
import time
from typing import Any

import uvicorn

from apps.admin.app import create_app
from apps.admin.config import AdminConfig
from apps.match_collector.config import AppConfig
from apps.match_collector.runners import (
    run_match_enricher,
    run_public_collector,
    run_storage_writer,
)

logger = logging.getLogger("launcher")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(processName)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def run_admin_server(config: AdminConfig) -> None:
    """Запускает HTTP-сервер панели управления (Uvicorn)."""
    _setup_logging()
    app = create_app(admin_config=config)
    logger.info("Admin control plane starting on http://%s:%d", config.host, config.port)
    uvicorn.run(app, host=config.host, port=config.port, log_config=None)


def main() -> None:
    """Универсальный процесс-супервизор для запуска воркеров и панели управления."""
    parser = argparse.ArgumentParser(description="Dota 2 Match Collector & Control Plane Launcher")
    parser.add_argument("--workers-only", action="store_true", help="Launch only match collection workers")
    parser.add_argument("--admin-only", action="store_true", help="Launch only admin control plane")
    args = parser.parse_args()

    _setup_logging()
    app_config = AppConfig()
    admin_config = AdminConfig(app_config=app_config)

    processes: list[multiprocessing.Process] = []

    if not args.admin_only:
        p_collector = multiprocessing.Process(
            target=run_public_collector,
            args=(app_config,),
            name="public_match_collector",
        )
        p_enricher = multiprocessing.Process(
            target=run_match_enricher,
            args=(app_config,),
            name="match_enricher",
        )
        p_writer = multiprocessing.Process(
            target=run_storage_writer,
            args=(app_config,),
            name="match_storage_writer",
        )
        processes.extend([p_collector, p_enricher, p_writer])

    if not args.workers_only:
        p_admin = multiprocessing.Process(
            target=run_admin_server,
            args=(admin_config,),
            name="admin_control_plane",
        )
        processes.append(p_admin)

    logger.info("Launching %d services: %s", len(processes), ", ".join(p.name for p in processes))

    for p in processes:
        p.start()

    def handle_signal(sig: int, frame: Any) -> None:
        logger.info("Termination signal %s received. Shutting down services...", sig)
        for proc in processes:
            if proc.is_alive():
                proc.terminate()

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    try:
        while any(p.is_alive() for p in processes):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        for proc in processes:
            if proc.is_alive():
                proc.terminate()
        for proc in processes:
            proc.join(timeout=5)
            if proc.is_alive():
                proc.kill()
        logger.info("All services shut down successfully.")


if __name__ == "__main__":
    main()
