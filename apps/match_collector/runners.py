from __future__ import annotations

import logging
import multiprocessing
import sys

from .config import AppConfig
from .container import ApplicationContainer

logger = logging.getLogger("apps.match_collector.runners")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(processName)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def run_public_collector(config: AppConfig) -> None:
    """Запускает воркер сбора публичных матчей в текущем процессе."""
    _setup_logging()
    container = ApplicationContainer(config=config)
    runner = container.create_public_collector_runner()
    try:
        runner.run(auto_enable=False)
    finally:
        container.close()


def run_match_enricher(config: AppConfig) -> None:
    """Запускает воркер обогащения матчей в текущем процессе."""
    _setup_logging()
    container = ApplicationContainer(config=config)
    runner = container.create_enricher_runner()
    try:
        runner.run(auto_enable=False)
    finally:
        container.close()


def run_storage_writer(config: AppConfig) -> None:
    """Запускает воркер сохранения обогащенных матчей в целевую базу данных."""
    _setup_logging()
    container = ApplicationContainer(config=config)
    runner = container.create_storage_writer_runner()
    try:
        runner.run(auto_enable=True)
    finally:
        container.close()


def run_all(config: AppConfig) -> None:
    """Запускает воркеры сбора, обогащения и сохранения в отдельных процессах ОС."""
    _setup_logging()
    p_collector = multiprocessing.Process(
        target=run_public_collector,
        args=(config,),
        name="public_match_collector",
    )
    p_enricher = multiprocessing.Process(
        target=run_match_enricher,
        args=(config,),
        name="match_enricher",
    )
    p_writer = multiprocessing.Process(
        target=run_storage_writer,
        args=(config,),
        name="match_storage_writer",
    )

    logger.info("Starting collector, enricher, and storage writer worker processes...")
    p_collector.start()
    p_enricher.start()
    p_writer.start()

    try:
        p_collector.join()
        p_enricher.join()
        p_writer.join()
    except KeyboardInterrupt:
        logger.info("Shutdown requested. Terminating worker processes...")
        p_collector.terminate()
        p_enricher.terminate()
        p_writer.terminate()
        p_collector.join()
        p_enricher.join()
        p_writer.join()
