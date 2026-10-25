"""Раннер обогатителя матчей (Run Match Enricher).

Отвечает за:
1. Инициализацию SQLite (`raw_matches`, `enriched_matches`, `enricher_state`, `enricher_config`);
2. Сериализацию стейта обогатителя (UPSERT);
3. Динамическое чтение конфигурации из таблицы `enricher_config` на каждой итерации;
4. Внешний контроль пауз (сна) через `match state:`.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Добавляем корень проекта в sys.path для прямой поддержки запуска скрипта
ROOT_DIR = str(Path(__file__).resolve().parent.parent.parent)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    from src import config
    from src.clients.stratz import Stratz
    from src.workers.match_enricher import (
        BaseEnricherState,
        DailyLimitExceeded,
        Enriching,
        EnricherState,
        Idle,
        WaitForLimits,
        enforce_enriched_limit,
        step,
    )
except ImportError:
    import config  # type: ignore
    from stratz import Stratz  # type: ignore
    from match_enricher import (  # type: ignore
        BaseEnricherState,
        DailyLimitExceeded,
        Enriching,
        EnricherState,
        Idle,
        WaitForLimits,
        enforce_enriched_limit,
        step,
    )

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("enricher_runner")


@dataclass
class EnricherConfig:
    """Конфигурация параметров обогатителя, читаемая из SQLite."""

    running: bool = False
    stratz_token: Optional[str] = config.STRATZ_TOKEN
    min_age_hours: float = config.ENRICHER_MIN_AGE_HOURS
    chunk_size: int = config.ENRICHER_CHUNK_SIZE
    poll_interval: float = config.ENRICHER_POLL_INTERVAL
    limit_sleep: float = config.ENRICHER_LIMIT_SLEEP
    chunk_delay: float = config.ENRICHER_CHUNK_DELAY
    max_enriched_matches: int = config.ENRICHED_MAX_MATCHES
    daily_request_limit: Optional[int] = None
    daily_requests_count: int = 0
    daily_requests_date: Optional[str] = None


def init_db(db_path: str = config.DB_PATH) -> sqlite3.Connection:
    """Инициализация SQLite: таблицы raw_matches, enriched_matches, enricher_state и enricher_config."""
    dirname = os.path.dirname(db_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=config.DB_TIMEOUT_SEC,
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute(f"PRAGMA busy_timeout = {config.DB_BUSY_TIMEOUT_MS}")

    with conn:
        # Буфер сырых матчей от продюсера
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_matches (
                match_id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_matches_created_at
            ON raw_matches (created_at ASC)
            """
        )

        # Таблица обогащённых матчей
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS enriched_matches (
                match_id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_enriched_matches_created_at
            ON enriched_matches (created_at ASC, match_id ASC)
            """
        )

        # Таблица сохранения стейта обогатителя
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS enricher_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute("INSERT OR IGNORE INTO enricher_state (id, state_json) VALUES (1, '{}')")

        # Таблица параметров обогатителя и управления (running/pause)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS enricher_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                running INTEGER NOT NULL DEFAULT 0,
                stratz_token TEXT,
                min_age_hours REAL NOT NULL DEFAULT {config.ENRICHER_MIN_AGE_HOURS},
                chunk_size INTEGER NOT NULL DEFAULT {config.ENRICHER_CHUNK_SIZE},
                poll_interval REAL NOT NULL DEFAULT {config.ENRICHER_POLL_INTERVAL},
                limit_sleep REAL NOT NULL DEFAULT {config.ENRICHER_LIMIT_SLEEP},
                chunk_delay REAL NOT NULL DEFAULT {config.ENRICHER_CHUNK_DELAY},
                max_enriched_matches INTEGER NOT NULL DEFAULT {config.ENRICHED_MAX_MATCHES},
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO enricher_config (
                id, running, stratz_token, min_age_hours, chunk_size,
                poll_interval, limit_sleep, chunk_delay, max_enriched_matches
            ) VALUES (1, 0, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                config.STRATZ_TOKEN,
                config.ENRICHER_MIN_AGE_HOURS,
                config.ENRICHER_CHUNK_SIZE,
                config.ENRICHER_POLL_INTERVAL,
                config.ENRICHER_LIMIT_SLEEP,
                config.ENRICHER_CHUNK_DELAY,
                config.ENRICHED_MAX_MATCHES,
            ),
        )

    return conn


STOP_REQUESTED = False


def _signal_handler(signum: int, frame: Any) -> None:
    global STOP_REQUESTED
    logger.info("Получен сигнал остановки обогатителя (%s). Завершаем текущий шаг...", signum)
    STOP_REQUESTED = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def sleep_interruptible(seconds: float) -> bool:
    """Прерываемый сон для мгновенного отклика на сигналы остановки (Ctrl+C / SIGTERM)."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        if STOP_REQUESTED:
            return True
        time.sleep(min(0.2, max(0.0, deadline - time.time())))
    return STOP_REQUESTED


def load_enricher_config(conn: sqlite3.Connection) -> EnricherConfig:
    """Загрузить актуальные параметры обогатителя из таблицы enricher_config."""
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    row = cur.execute("SELECT * FROM enricher_config WHERE id = 1").fetchone()
    if not row:
        return EnricherConfig()

    keys = row.keys()
    return EnricherConfig(
        running=bool(row["running"]),
        stratz_token=row["stratz_token"] if row["stratz_token"] else "",
        min_age_hours=max(0.0, float(row["min_age_hours"])) if row["min_age_hours"] is not None else config.ENRICHER_MIN_AGE_HOURS,
        chunk_size=max(1, int(row["chunk_size"])) if row["chunk_size"] is not None else config.ENRICHER_CHUNK_SIZE,
        poll_interval=max(0.1, float(row["poll_interval"])) if row["poll_interval"] is not None else config.ENRICHER_POLL_INTERVAL,
        limit_sleep=max(0.1, float(row["limit_sleep"])) if row["limit_sleep"] is not None else config.ENRICHER_LIMIT_SLEEP,
        chunk_delay=max(0.0, float(row["chunk_delay"])) if row["chunk_delay"] is not None else config.ENRICHER_CHUNK_DELAY,
        max_enriched_matches=max(0, int(row["max_enriched_matches"])) if row["max_enriched_matches"] is not None else config.ENRICHED_MAX_MATCHES,
        daily_request_limit=int(row["daily_request_limit"]) if "daily_request_limit" in keys and row["daily_request_limit"] is not None else None,
        daily_requests_count=int(row["daily_requests_count"]) if "daily_requests_count" in keys and row["daily_requests_count"] is not None else 0,
        daily_requests_date=str(row["daily_requests_date"]) if "daily_requests_date" in keys and row["daily_requests_date"] else None,
    )


def update_enricher_config(
    conn: sqlite3.Connection,
    running: Optional[bool] = None,
    stratz_token: Optional[str] = None,
    min_age_hours: Optional[float] = None,
    chunk_size: Optional[int] = None,
    poll_interval: Optional[float] = None,
    limit_sleep: Optional[float] = None,
    chunk_delay: Optional[float] = None,
    max_enriched_matches: Optional[int] = None,
) -> None:
    """Обновить параметры обогатителя в таблице enricher_config."""
    fields = []
    values = []

    if running is not None:
        fields.append("running = ?")
        values.append(1 if running else 0)
    if stratz_token is not None:
        fields.append("stratz_token = ?")
        values.append(stratz_token)
    if min_age_hours is not None:
        fields.append("min_age_hours = ?")
        values.append(max(0.0, min_age_hours))
    if chunk_size is not None:
        fields.append("chunk_size = ?")
        values.append(max(1, chunk_size))
    if poll_interval is not None:
        fields.append("poll_interval = ?")
        values.append(max(0.1, poll_interval))
    if limit_sleep is not None:
        fields.append("limit_sleep = ?")
        values.append(max(0.1, limit_sleep))
    if chunk_delay is not None:
        fields.append("chunk_delay = ?")
        values.append(max(0.0, chunk_delay))
    if max_enriched_matches is not None:
        fields.append("max_enriched_matches = ?")
        values.append(max(0, max_enriched_matches))

    if not fields:
        return

    fields.append("updated_at = datetime('now')")
    query = f"UPDATE enricher_config SET {', '.join(fields)} WHERE id = 1"
    with conn:
        conn.execute(query, tuple(values))


def load_enricher_state(conn: sqlite3.Connection) -> EnricherState:
    """Загрузить стейт обогатителя из БД."""
    row = conn.execute("SELECT state_json FROM enricher_state WHERE id = 1").fetchone()
    if row and row[0]:
        return BaseEnricherState.from_json(row[0])
    return Idle()


def save_enricher_state(conn: sqlite3.Connection, state: EnricherState) -> None:
    """Сериализовать стейт обогатителя в SQLite (UPSERT)."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with conn:
        conn.execute(
            """
            INSERT INTO enricher_state (id, state_json, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (state.to_json(), now_utc),
        )


def run_enricher(db_path: str = config.DB_PATH) -> None:
    """Главный цикл обогатителя: параметры читаются из enricher_config на каждой итерации."""
    conn = init_db(db_path)
    client = Stratz()

    state = load_enricher_state(conn)
    logger.info("Старт обогатителя. Начальный стейт: %s", state.status.value)

    was_running = False
    first_check = True

    try:
        while not STOP_REQUESTED:
            try:
                # 1. Читаем актуальные параметры из базы данных на каждой итерации
                cfg = load_enricher_config(conn)

                # 2. Обогатитель непрерывно следит за тем, не превышает ли таблица максимум записей (буквально 0 = очистить)
                if cfg.max_enriched_matches >= 0:
                    enforce_enriched_limit(conn, cfg.max_enriched_matches)

                # 3. Проверяем флаг running (пауза/пуск из админки)
                if not cfg.running:
                    if was_running or first_check:
                        logger.info("Обогатитель на паузе (running=0 в enricher_config). Ожидание запуска...")
                        was_running = False
                        first_check = False
                    if sleep_interruptible(cfg.poll_interval):
                        break
                    continue

                if not was_running:
                    logger.info("Обогатитель возобновил работу (running=1).")
                    was_running = True
                    first_check = False

                # 3. Синхронизируем токен Stratz при его изменении в БД
                if cfg.stratz_token != client.token:
                    client.token = cfg.stratz_token
                    client.session.headers.update({"Authorization": f"Bearer {cfg.stratz_token}"})

                # 4. Шаг стейт-машины с актуальными параметрами из БД
                state = step(
                    state=state,
                    client=client,
                    conn=conn,
                    min_age_hours=cfg.min_age_hours,
                    chunk_size=cfg.chunk_size,
                    max_enriched_matches=cfg.max_enriched_matches,
                )

                # 5. Раннер сериализует обновлённый стейт
                save_enricher_state(conn, state)

                if STOP_REQUESTED:
                    break

                # 6. Раннер контролирует сон снаружи через match state: с актуальными таймингами
                match state:
                    case Idle() | DailyLimitExceeded():
                        if sleep_interruptible(cfg.poll_interval):
                            break
                    case WaitForLimits():
                        logger.info("Сон по лимитам Stratz API: %.1f сек...", cfg.limit_sleep)
                        if sleep_interruptible(cfg.limit_sleep):
                            break
                    case Enriching():
                        if sleep_interruptible(cfg.chunk_delay):
                            break
            except Exception as loop_err:
                logger.exception("Непредвиденная ошибка в итерации обогатителя: %s. Повтор через 2 сек...", loop_err)
                if sleep_interruptible(2.0):
                    break

    except KeyboardInterrupt:
        logger.info("Остановка обогатителя пользователем.")
    finally:
        save_enricher_state(conn, state)
        conn.close()
        client.session.close()
        logger.info("Обогатитель остановлен.")


def main() -> None:
    """Точка входа CLI."""
    parser = argparse.ArgumentParser(description="Раннер обогатителя матчей Dota 2.")
    parser.add_argument("--db", default=config.DB_PATH, help=f"Путь к SQLite (по умолчанию {config.DB_PATH})")
    args = parser.parse_args()

    run_enricher(db_path=args.db)


if __name__ == "__main__":
    main()
