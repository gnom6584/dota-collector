"""Раннер сборщика матчей (Run Match Producer).

Отвечает за:
1. Инициализацию SQLite (`raw_matches`, `producer_state`, `producer_config`);
2. Сериализацию стейта продюсера (UPSERT);
3. Динамическое чтение конфигурации из таблицы `producer_config` на каждой итерации;
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
    from src.clients.opendota import OpenDota
    from src.workers.match_producer import (
        BaseState,
        Collecting,
        DailyLimitExceeded,
        Idle,
        ProducerState,
        WaitForLimits,
        step,
        trim_raw_matches,
    )
except ImportError:
    import config  # type: ignore
    from opendota import OpenDota  # type: ignore
    from match_producer import (  # type: ignore
        BaseState,
        Collecting,
        DailyLimitExceeded,
        Idle,
        ProducerState,
        WaitForLimits,
        step,
        trim_raw_matches,
    )

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("producer_runner")


@dataclass
class ProducerConfig:
    """Конфигурация параметров продюсера, читаемая из SQLite."""

    running: bool = False
    api_key: Optional[str] = config.OPENDOTA_API_KEY
    min_rank: Optional[int] = config.PRODUCER_MIN_RANK
    max_rank: Optional[int] = config.PRODUCER_MAX_RANK
    min_duration: Optional[int] = None
    max_duration: Optional[int] = None
    lobby_type: Optional[int] = None
    game_mode: Optional[int] = None
    min_start_time: Optional[int] = None
    daily_request_limit: Optional[int] = None
    daily_requests_count: int = 0
    daily_requests_date: Optional[str] = None
    buffer_size: int = config.PRODUCER_BUFFER_SIZE
    delay: float = config.PRODUCER_DELAY
    idle_delay: float = config.PRODUCER_IDLE_DELAY
    limit_sleep: float = config.PRODUCER_LIMIT_SLEEP


def init_db(db_path: str = config.DB_PATH) -> sqlite3.Connection:
    """Инициализация SQLite: таблицы raw_matches, producer_state и producer_config."""
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
        # Буфер сырых матчей для обогатителя
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

        # Таблица сохранения стейта воркера
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS producer_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute("INSERT OR IGNORE INTO producer_state (id, state_json) VALUES (1, '{}')")

        # Таблица параметров продюсера и управления (running/pause)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS producer_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                running INTEGER NOT NULL DEFAULT 0,
                api_key TEXT,
                min_rank INTEGER,
                max_rank INTEGER,
                min_duration INTEGER DEFAULT NULL,
                max_duration INTEGER DEFAULT NULL,
                lobby_type INTEGER DEFAULT NULL,
                game_mode INTEGER DEFAULT NULL,
                min_start_time INTEGER DEFAULT NULL,
                buffer_size INTEGER NOT NULL DEFAULT {config.PRODUCER_BUFFER_SIZE},
                delay REAL NOT NULL DEFAULT {config.PRODUCER_DELAY},
                idle_delay REAL NOT NULL DEFAULT {config.PRODUCER_IDLE_DELAY},
                limit_sleep REAL NOT NULL DEFAULT {config.PRODUCER_LIMIT_SLEEP},
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO producer_config (
                id, running, api_key, min_rank, max_rank,
                buffer_size, delay, idle_delay, limit_sleep
            ) VALUES (1, 0, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                config.OPENDOTA_API_KEY,
                config.PRODUCER_MIN_RANK,
                config.PRODUCER_MAX_RANK,
                config.PRODUCER_BUFFER_SIZE,
                config.PRODUCER_DELAY,
                config.PRODUCER_IDLE_DELAY,
                config.PRODUCER_LIMIT_SLEEP,
            ),
        )

        # Автоматическая миграция недостающих колонок фильтрации
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(producer_config)").fetchall()}
        for col_name, col_type in [
            ("min_rank", "INTEGER"),
            ("max_rank", "INTEGER"),
            ("min_duration", "INTEGER"),
            ("max_duration", "INTEGER"),
            ("lobby_type", "INTEGER"),
            ("game_mode", "INTEGER"),
            ("min_start_time", "INTEGER"),
        ]:
            if col_name not in existing_cols:
                conn.execute(f"ALTER TABLE producer_config ADD COLUMN {col_name} {col_type} DEFAULT NULL")

    return conn


STOP_REQUESTED = False


def _signal_handler(signum: int, frame: Any) -> None:
    global STOP_REQUESTED
    logger.info("Получен сигнал остановки (%s). Завершаем текущий шаг...", signum)
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


def load_producer_config(conn: sqlite3.Connection) -> ProducerConfig:
    """Загрузить актуальные параметры продюсера из таблицы producer_config."""
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    row = cur.execute("SELECT * FROM producer_config WHERE id = 1").fetchone()
    if not row:
        return ProducerConfig()

    keys = row.keys()
    return ProducerConfig(
        running=bool(row["running"]),
        api_key=row["api_key"] if row["api_key"] else None,
        min_rank=int(row["min_rank"]) if "min_rank" in keys and row["min_rank"] is not None else None,
        max_rank=int(row["max_rank"]) if "max_rank" in keys and row["max_rank"] is not None else None,
        min_duration=int(row["min_duration"]) if "min_duration" in keys and row["min_duration"] is not None else None,
        max_duration=int(row["max_duration"]) if "max_duration" in keys and row["max_duration"] is not None else None,
        lobby_type=int(row["lobby_type"]) if "lobby_type" in keys and row["lobby_type"] is not None else None,
        game_mode=int(row["game_mode"]) if "game_mode" in keys and row["game_mode"] is not None else None,
        min_start_time=int(row["min_start_time"]) if "min_start_time" in keys and row["min_start_time"] is not None else None,
        daily_request_limit=int(row["daily_request_limit"]) if "daily_request_limit" in keys and row["daily_request_limit"] is not None else None,
        daily_requests_count=int(row["daily_requests_count"]) if "daily_requests_count" in keys and row["daily_requests_count"] is not None else 0,
        daily_requests_date=str(row["daily_requests_date"]) if "daily_requests_date" in keys and row["daily_requests_date"] else None,
        buffer_size=max(1, int(row["buffer_size"])) if row["buffer_size"] is not None else config.PRODUCER_BUFFER_SIZE,
        delay=max(0.0, float(row["delay"])) if row["delay"] is not None else config.PRODUCER_DELAY,
        idle_delay=max(0.1, float(row["idle_delay"])) if row["idle_delay"] is not None else config.PRODUCER_IDLE_DELAY,
        limit_sleep=max(0.1, float(row["limit_sleep"])) if row["limit_sleep"] is not None else config.PRODUCER_LIMIT_SLEEP,
    )


def update_producer_config(
    conn: sqlite3.Connection,
    running: Optional[bool] = None,
    api_key: Optional[str] = None,
    min_rank: Optional[int] = None,
    max_rank: Optional[int] = None,
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None,
    lobby_type: Optional[int] = None,
    game_mode: Optional[int] = None,
    min_start_time: Optional[int] = None,
    buffer_size: Optional[int] = None,
    delay: Optional[float] = None,
    idle_delay: Optional[float] = None,
    limit_sleep: Optional[float] = None,
) -> None:
    """Обновить параметры продюсера в таблице producer_config."""
    fields = []
    values = []

    if running is not None:
        fields.append("running = ?")
        values.append(1 if running else 0)
    if api_key is not None:
        fields.append("api_key = ?")
        values.append(api_key or None)
    if min_rank is not None:
        fields.append("min_rank = ?")
        values.append(min_rank)
    if max_rank is not None:
        fields.append("max_rank = ?")
        values.append(max_rank)
    if min_duration is not None:
        fields.append("min_duration = ?")
        values.append(min_duration)
    if max_duration is not None:
        fields.append("max_duration = ?")
        values.append(max_duration)
    if lobby_type is not None:
        fields.append("lobby_type = ?")
        values.append(lobby_type)
    if game_mode is not None:
        fields.append("game_mode = ?")
        values.append(game_mode)
    if min_start_time is not None:
        fields.append("min_start_time = ?")
        values.append(min_start_time)
    if buffer_size is not None:
        fields.append("buffer_size = ?")
        values.append(max(1, buffer_size))
    if delay is not None:
        fields.append("delay = ?")
        values.append(max(0.0, delay))
    if idle_delay is not None:
        fields.append("idle_delay = ?")
        values.append(max(0.1, idle_delay))
    if limit_sleep is not None:
        fields.append("limit_sleep = ?")
        values.append(max(0.1, limit_sleep))

    if not fields:
        return

    fields.append("updated_at = datetime('now')")
    query = f"UPDATE producer_config SET {', '.join(fields)} WHERE id = 1"
    with conn:
        conn.execute(query, tuple(values))


def load_producer_state(conn: sqlite3.Connection) -> ProducerState:
    """Загрузить стейт продюсера из БД."""
    row = conn.execute("SELECT state_json FROM producer_state WHERE id = 1").fetchone()
    if row and row[0]:
        return BaseState.from_json(row[0])
    return Idle()


def save_producer_state(conn: sqlite3.Connection, state: ProducerState) -> None:
    """Сериализовать стейт продюсера в SQLite (UPSERT)."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with conn:
        conn.execute(
            """
            INSERT INTO producer_state (id, state_json, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (state.to_json(), now_utc),
        )


def run_collector(db_path: str = config.DB_PATH) -> None:
    """Главный цикл сборщика: параметры читаются из producer_config на каждой итерации."""
    conn = init_db(db_path)
    client = OpenDota()

    state = load_producer_state(conn)
    logger.info("Старт сборщика. Начальный стейт: %s (курсор < %s)", state.status.value, state.last_match_id)

    was_running = False
    first_check = True

    try:
        while not STOP_REQUESTED:
            try:
                # 1. Читаем актуальные параметры из базы данных на каждой итерации
                cfg = load_producer_config(conn)

                # 2. Непрерывно обрезаем излишки буфера raw_matches, если они превышают лимит
                if cfg.buffer_size > 0:
                    trim_raw_matches(conn, cfg.buffer_size)

                # 3. Проверяем флаг running (пауза/пуск из админки)
                if not cfg.running:
                    if was_running or first_check:
                        logger.info("Продюсер на паузе (running=0 в producer_config). Ожидание запуска...")
                        was_running = False
                        first_check = False
                    if sleep_interruptible(cfg.idle_delay):
                        break
                    continue

                if not was_running:
                    logger.info("Продюсер возобновил работу (running=1).")
                    was_running = True
                    first_check = False

                # 3. Синхронизируем API-ключ клиента при его изменении в БД
                if cfg.api_key != client.api_key:
                    client.api_key = cfg.api_key
                    if cfg.api_key:
                        client.session.headers.update({"Authorization": f"Bearer {cfg.api_key}"})
                    else:
                        client.session.headers.pop("Authorization", None)

                # 4. Шаг стейт-машины с актуальными параметрами из БД
                state = step(
                    state=state,
                    client=client,
                    conn=conn,
                    buffer_size=cfg.buffer_size,
                    min_rank=cfg.min_rank,
                    max_rank=cfg.max_rank,
                    min_duration=cfg.min_duration,
                    max_duration=cfg.max_duration,
                    lobby_type=cfg.lobby_type,
                    game_mode=cfg.game_mode,
                    min_start_time=cfg.min_start_time,
                )

                # 5. Раннер сериализует обновлённый стейт
                save_producer_state(conn, state)

                if STOP_REQUESTED:
                    break

                # 6. Раннер контролирует сон снаружи через match state: с актуальными таймингами
                match state:
                    case Idle() | DailyLimitExceeded():
                        if sleep_interruptible(cfg.idle_delay):
                            break
                    case WaitForLimits():
                        logger.info("Сон по лимитам API: %.1f сек...", cfg.limit_sleep)
                        if sleep_interruptible(cfg.limit_sleep):
                            break
                    case Collecting():
                        if sleep_interruptible(cfg.delay):
                            break
            except Exception as loop_err:
                logger.exception("Непредвиденная ошибка в итерации сборщика: %s. Повтор через 2 сек...", loop_err)
                if sleep_interruptible(2.0):
                    break

    except KeyboardInterrupt:
        logger.info("Остановка сборщика пользователем.")
    finally:
        save_producer_state(conn, state)
        conn.close()
        client.close()
        logger.info("Сборщик остановлен.")


def main() -> None:
    """Точка входа CLI."""
    parser = argparse.ArgumentParser(description="Раннер сборщика матчей Dota 2.")
    parser.add_argument("--db", default=config.DB_PATH, help=f"Путь к SQLite (по умолчанию {config.DB_PATH})")
    args = parser.parse_args()

    run_collector(db_path=args.db)


if __name__ == "__main__":
    main()
