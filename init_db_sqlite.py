from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from config import DatabaseConfig

logger = logging.getLogger("init_db_sqlite")


def init_database(config: DatabaseConfig | None = None) -> Path:
    """Инициализирует структуру таблиц и индексов в базе данных SQLite.

    Создает все системные таблицы:
    - очередь созревания публичных матчей (public_match_queue);
    - целевое хранилище обогащенных матчей (enriched_matches);
    - координация воркеров, стейты и ошибки (worker_control, worker_states, worker_errors);
    - суточные квоты внешних API (daily_quotas);
    - персистентные конфигурации воркеров (worker_configs).

    Args:
        config: Конфигурация базы данных. Если None, создается экземпляр DatabaseConfig().

    Returns:
        Path: Путь к инициализированному файлу базы данных SQLite.
    """
    cfg = config or DatabaseConfig()
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.lock_dir.mkdir(parents=True, exist_ok=True)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{cfg.queue_table}" (
        match_id INTEGER PRIMARY KEY,
        finished_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_{cfg.queue_table}_finished_at 
    ON "{cfg.queue_table}"(finished_at);

    CREATE TABLE IF NOT EXISTS "{cfg.storage_table}" (
        match_id INTEGER PRIMARY KEY,
        data_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS "{cfg.worker_control_table}" (
        worker_name TEXT PRIMARY KEY,
        worker_type TEXT,
        is_enabled INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'stopped',
        pid INTEGER,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS "{cfg.worker_states_table}" (
        worker_name TEXT PRIMARY KEY,
        state_json TEXT NOT NULL DEFAULT '{{}}',
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS "{cfg.worker_errors_table}" (
        worker_name TEXT PRIMARY KEY,
        error TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS "{cfg.daily_quotas_table}" (
        name TEXT PRIMARY KEY,
        used INTEGER NOT NULL,
        reset_at INTEGER NOT NULL,
        max_limit INTEGER
    );

    CREATE TABLE IF NOT EXISTS "{cfg.worker_configs_table}" (
        worker_name TEXT PRIMARY KEY,
        config_json TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS "{cfg.registry_table}" (
        match_id INTEGER PRIMARY KEY,
        registered_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS "{cfg.transaction_table}" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """

    conn = sqlite3.connect(cfg.db_path)
    try:
        cursor = conn.cursor()
        try:
            cursor.executescript(ddl)
            try:
                cursor.execute(f'ALTER TABLE "{cfg.daily_quotas_table}" ADD COLUMN max_limit INTEGER')
            except sqlite3.OperationalError:
                pass
            conn.commit()
        finally:
            cursor.close()
    finally:
        conn.close()

    logger.info("Initialized SQLite database schema at %s", cfg.db_path)
    return cfg.db_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db_file = init_database()
    print(f"Database schema initialized successfully at: {db_file}")
