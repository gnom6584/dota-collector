from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from packages.match_storage import Storage

logger = logging.getLogger("packages.match_storage_sqlite")


class SqliteStorage(Storage):
    """SQLite-реализация целевого хранилища обогащенных матчей Dota 2.

    Архитектурные принципы:
    1. Изоляция транзакций: запись пачки матчей выполняется в атомарной транзакции
       (BEGIN IMMEDIATE ... COMMIT). При любой ошибке выполняется ROLLBACK и возвращается False.
    2. Скользящее кольцевое окно (FIFO-вытеснение): хранилище поддерживает лимит `max_matches`.
       При достижении предела старые матчи (с наименьшими match_id) автоматически удаляются,
       обеспечивая непрерывное хранение наиболее актуального среза игр Dota 2.
    3. Отсутствие неявного DDL: класс не создает таблицы на лету в рантайме. Схема БД
       должна быть инициализирована заранее миграциями или скриптом деплоя.

    Схема DDL:
    ```sql
    CREATE TABLE IF NOT EXISTS {table_name} (
        match_id INTEGER PRIMARY KEY,
        data_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );
    ```
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        max_matches: int,
        table_name: str = "enriched_matches",
        config_table_name: str = "worker_configs",
    ) -> None:
        """Инициализирует SQLite-хранилище обогащенных матчей.

        Args:
            conn: Активное подключение к базе данных SQLite.
            max_matches: Максимально допустимое количество матчей в таблице по умолчанию.
            table_name: Имя целевой таблицы (по умолчанию 'enriched_matches').
            config_table_name: Имя таблицы хранения персистентных настроек (по умолчанию 'worker_configs').
        """
        if max_matches <= 0:
            raise ValueError(f"Storage max_matches must be positive integer, got {max_matches}.")
        self.conn = conn
        self._max_matches = max_matches
        self.table_name = table_name
        self.config_table_name = config_table_name

    @property
    def max_matches(self) -> int:
        """Возвращает актуальную максимальную вместимость хранилища из базы данных."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT config_json FROM "{self.config_table_name}" WHERE worker_name = ?',
                ("match_storage",),
            )
            row = cursor.fetchone()
            if row and row[0]:
                data = json.loads(row[0])
                val = data.get("max_matches")
                if val and int(val) > 0:
                    return int(val)
            return self._max_matches
        except Exception:
            return self._max_matches
        finally:
            cursor.close()

    def set_max_matches(self, max_matches: int) -> None:
        """Сохраняет новую максимальную вместимость хранилища в базу данных."""
        if max_matches <= 0:
            raise ValueError(f"Storage max_matches must be positive integer, got {max_matches}.")
        self._max_matches = max_matches
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{self.config_table_name}" (worker_name, config_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(worker_name) DO UPDATE SET
                    config_json = excluded.config_json,
                    updated_at = excluded.updated_at
                """,
                ("match_storage", json.dumps({"max_matches": max_matches}), now_utc),
            )
            cursor.execute("COMMIT")
        except Exception:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            cursor.close()

    def count(self) -> int:
        """Возвращает текущее количество сохраненных матчей в таблице.

        Returns:
            int: Число записей в целевой таблице.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        finally:
            cursor.close()

    def get_free_space(self) -> int:
        """Возвращает свободную вместимость хранилища в количестве матчей.

        Returns:
            int: Допустимое количество матчей до достижения лимита max_matches.
        """
        try:
            return max(0, self.max_matches - self.count())
        except Exception as err:
            logger.error("Failed to calculate free space: %s", err)
            return 0

    def insert_many(self, matches: list[dict[str, Any]]) -> bool:
        """Сохраняет пачку обогащенных данных матчей в SQLite с вытеснением старых записей.

        Поддерживает выполнение как в автономном режиме, так и в рамках внешнего Unit of Work.
        При достижении предела max_matches удаляет наиболее старые матчи (с наименьшими match_id),
        освобождая место под новые данные без отклонения записи.

        Args:
            matches: Список словарей с полными данными обогащенных матчей.

        Returns:
            bool: True при успешной записи всей пачки, False при системной ошибке СУБД.
        """
        if not matches:
            return True

        records_map: dict[int, str] = {}
        for m in matches:
            mid = m.get("match_id") or m.get("id")
            if mid is None:
                logger.error("Match record is missing 'match_id' or 'id': %s", m)
                return False
            records_map[int(mid)] = json.dumps(m, ensure_ascii=False)

        max_allowed = self.max_matches
        sorted_records = sorted(records_map.items(), key=lambda r: r[0])
        if len(sorted_records) > max_allowed:
            sorted_records = sorted_records[-max_allowed:]
        records = sorted_records

        cursor = self.conn.cursor()
        is_nested = self.conn.in_transaction
        try:
            if not is_nested:
                try:
                    cursor.execute("BEGIN IMMEDIATE")
                except sqlite3.OperationalError:
                    is_nested = True

            cursor.execute(f'SELECT COUNT(*) FROM "{self.table_name}"')
            row = cursor.fetchone()
            current_count = int(row[0]) if row else 0

            unique_ids = [r[0] for r in records]
            placeholders = ",".join("?" for _ in unique_ids)
            cursor.execute(
                f'SELECT COUNT(*) FROM "{self.table_name}" WHERE match_id IN ({placeholders})',
                unique_ids,
            )
            existing_row = cursor.fetchone()
            existing_count = int(existing_row[0]) if existing_row else 0
            net_new_count = len(unique_ids) - existing_count

            excess = (current_count + net_new_count) - max_allowed
            if excess > 0:
                logger.info(
                    "Storage capacity reached (%d + %d > %d). Evicting %d oldest matches.",
                    current_count,
                    net_new_count,
                    max_allowed,
                    excess,
                )
                cursor.execute(
                    f"""
                    DELETE FROM "{self.table_name}"
                    WHERE match_id IN (
                        SELECT match_id FROM "{self.table_name}"
                        WHERE match_id NOT IN ({placeholders})
                        ORDER BY match_id ASC
                        LIMIT ?
                    )
                    """,
                    (*unique_ids, excess),
                )

            cursor.executemany(
                f'INSERT OR REPLACE INTO "{self.table_name}" (match_id, data_json) VALUES (?, ?)',
                records,
            )
            if not is_nested:
                cursor.execute("COMMIT")
            return True
        except Exception as err:
            if not is_nested:
                try:
                    cursor.execute("ROLLBACK")
                except Exception:
                    pass
            logger.error("Failed to insert matches into SQLite storage: %s", err)
            return False
        finally:
            cursor.close()

    def close(self) -> None:
        """Освобождает дескрипторы."""
        pass
