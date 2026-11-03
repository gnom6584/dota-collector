from __future__ import annotations

import sqlite3
import time

from packages.quotas import DailyQuota


class SqliteDailyQuota(DailyQuota):
    """Высоконадежная суточная квота поверх SQLite с автоматическим сбросом в 00:00:00 UTC.

    Архитектура надежности:
    1. Календарная привязка: сброс окна рассчитывается строго по границе суток
       `((now // 86400) + 1) * 86400` (полночь UTC).
    2. Изоляция транзакций (BEGIN IMMEDIATE): конкурентные воркеры сериализуются
       на уровне write-лока базы, исключая перерасход квоты.
    3. Разделение контрактов: воркер использует только базовый метод `acquire()`,
       а методы инспекции (`remaining()`, `used()`, `reset_at()`) доступны оркестратору.

    Необходимый DDL:
    ```sql
    CREATE TABLE IF NOT EXISTS {table_name} (
        name TEXT PRIMARY KEY,
        used INTEGER NOT NULL,
        reset_at INTEGER NOT NULL
    );
    ```
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        name: str,
        limit: int,
        table_name: str = "daily_quotas",
    ) -> None:
        if limit <= 0:
            raise ValueError(f"Limit must be positive integer, got {limit}.")
        self.conn = conn
        self._name = name
        self._limit = limit
        self.table_name = table_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def limit(self) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT max_limit FROM "{self.table_name}" WHERE name = ?',
                (self.name,),
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None and row[0] > 0:
                return int(row[0])
            return self._limit
        except Exception:
            return self._limit
        finally:
            cursor.close()

    def set_limit(self, limit: int) -> None:
        """Обновляет максимальный лимит суточной квоты и сохраняет его в базе данных.

        Args:
            limit: Новое положительное целочисленное значение лимита.
        """
        if limit <= 0:
            raise ValueError(f"Limit must be positive integer, got {limit}.")
        self._limit = limit
        now = int(time.time())
        next_midnight = ((now // 86400) + 1) * 86400
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{self.table_name}" (name, used, reset_at, max_limit)
                VALUES (?, 0, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    max_limit = excluded.max_limit
                """,
                (self.name, next_midnight, limit),
            )
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def acquire(self, cost: int = 1) -> bool:
        if cost <= 0:
            return True
        current_limit = self.limit
        if cost > current_limit:
            return False

        now = int(time.time())
        next_midnight = ((now // 86400) + 1) * 86400
        cursor = self.conn.cursor()

        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f'SELECT used, reset_at, max_limit FROM "{self.table_name}" WHERE name = ?',
                (self.name,),
            )
            row = cursor.fetchone()

            if row is None:
                cursor.execute(
                    f'INSERT INTO "{self.table_name}" (name, used, reset_at, max_limit) VALUES (?, ?, ?, ?)',
                    (self.name, cost, next_midnight, self._limit),
                )
                cursor.execute("COMMIT")
                return True

            used = row[0]
            reset_at = row[1]
            stored_limit = row[2] if len(row) > 2 and row[2] is not None and row[2] > 0 else self._limit

            if now >= reset_at:
                cursor.execute(
                    f'UPDATE "{self.table_name}" SET used = ?, reset_at = ? WHERE name = ?',
                    (cost, next_midnight, self.name),
                )
                cursor.execute("COMMIT")
                return True

            if used + cost <= stored_limit:
                cursor.execute(
                    f'UPDATE "{self.table_name}" SET used = used + ? WHERE name = ?',
                    (cost, self.name),
                )
                cursor.execute("COMMIT")
                return True

            cursor.execute("COMMIT")
            return False

        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def remaining(self) -> int:
        """Возвращает остаток неиспользованной квоты на текущие календарные сутки UTC."""
        return max(0, self.limit - self.used())

    def used(self) -> int:
        """Возвращает израсходованный объем квоты за текущие сутки UTC."""
        now = int(time.time())
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT used, reset_at FROM "{self.table_name}" WHERE name = ?',
                (self.name,),
            )
            row = cursor.fetchone()
            if row is None or now >= row[1]:
                return 0
            return int(row[0])
        finally:
            cursor.close()

    def reset_at(self) -> int:
        """Возвращает Unix timestamp ближайшей полуночи 00:00:00 UTC."""
        now = int(time.time())
        return ((now // 86400) + 1) * 86400

    def reset(self) -> None:
        """Сбрасывает использованный объем квоты на текущие сутки UTC."""
        self.reset_name(conn=self.conn, name=self.name, table_name=self.table_name)

    @classmethod
    def reset_name(
        cls,
        conn: sqlite3.Connection,
        name: str,
        table_name: str = "daily_quotas",
    ) -> None:
        """Внешний сброс квоты по имени без необходимости создания экземпляра класса."""
        now = int(time.time())
        next_midnight = ((now // 86400) + 1) * 86400
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{table_name}" (name, used, reset_at)
                VALUES (?, 0, ?)
                ON CONFLICT(name) DO UPDATE SET
                    used = 0,
                    reset_at = excluded.reset_at
                """,
                (name, next_midnight),
            )
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
