from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from packages.workers import Backend, WorkerStatus


def _serialize_state(state: Any) -> str:
    if state is None:
        return "{}"
    if hasattr(state, "model_dump_json") and callable(state.model_dump_json):
        return str(state.model_dump_json())
    if is_dataclass(state) and not isinstance(state, type):
        return json.dumps(asdict(state), default=str)
    if isinstance(state, str):
        try:
            json.loads(state)
            return state
        except Exception:
            return json.dumps(state)
    try:
        return json.dumps(state, default=str)
    except Exception:
        return str(state)


def _deserialize_state(raw: str | None, state_cls: Any = None) -> Any:
    if raw is None or raw == "":
        return None

    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = raw

    if state_cls is not None:
        if hasattr(state_cls, "model_validate"):
            try:
                return state_cls.model_validate(parsed)
            except Exception:
                pass
        if hasattr(state_cls, "parse_raw") and isinstance(raw, str):
            try:
                return state_cls.parse_raw(raw)
            except Exception:
                pass
        if callable(state_cls):
            try:
                return state_cls(**parsed) if isinstance(parsed, dict) else state_cls(parsed)
            except Exception:
                pass

    return parsed


class SqliteBackend(Backend):
    """SQLite-реализация бэкенда воркеров через инжектированное соединение.

    Внимание:
    Таблицы схемы ДОЛЖНЫ быть созданы заранее до запуска воркера (через миграции
    или скрипт инициализации БД). Класс не выполняет неявных DDL-запросов в рантайме.

    Необходимый DDL (с учетом дополнительного префикса `prefix`, по умолчанию `prefix=''`):
    ```sql
    CREATE TABLE IF NOT EXISTS {prefix}worker_control (
        worker_name TEXT PRIMARY KEY,
        worker_type TEXT,
        is_enabled INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'stopped',
        pid INTEGER,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS {prefix}worker_errors (
        worker_name TEXT PRIMARY KEY,
        error TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS {prefix}worker_states (
        worker_name TEXT PRIMARY KEY,
        state_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT DEFAULT (datetime('now'))
    );
    ```

    Управление соединением:
    - Экземпляр `sqlite3.Connection` создается и настраивается (WAL, timeouts) снаружи.
    - Если соединение используется между разными потоками, оно должно быть открыто
      с флагом `check_same_thread=False`.
    """

    def __init__(self, conn: sqlite3.Connection, prefix: str = "") -> None:
        self.conn = conn
        self.table_control = f"{prefix}worker_control"
        self.table_errors = f"{prefix}worker_errors"
        self.table_states = f"{prefix}worker_states"

    def is_enabled(self, worker_name: str) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT is_enabled FROM "{self.table_control}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            return bool(row[0]) if row is not None else False
        finally:
            cursor.close()

    def register_worker(self, worker_name: str, worker_type: str) -> None:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f'SELECT worker_type FROM "{self.table_control}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()

            if row is None:
                cursor.execute(
                    f"""
                    INSERT INTO "{self.table_control}" (worker_name, worker_type, is_enabled, updated_at)
                    VALUES (?, ?, 0, ?)
                    """,
                    (worker_name, worker_type, now_utc),
                )
                cursor.execute("COMMIT")
                return

            existing_type = row[0]
            if existing_type is None:
                cursor.execute(
                    f"""
                    UPDATE "{self.table_control}"
                    SET worker_type = ?, updated_at = ?
                    WHERE worker_name = ?
                    """,
                    (worker_type, now_utc, worker_name),
                )
                cursor.execute("COMMIT")
                return

            if existing_type != worker_type:
                cursor.execute("ROLLBACK")
                raise TypeError(
                    f"Worker identity mismatch for '{worker_name}': "
                    f"registered in database as '{existing_type}', but instantiated with '{worker_type}'."
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

    def enable(self, worker_name: str) -> None:
        self._set_control(self.conn, self.table_control, worker_name, is_enabled=True)

    def disable(self, worker_name: str) -> None:
        self._set_control(self.conn, self.table_control, worker_name, is_enabled=False)

    def set_status(
        self,
        worker_name: str,
        status: WorkerStatus,
        pid: int | None = None,
    ) -> None:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        status_str = status.value
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{self.table_control}" (worker_name, status, pid, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(worker_name) DO UPDATE SET
                    status = excluded.status,
                    pid = excluded.pid,
                    updated_at = excluded.updated_at
                """,
                (worker_name, status_str, pid, now_utc),
            )
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def get_status(self, worker_name: str) -> WorkerStatus:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT status FROM "{self.table_control}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            if row is None or not row[0]:
                return WorkerStatus.STOPPED
            return WorkerStatus(row[0])
        finally:
            cursor.close()

    @classmethod
    def enable_worker(cls, conn: sqlite3.Connection, worker_name: str, prefix: str = "") -> None:
        """Внешняя активация воркера по имени без создания экземпляра класса."""
        table = f"{prefix}worker_control"
        cls._set_control(conn, table, worker_name, is_enabled=True)

    @classmethod
    def disable_worker(cls, conn: sqlite3.Connection, worker_name: str, prefix: str = "") -> None:
        """Внешняя деактивация воркера по имени без создания экземпляра класса."""
        table = f"{prefix}worker_control"
        cls._set_control(conn, table, worker_name, is_enabled=False)

    @classmethod
    def get_worker_status(cls, conn: sqlite3.Connection, worker_name: str, prefix: str = "") -> WorkerStatus:
        """Внешнее получение статуса выполнения воркера."""
        table = f"{prefix}worker_control"
        cursor = conn.cursor()
        try:
            cursor.execute(
                f'SELECT status FROM "{table}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            if row is None or not row[0]:
                return WorkerStatus.STOPPED
            return WorkerStatus(row[0])
        finally:
            cursor.close()

    @classmethod
    def get_worker_pid(cls, conn: sqlite3.Connection, worker_name: str, prefix: str = "") -> int | None:
        """Внешнее получение PID процесса воркера."""
        table = f"{prefix}worker_control"
        cursor = conn.cursor()
        try:
            cursor.execute(
                f'SELECT pid FROM "{table}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            return int(row[0]) if row and row[0] is not None else None
        finally:
            cursor.close()

    @staticmethod
    def _set_control(
        conn: sqlite3.Connection,
        table: str,
        worker_name: str,
        is_enabled: bool,
    ) -> None:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{table}" (worker_name, is_enabled, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(worker_name) DO UPDATE SET
                    is_enabled = excluded.is_enabled,
                    updated_at = excluded.updated_at
                """,
                (worker_name, 1 if is_enabled else 0, now_utc),
            )
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def get_state(self, worker_name: str, state_cls: Any = None) -> Any:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT state_json FROM "{self.table_states}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return _deserialize_state(row[0], state_cls=state_cls)
        finally:
            cursor.close()

    def set_state(self, worker_name: str, state: Any) -> None:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        state_json = _serialize_state(state)
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{self.table_states}" (worker_name, state_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(worker_name) DO UPDATE SET
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (worker_name, state_json, now_utc),
            )
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def set_error(self, worker_name: str, error: str) -> None:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{self.table_errors}" (worker_name, error, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(worker_name) DO UPDATE SET
                    error = excluded.error,
                    updated_at = excluded.updated_at
                """,
                (worker_name, error, now_utc),
            )
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def clear_error(self, worker_name: str) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f'DELETE FROM "{self.table_errors}" WHERE worker_name = ?',
                (worker_name,),
            )
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def get_error(self, worker_name: str) -> str | None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT error FROM "{self.table_errors}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            return str(row[0]) if row and row[0] is not None else None
        finally:
            cursor.close()

    def get_pid(self, worker_name: str) -> int | None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT pid FROM "{self.table_control}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            return int(row[0]) if row and row[0] is not None else None
        finally:
            cursor.close()

    def get_worker_type(self, worker_name: str) -> str | None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT worker_type FROM "{self.table_control}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            return str(row[0]) if row and row[0] is not None else None
        finally:
            cursor.close()

    def get_updated_at(self, worker_name: str) -> str | None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT updated_at FROM "{self.table_control}" WHERE worker_name = ?',
                (worker_name,),
            )
            row = cursor.fetchone()
            return str(row[0]) if row and row[0] is not None else None
        finally:
            cursor.close()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
