from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Generic, TypeVar
from pydantic import BaseModel

from packages.config_store import ConfigStore

logger = logging.getLogger("packages.config_store_sqlite")

T = TypeVar("T", bound=BaseModel)


class SqliteConfigStore(ConfigStore[T], Generic[T]):
    """SQLite-реализация персистентного типобезопасного хранилища конфигураций.

    Архитектурные принципы:
    1. Изоляция транзакций (BEGIN IMMEDIATE): обеспечивает сериализацию параллельных записей
       и защиту от race conditions при обновлении конфигураций.
    2. Идемпотентная автоинициализация: при первом обращении сохраняет конфигурацию по умолчанию
       (INSERT OR IGNORE), если запись в БД отсутствует.
    3. Отказоустойчивость при десериализации: при повреждении JSON в базе данных логирует ошибку
       и возвращает дефолтный экземпляр конфигурации, предотвращая падение воркера.
    4. Инвариант внешнего владения соединением: класс не закрывает инжектированное соединение `conn`,
       поскольку оно управляется жизненным циклом вызывающего приложения.

    Необходимый DDL:
    ```sql
    CREATE TABLE IF NOT EXISTS {table_name} (
        worker_name TEXT PRIMARY KEY,
        config_json TEXT NOT NULL,
        updated_at TEXT DEFAULT (datetime('now'))
    );
    ```
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        key: str,
        model_cls: type[T],
        table_name: str,
        default: T | None = None,
    ) -> None:
        """Инициализирует SQLite-хранилище конфигурации.

        Args:
            conn: Активное подключение к базе данных SQLite.
            key: Уникальный строковый ключ записи (например, имя воркера).
            model_cls: Класс Pydantic-модели для валидации и парсинга JSON.
            table_name: Имя таблицы в SQLite для хранения конфигураций.
            default: Кастомный экземпляр конфигурации по умолчанию при инициализации.
        """
        self.conn = conn
        self.key = key
        self.model_cls = model_cls
        self.default = default if default is not None else model_cls()
        self.table_name = table_name

    def init_default(self) -> None:
        """Записывает конфигурацию по умолчанию в БД, если запись для ключа отсутствует."""
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO "{self.table_name}" (worker_name, config_json, updated_at)
                VALUES (?, ?, ?)
                """,
                (self.key, self.default.model_dump_json(), now_utc),
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

    def get(self) -> T:
        """Возвращает актуальный экземпляр конфигурации из SQLite.

        При отсутствии записи в таблице сохраняет дефолтную конфигурацию и возвращает её.
        При возникновении ошибки парсинга возвращает копию дефолтной конфигурации.

        Returns:
            T: Десериализованный и валидированный экземпляр Pydantic-модели.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT config_json FROM "{self.table_name}" WHERE worker_name = ?',
                (self.key,),
            )
            row = cursor.fetchone()
            if row is None or row[0] is None:
                self.init_default()
                return self.default.model_copy(deep=True)
            return self.model_cls.model_validate_json(row[0])
        except Exception as err:
            logger.error("Failed to read config for key '%s': %s", self.key, err)
            return self.default.model_copy(deep=True)
        finally:
            cursor.close()

    def set(self, config: T) -> None:
        """Сохраняет обновленную конфигурацию в таблицу SQLite.

        Args:
            config: Экземпляр конфигурации для сохранения.
        """
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        config_json = config.model_dump_json()
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{self.table_name}" (worker_name, config_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(worker_name) DO UPDATE SET
                    config_json = excluded.config_json,
                    updated_at = excluded.updated_at
                """,
                (self.key, config_json, now_utc),
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

    def close(self) -> None:
        """Завершает работу с хранилищем."""
        pass
