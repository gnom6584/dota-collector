from __future__ import annotations

import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger("packages.public_match_pipeline.control")


class PipelineControl(ABC):
    """Базовый абстрактный контракт управления и мониторинга параметров конвейера публичных матчей.

    Архитектурные принципы:
    1. Разделение обязанностей (CQS/CQRS): изолирует администрирование, динамическую настройку
       (лимит очереди, задержка созревания) и телеметрию от высоконагруженного конвейера данных.
    2. Минимизация зависимостей: компоненты панели управления (ControlPlane) и фоновые контроллеры
       взаимодействуют с параметрами конвейера без зависимости от тяжеловесной реализации Data Plane.
    3. Атомарное принудительное усечение: при уменьшении вместимости очереди гарантирует
       немедленную очистку избытка из очереди и снятие с учета в активном регистре.
    """

    @abstractmethod
    def get_max_queue_size(self) -> int:
        """Возвращает максимальную вместимость очереди созревания."""
        pass

    @abstractmethod
    def set_max_queue_size(self, max_size: int) -> None:
        """Устанавливает максимальную вместимость очереди и усекает избыточные записи."""
        pass

    @abstractmethod
    def get_maturation_delay(self) -> float:
        """Возвращает время созревания матчей перед обогащением в секундах."""
        pass

    @abstractmethod
    def set_maturation_delay(self, seconds: float) -> None:
        """Обновляет задержку созревания матчей в секундах."""
        pass

    @abstractmethod
    def get_queue_count(self) -> int:
        """Возвращает текущее количество записей в очереди созревания."""
        pass


class SqlitePipelineControl(PipelineControl):
    """SQLite-реализация фасада управления и мониторинга конвейера публичных матчей.

    Архитектурные принципы:
    1. Персистентность без лишних сущностей: сохраняет параметры конвейера (max_size, maturation_delay)
       в таблице конфигураций под ключом 'public_match_pipeline'.
    2. Безопасное пакетное усечение (Anti-Overflow): удаление избытка записей при сокращении лимита
       выполняется порциями по 500 элементов, предотвращая превышение лимита переменных SQLite.
    3. Синхронная очистка регистра: при вытеснении записей из очереди они немедленно удаляются
       из активного регистра в единой транзакции BEGIN IMMEDIATE, исключая утечку идентификаторов.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        default_max_size: int = 30_000,
        default_maturation_delay: float = 21600.0,
        queue_table: str = "public_match_queue",
        registry_table: str = "active_match_registry",
        config_table: str = "worker_configs",
    ) -> None:
        self.conn = conn
        self.default_max_size = default_max_size
        self.default_maturation_delay = default_maturation_delay
        self.queue_table = queue_table
        self.registry_table = registry_table
        self.config_table = config_table
        self._config_key = "public_match_pipeline"

    def _read_params(self) -> dict[str, Any]:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT config_json FROM "{self.config_table}" WHERE worker_name = ?',
                (self._config_key,),
            )
            row = cursor.fetchone()
            if row and row[0]:
                return json.loads(row[0])
            return {}
        except Exception as err:
            logger.error("Failed to read pipeline control params: %s", err)
            return {}
        finally:
            cursor.close()

    def _save_params(self, params: dict[str, Any]) -> None:
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f"""
                INSERT INTO "{self.config_table}" (worker_name, config_json, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(worker_name) DO UPDATE SET
                    config_json = excluded.config_json,
                    updated_at = excluded.updated_at
                """,
                (self._config_key, json.dumps(params)),
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

    def get_max_queue_size(self) -> int:
        data = self._read_params()
        val = data.get("max_size")
        if val is not None and int(val) > 0:
            return int(val)
        return self.default_max_size

    def set_max_queue_size(self, max_size: int) -> None:
        if max_size <= 0:
            raise ValueError(f"Queue max_size must be positive integer, got {max_size}.")
        params = self._read_params()
        params["max_size"] = int(max_size)
        params.setdefault("maturation_delay_seconds", self.default_maturation_delay)
        self._save_params(params)
        self.enforce_capacity(max_size)

    def get_maturation_delay(self) -> float:
        data = self._read_params()
        val = data.get("maturation_delay_seconds")
        if val is not None and float(val) >= 0:
            return float(val)
        return self.default_maturation_delay

    def set_maturation_delay(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError(f"Maturation delay must be non-negative, got {seconds}.")
        params = self._read_params()
        params["maturation_delay_seconds"] = float(seconds)
        params.setdefault("max_size", self.default_max_size)
        self._save_params(params)

    def enforce_capacity(self, max_size: int, cursor: sqlite3.Cursor | None = None) -> int:
        """Усекает избыточные записи из очереди и регистра, если их количество превышает max_size.

        Args:
            max_size: Максимально допустимое количество записей в очереди.
            cursor: Опциональный внешний транзакционный курсор (BEGIN IMMEDIATE).

        Returns:
            int: Количество удаленных избыточных записей.
        """
        own_cursor = cursor is None
        cur = self.conn.cursor() if own_cursor else cursor
        pruned_total = 0
        try:
            if own_cursor:
                cur.execute("BEGIN IMMEDIATE")

            cur.execute(f'SELECT COUNT(*) FROM "{self.queue_table}"')
            current_count = int(cur.fetchone()[0])

            if current_count > max_size:
                excess = current_count - max_size
                cur.execute(
                    f'SELECT match_id FROM "{self.queue_table}" ORDER BY finished_at DESC LIMIT ?',
                    (excess,),
                )
                pruned_ids = [row[0] for row in cur.fetchall()]
                pruned_total = len(pruned_ids)

                chunk_size = 500
                for i in range(0, len(pruned_ids), chunk_size):
                    chunk = pruned_ids[i : i + chunk_size]
                    placeholders = ",".join("?" for _ in chunk)
                    cur.execute(
                        f'DELETE FROM "{self.queue_table}" WHERE match_id IN ({placeholders})',
                        chunk,
                    )
                    cur.execute(
                        f'DELETE FROM "{self.registry_table}" WHERE match_id IN ({placeholders})',
                        chunk,
                    )

                logger.warning(
                    "Queue capacity trimmed: removed %d excess matches (current=%d, max=%d).",
                    pruned_total,
                    current_count,
                    max_size,
                )

            if own_cursor:
                cur.execute("COMMIT")
            return pruned_total
        except Exception as err:
            if own_cursor:
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
            logger.error("Failed to enforce queue capacity: %s", err)
            raise
        finally:
            if own_cursor:
                cur.close()

    def get_queue_count(self) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{self.queue_table}"')
            return int(cursor.fetchone()[0])
        except Exception as err:
            logger.error("Failed to count queue records: %s", err)
            return 0
        finally:
            cursor.close()
