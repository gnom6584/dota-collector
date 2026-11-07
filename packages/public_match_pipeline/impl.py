from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any

from packages.match_storage_writer import StorageTransaction
from packages.opendota import PublicMatch

from .control import SqlitePipelineControl
from .pipeline import PublicMatchPipeline

logger = logging.getLogger("packages.public_match_pipeline.impl")


class PublicMatchPipelineImpl(PublicMatchPipeline):
    """SQLite-реализация конвейера публичных матчей Dota 2.

    Архитектурные принципы:
    1. Инкапсуляция состояний Data Plane: закрывает внутри единого класса работу с таблицами
       активного регистра (`active_match_registry`), очереди созревания (`public_match_queue`)
       и буфера исходящих транзакций (`pipeline_transactions`).
    2. Атомарность входного барьера: в транзакции `BEGIN IMMEDIATE` проверяет наличие
       матчей в регистре и целевой БД, регистрирует новые и кладет в очередь.
       При переполнении очереди откатывает регистрацию полностью (обратное давление без утечек).
    3. Однотранзакционный буфер передачи: фиксация обогащенных матчей разрешена
       только при отсутствии незакрытых транзакций в буфере, исключая переполнение памяти.
    4. Делегирование управления: параметры созревания, лимит вместимости и усечение очереди
       делегируются сущности `PipelineControl`.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        control: SqlitePipelineControl,
        queue_table: str = "public_match_queue",
        storage_table: str = "enriched_matches",
        registry_table: str = "active_match_registry",
        transaction_table: str = "pipeline_transactions",
        batch_wait_seconds: float = 300.0,
    ) -> None:
        self.conn = conn
        self.control = control
        self.queue_table = queue_table
        self.storage_table = storage_table
        self.registry_table = registry_table
        self.transaction_table = transaction_table
        self.batch_wait_seconds = batch_wait_seconds
        self._wait_started_at: float | None = None

    @property
    def maturation_delay_seconds(self) -> float:
        return self.control.get_maturation_delay()

    def push(self, matches: list[PublicMatch]) -> bool:
        if not matches:
            return True

        match_map = {m.match_id: m for m in matches}
        incoming_ids = list(match_map.keys())

        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")

            max_size = self.control.get_max_queue_size()
            if hasattr(self.control, "enforce_capacity"):
                self.control.enforce_capacity(max_size, cursor=cursor)

            existing_ids: set[int] = set()
            chunk_size = 400
            for i in range(0, len(incoming_ids), chunk_size):
                chunk = incoming_ids[i : i + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                cursor.execute(
                    f'SELECT match_id FROM "{self.registry_table}" WHERE match_id IN ({placeholders}) '
                    f'UNION '
                    f'SELECT match_id FROM "{self.storage_table}" WHERE match_id IN ({placeholders})',
                    chunk * 2,
                )
                for row in cursor.fetchall():
                    existing_ids.add(row[0])

            new_ids = [mid for mid in incoming_ids if mid not in existing_ids]

            if not new_ids:
                cursor.execute("COMMIT")
                return True

            cursor.execute(f'SELECT COUNT(*) FROM "{self.queue_table}"')
            current_queue_count = int(cursor.fetchone()[0])

            if current_queue_count + len(new_ids) > max_size:
                cursor.execute("ROLLBACK")
                logger.warning(
                    "Queue capacity exceeded (current=%d, new=%d, max=%d). Rejecting push (backpressure).",
                    current_queue_count,
                    len(new_ids),
                    max_size,
                )
                return False

            cursor.executemany(
                f'INSERT OR IGNORE INTO "{self.registry_table}" (match_id) VALUES (?)',
                [(mid,) for mid in new_ids],
            )
            queue_records = [
                (mid, match_map[mid].start_time + match_map[mid].duration)
                for mid in new_ids
            ]
            cursor.executemany(
                f'INSERT OR IGNORE INTO "{self.queue_table}" (match_id, finished_at) VALUES (?, ?)',
                queue_records,
            )

            cursor.execute("COMMIT")
            logger.info(
                "Pushed %d new matches to pipeline (filtered %d duplicates, queue_total=%d).",
                len(new_ids),
                len(incoming_ids) - len(new_ids),
                current_queue_count + len(new_ids),
            )
            return True
        except Exception as err:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            logger.error("Failed to push matches to pipeline: %s", err)
            return False
        finally:
            cursor.close()

    def peek(self, limit: int) -> list[int]:
        if limit <= 0:
            return []

        now = time.time()

        # Если включен режим спячки и время еще не вышло — ничего не отдаем
        if self._wait_started_at is not None:
            if (now - self._wait_started_at) < self.batch_wait_seconds:
                return []

        max_finished_at = int(now - self.maturation_delay_seconds)
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT match_id FROM "{self.queue_table}"
                WHERE finished_at <= ?
                ORDER BY finished_at ASC
                LIMIT ?
                """,
                (max_finished_at, limit),
            )
            matches = [row[0] for row in cursor.fetchall()]

            # 1. Созревших матчей нет вообще — сбрасываем таймер
            if not matches:
                self._wait_started_at = None
                return []

            # 2. Набрали полный батч — сбрасываем таймер и сразу отдаем
            if len(matches) >= limit:
                self._wait_started_at = None
                return matches

            # 3. Не добрали до limit, но еще не спали — уходим в спячку
            if self._wait_started_at is None:
                self._wait_started_at = now
                logger.debug(
                    "Mature matches (%d) < limit (%d). Entering sleep mode for %.1fs.",
                    len(matches),
                    limit,
                    self.batch_wait_seconds,
                )
                return []

            # 4. Время спячки истекло — сбрасываем таймер и отдаем сколько есть
            logger.debug(
                "Sleep mode expired. Returning partial batch of %d/%d matches.",
                len(matches),
                limit,
            )
            self._wait_started_at = None
            return matches

        except Exception as err:
            logger.error("Failed to peek mature matches: %s", err)
            return []
        finally:
            cursor.close()

    def commit(
        self,
        enriched: list[dict[str, Any]],
        failed: dict[int, str] | None = None,
    ) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")

            if failed:
                failed_ids = [(int(fid),) for fid in failed.keys()]
                cursor.executemany(
                    f'DELETE FROM "{self.registry_table}" WHERE match_id = ?',
                    failed_ids,
                )
                cursor.executemany(
                    f'DELETE FROM "{self.queue_table}" WHERE match_id = ?',
                    failed_ids,
                )

            if enriched:
                cursor.execute(f'SELECT id FROM "{self.transaction_table}" LIMIT 1')
                if cursor.fetchone() is not None:
                    cursor.execute("ROLLBACK")
                    logger.warning("Active transaction already in progress. Rejecting commit (backpressure).")
                    return False

                payload_str = json.dumps(enriched, ensure_ascii=False)
                cursor.execute(
                    f'INSERT INTO "{self.transaction_table}" (payload_json, created_at) VALUES (?, datetime(\'now\'))',
                    (payload_str,),
                )

                enriched_ids = []
                for m in enriched:
                    mid = m.get("match_id") or m.get("id")
                    if mid is not None:
                        enriched_ids.append((int(mid),))

                if enriched_ids:
                    cursor.executemany(
                        f'DELETE FROM "{self.queue_table}" WHERE match_id = ?',
                        enriched_ids,
                    )

            cursor.execute("COMMIT")
            self._wait_started_at = None
            return True
        except Exception as err:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            logger.error("Failed to commit enrichment in pipeline: %s", err)
            return False
        finally:
            cursor.close()

    def get_pending_transaction(self) -> StorageTransaction | None:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f'SELECT id, payload_json, created_at FROM "{self.transaction_table}" ORDER BY id ASC LIMIT 1'
            )
            row = cursor.fetchone()
            if not row:
                return None
            return StorageTransaction(
                id=row[0],
                matches=json.loads(row[1]),
                created_at=row[2],
            )
        except Exception as err:
            logger.error("Failed to read pending transaction: %s", err)
            return None
        finally:
            cursor.close()

    def complete_transaction(self, transaction_id: int) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                f'SELECT payload_json FROM "{self.transaction_table}" WHERE id = ?',
                (transaction_id,),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute("COMMIT")
                return True

            matches_data = json.loads(row[0])
            match_ids = []
            for m in matches_data:
                mid = m.get("match_id") or m.get("id")
                if mid is not None:
                    match_ids.append((int(mid),))

            cursor.execute(
                f'DELETE FROM "{self.transaction_table}" WHERE id = ?',
                (transaction_id,),
            )
            if match_ids:
                cursor.executemany(
                    f'DELETE FROM "{self.registry_table}" WHERE match_id = ?',
                    match_ids,
                )

            cursor.execute("COMMIT")
            logger.info(
                "Completed transaction %d (released %d matches from active registry).",
                transaction_id,
                len(match_ids),
            )
            return True
        except Exception as err:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            logger.error("Failed to complete transaction %d: %s", transaction_id, err)
            return False
        finally:
            cursor.close()