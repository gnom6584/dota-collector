from __future__ import annotations

import logging
from collections.abc import Callable
from pydantic import BaseModel

from packages.match_storage import Storage
from packages.workers import Worker

from .config import MatchStorageWriterConfig
from .transaction_source import TransactionSource

logger = logging.getLogger("packages.match_storage_writer.worker")


class MatchStorageWriterState(BaseModel):
    """Персистентное состояние воркера match_storage_writer."""

    last_transaction_id: int | None = None
    total_saved_matches: int = 0


class MatchStorageWriter(Worker[MatchStorageWriterState]):
    """Фоновый воркер асинхронной фиксации транзакций матчей в целевом хранилище.

    Архитектурные принципы:
    1. Изоляция источника транзакций (DIP): взаимодействует с источником исключительно
       через абстрактный контракт `TransactionSource`, не имея зависимости от конвейера публичных матчей.
    2. Идемпотентность и отказоустойчивость: запись выполняется через `Storage.insert_many()`.
       При любом сбое в хранилище транзакция не закрывается в источнике и автоматически
       повторяется на следующем цикле.
    3. Двухфазная фиксация без утечек: подтверждение транзакции (`complete_transaction`)
       вызывается строго после успешного ответа целевого хранилища, гарантируя атомарность
       перехода матча из буфера в постоянное хранилище.
    """

    name = "match_storage_writer"

    def __init__(
        self,
        source: TransactionSource,
        storage: Storage,
        config_factory: Callable[[], MatchStorageWriterConfig],
    ) -> None:
        self.source = source
        self.storage = storage
        self.config_factory = config_factory

    def execute(
        self,
        state: MatchStorageWriterState,
    ) -> tuple[MatchStorageWriterState, float]:
        config = self.config_factory()

        tx = self.source.get_pending_transaction()
        if tx is None:
            logger.info("[%s] No pending transactions available. Idling for %.1fs.", self.name, config.idle_delay)
            return state, config.idle_delay

        if not tx.matches:
            self.source.complete_transaction(tx.id)
            return state, config.cooldown

        try:
            saved = self.storage.insert_many(tx.matches)
        except Exception as err:
            logger.error("[%s] Unexpected error writing transaction %d to storage: %s", self.name, tx.id, err)
            return state, config.error_delay

        if not saved:
            logger.warning(
                "[%s] Storage rejected batch of %d matches in transaction %d. Retrying in %.1fs.",
                self.name,
                len(tx.matches),
                tx.id,
                config.error_delay,
            )
            return state, config.error_delay

        if not self.source.complete_transaction(tx.id):
            logger.error(
                "[%s] Failed to complete transaction %d in source. Retrying in %.1fs.",
                self.name,
                tx.id,
                config.error_delay,
            )
            return state, config.error_delay

        logger.info(
            "[%s] Successfully saved transaction %d (%d matches) into storage.",
            self.name,
            tx.id,
            len(tx.matches),
        )

        new_state = MatchStorageWriterState(
            last_transaction_id=tx.id,
            total_saved_matches=state.total_saved_matches + len(tx.matches),
        )
        return new_state, config.cooldown

    def close(self) -> None:
        pass
