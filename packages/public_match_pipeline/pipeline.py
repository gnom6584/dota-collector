from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from packages.match_storage_writer import StorageTransaction, TransactionSource
from packages.opendota import PublicMatch


class PublicMatchPipeline(TransactionSource, ABC):
    """Базовый абстрактный контракт конвейера публичных матчей Dota 2.

    Архитектурные принципы:
    1. Изоляция фаз: разделяет прием сырых матчей (push), созревание (peek), фиксацию
       обогащения (commit) и последующую асинхронную вставку в целевое хранилище
       (get_pending_transaction, complete_transaction через контракт TransactionSource).
    2. Атомарность входного контура: метод push атомарно регистрирует новые матчи
       в активном регистре и очереди созревания, гарантируя откат и backpressure при переполнении.
    3. Однотранзакционный буфер передачи: метод commit принимает обогащенную пачку только
       при отсутствии незакрытой транзакции в буфере (обратное давление на воркер обогащения).
    4. Идемпотентность целевой вставки: транзакции вставки могут безопасно перезаписываться
       в целевое хранилище при сбоях и сетевых ретраях.
    """

    @property
    @abstractmethod
    def maturation_delay_seconds(self) -> float:
        """Актуальное время созревания матчей в секундах."""
        pass

    @abstractmethod
    def push(self, matches: list[PublicMatch]) -> bool:
        """Принимает сырые матчи: фильтрует по регистру и БД, атомарно регистрирует и ставит в очередь.

        Returns:
            bool: True при успехе, False при переполнении очереди (backpressure с полным откатом).
        """
        pass

    @abstractmethod
    def peek(self, limit: int) -> list[int]:
        """Возвращает пачку идентификаторов созревших матчей для обогащения."""
        pass

    @abstractmethod
    def commit(
        self,
        enriched: list[dict[str, Any]],
        failed: dict[int, str] | None = None,
    ) -> bool:
        """Фиксирует результаты обогащения: удаляет сбойные из регистра,
        создает транзакцию для вставщика и возвращает True.
        Если предыдущая транзакция еще не закрыта — возвращает False (backpressure).
        """
        pass

    @abstractmethod
    def get_pending_transaction(self) -> StorageTransaction | None:
        """Возвращает ожидающую транзакцию для записи в целевую БД или None при ее отсутствии."""
        pass

    @abstractmethod
    def complete_transaction(self, transaction_id: int) -> bool:
        """Подтверждает успешную запись транзакции: удаляет транзакцию и снимает матчи с учета в регистре."""
        pass
