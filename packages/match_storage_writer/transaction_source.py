from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from pydantic import BaseModel


class StorageTransaction(BaseModel):
    """Схема транзакции обогащенных матчей, ожидающих фиксации в целевом хранилище."""

    id: int
    matches: list[dict[str, Any]]
    created_at: str | None = None


class TransactionSource(ABC):
    """Базовый абстрактный контракт источника транзакций обогащенных матчей.

    Архитектурные принципы:
    1. Изоляция потребителя транзакций (DIP): воркер сохранения взаимодействует
       исключительно с данным интерфейсом, не имея зависимости от реализации конвейера.
    2. Однотранзакционный протокол: метод `get_pending_transaction` возвращает первую
       готовую транзакцию из очереди до тех пор, пока она не подтверждена методом `complete_transaction`.
    3. Идемпотентность закрытия: повторный вызов `complete_transaction` для уже закрытой
       или отсутствующей транзакции должен завершаться успешно без ошибок.
    """

    @abstractmethod
    def get_pending_transaction(self) -> StorageTransaction | None:
        """Возвращает ожидающую транзакцию для фиксации в целевом хранилище или None."""
        pass

    @abstractmethod
    def complete_transaction(self, transaction_id: int) -> bool:
        """Подтверждает успешную запись транзакции и снимает ее с учета в источнике."""
        pass
