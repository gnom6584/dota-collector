from __future__ import annotations

from pydantic import BaseModel, Field


class MatchStorageWriterConfig(BaseModel):
    """Конфигурация параметров выполнения воркера match_storage_writer.

    Архитектурные принципы:
    1. Независимость интервалов: разделяет задержки между успешными циклами (`cooldown`),
       период ожидания при отсутствии транзакций (`idle_delay`) и паузу при сбоях (`error_delay`).
    2. Валидация диапазонов: все временные интервалы выражаются в положительных секундах.
    3. Типобезопасная сериализация: поддерживается сохранение и чтение через ConfigStore.
    """

    cooldown: float = Field(
        default=0.5,
        ge=0.01,
        description="Pause in seconds between consecutive successful transaction writes.",
    )
    idle_delay: float = Field(
        default=1.0,
        ge=0.1,
        description="Pause in seconds when no pending transactions are available.",
    )
    error_delay: float = Field(
        default=2.0,
        ge=0.1,
        description="Pause in seconds upon storage write failures or transient errors.",
    )
