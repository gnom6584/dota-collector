from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class PublicMatchCollectorConfig(BaseModel):
    """Конфигурация параметров сбора, фильтрации и задержек воркера public_match_collector.

    Архитектурные принципы:
    1. Опрос свежей ленты: периодический опрос вершины ленты OpenDota с шагом cooldown.
    2. Сетевая фильтрация API: параметры `min_rank` / `max_rank` передаются в OpenDota.
    3. Локальная фильтрация: диапазон длительности (`min_duration`, `max_duration`),
       множество кластеров (`cluster`) и игровых режимов (`game_mode`).
    """

    model_config = ConfigDict(extra="ignore")

    api_key: str | None = None
    min_rank: int | None = None
    max_rank: int | None = None
    min_duration: int | None = None
    max_duration: int | None = None
    cluster: set[int] | None = None
    game_mode: set[int] | None = None
    cooldown: float = 2.0
    quota_retry_delay: float = 10.0
    rate_limit_delay: float = 60.0
    error_delay: float = 20.0
