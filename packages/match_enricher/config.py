from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class MatchEnricherConfig(BaseModel):
    """Конфигурация параметров батчинга и задержек воркера match_enricher."""

    model_config = ConfigDict(extra="ignore")

    api_token: str | None = None
    batch_size: int = 10
    # Пауза между успешными обработанными батчами в секундах
    cooldown: float = 2.0
    # Пауза при отсутствии готовых к обогащению матчей в очереди в секундах
    idle_delay: float = 5.0
    # Интервал повторной проверки при исчерпании локальной квоты в секундах
    quota_retry_delay: float = 10.0
    # Интервал сна при получении HTTP 429 Too Many Requests в секундах
    rate_limit_delay: float = 60.0
    # Интервал сна при сетевых сбоях и ошибках 5xx серверов API в секундах
    error_delay: float = 20.0
