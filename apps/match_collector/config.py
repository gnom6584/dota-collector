from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from config import DatabaseConfig


class AppConfig(BaseSettings):
    """Сводная конфигурация приложения сбора и обогащения матчей Dota 2.

    Поддерживает считывание переменных окружения и файла .env:
    - API-ключи: OPENDOTA_API_KEY, STRATZ_API_TOKEN;
    - Суточные лимиты: OPENDOTA_DAILY_LIMIT, STRATZ_DAILY_LIMIT;
    - Параметры пайплайна: MAX_QUEUE_SIZE, MAX_STORAGE_MATCHES, MATURITY_COOLDOWN_SECONDS;
    - Пути и таблицы базы данных через вложенную DatabaseConfig.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db: DatabaseConfig = Field(default_factory=DatabaseConfig)

    opendota_api_key: str | None = Field(
        default=None,
        validation_alias="OPENDOTA_API_KEY",
        description="Ключ доступа к OpenDota API (опционально для бесплатного тарифа).",
    )
    stratz_api_token: str = Field(
        default="",
        validation_alias="STRATZ_API_TOKEN",
        description="Bearer токен доступа к Stratz GraphQL API.",
    )

    opendota_daily_limit: int = Field(
        default=50_000,
        validation_alias="OPENDOTA_DAILY_LIMIT",
        description="Суточный лимит запросов к OpenDota API.",
    )
    stratz_daily_limit: int = Field(
        default=10_000,
        validation_alias="STRATZ_DAILY_LIMIT",
        description="Суточный лимит вызовов к Stratz GraphQL API.",
    )

    max_queue_size: int = Field(
        default=30_000,
        validation_alias="MAX_QUEUE_SIZE",
        description="Максимальная емкость очереди созревания матчей (backpressure).",
    )
    max_storage_matches: int = Field(
        default=1_000_000,
        validation_alias="MAX_STORAGE_MATCHES",
        description="Предельная емкость хранилища обогащенных матчей.",
    )
    maturity_cooldown_seconds: float = Field(
        default=21600.0,
        validation_alias="MATURITY_COOLDOWN_SECONDS",
        description="Время созревания матчей перед обогащением в секундах (по умолчанию 30 минут).",
    )
