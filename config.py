from __future__ import annotations

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseSettings):
    """Конфигурация путей к файлам SQLite и канонических наименований таблиц системы.

    Поддерживает переопределение через переменные окружения (.env или переменные ОС):
    - DB_PATH: путь к файлу базы данных SQLite;
    - LOCK_DIR: директория для PID-файлов файловых блокировок процессов.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db_path: Path = Field(
        default=Path("data/matches.db"),
        validation_alias="DB_PATH",
        description="Путь к файлу базы данных SQLite.",
    )
    lock_dir: Path = Field(
        default=Path("data/locks"),
        validation_alias="LOCK_DIR",
        description="Директория для PID-файлов файловых блокировок процессов воркеров.",
    )
    disabled_poll_interval: float = Field(
        default=3.0,
        validation_alias="DISABLED_POLL_INTERVAL",
        description="Интервал опроса флага активности деактивированного воркера в секундах.",
    )

    # Имена таблиц в базе данных SQLite
    queue_table: str = Field(
        default="public_match_queue",
        description="Таблица очереди созревания публичных матчей.",
    )
    storage_table: str = Field(
        default="enriched_matches",
        description="Таблица постоянного хранения обогащенных матчей.",
    )
    worker_control_table: str = Field(
        default="worker_control",
        description="Таблица координации активности и статусов воркеров.",
    )
    worker_states_table: str = Field(
        default="worker_states",
        description="Таблица персистентных состояний воркеров.",
    )
    worker_errors_table: str = Field(
        default="worker_errors",
        description="Таблица аварийных ошибок и трейсбеков воркеров.",
    )
    daily_quotas_table: str = Field(
        default="daily_quotas",
        description="Таблица суточных квот внешних API.",
    )
    worker_configs_table: str = Field(
        default="worker_configs",
        description="Таблица динамических параметров конфигурации воркеров.",
    )
    registry_table: str = Field(
        default="active_match_registry",
        description="Таблица реестра активных матчей в контуре обработки.",
    )
    transaction_table: str = Field(
        default="pipeline_transactions",
        description="Таблица исходящих транзакций обогащенных матчей для вставщика.",
    )
