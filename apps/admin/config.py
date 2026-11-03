from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from apps.match_collector.config import AppConfig


class AdminConfig(BaseSettings):
    """Конфигурация HTTP-сервера панели администратора (Control Plane API).

    Определяет сетевые параметры привязки веб-сервера и инкапсулирует
    базовую конфигурацию домена сбора матчей AppConfig.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(
        default="127.0.0.1",
        validation_alias="ADMIN_HOST",
        description="Хост сетевого интерфейса для запуска HTTP-сервера панели управления.",
    )
    port: int = Field(
        default=2228,
        validation_alias="ADMIN_PORT",
        description="TCP-порт для входящих HTTP-соединений панели управления.",
    )
    app_config: AppConfig = Field(
        default_factory=AppConfig,
        description="Сводная конфигурация инфраструктуры и пайплайна сбора матчей.",
    )
