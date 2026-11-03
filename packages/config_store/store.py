from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class ConfigStore(ABC, Generic[T]):
    """Абстрактный контракт типобезопасного персистентного хранилища конфигураций.

    Архитектурные принципы:
    1. Строгая валидация и типизация (Generic[T: BaseModel]): гарантирует работу с конкретными
       Pydantic-моделями параметров воркеров без работы с нетипизированными словарями.
    2. Инкапсуляция персистентности: изолирует алгоритм чтения, кэширования и записи конфигураций
       от используемого бэкенда (SQLite, PostgreSQL, Redis, локальный файл или внешнее Key-Value).
    3. Прямая совместимость с воркерами: метод `get` экземпляра ConfigStore напрямую совместим
       с сигнатурой `config_factory: Callable[[], TConfig]`, требуемой раннером и воркером.
    4. Безопасность ресурсов: метод `close` освобождает внешние дескрипторы при необходимости.
    """

    @abstractmethod
    def get(self) -> T:
        """Возвращает актуальный экземпляр конфигурации.

        Returns:
            T: Валидированная Pydantic-модель актуальной конфигурации.
        """
        pass

    @abstractmethod
    def set(self, config: T) -> None:
        """Сохраняет обновленную конфигурацию в постоянное хранилище.

        Args:
            config: Экземпляр Pydantic-модели с новыми параметрами.
        """
        pass

    def close(self) -> None:
        """Освобождает дескрипторы и ресурсы подключения хранилища при необходимости."""
        pass
