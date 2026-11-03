# Config Store

Абстрактный контракт типобезопасного персистентного хранилища конфигураций воркеров.

## Архитектура

```
packages/config_store/
├── docs/
│   └── README.md        # Архитектурная документация пакета
├── store.py             # Базовый контракт ConfigStore[T]
└── __init__.py          # Публичный API пакета
```

---

## Архитектурные принципы

1. **Строгая валидация и типизация (`Generic[T: BaseModel]`):** Гарантирует работу исключительно с валидированными Pydantic-моделями параметров воркеров без оперирования нетипизированными словарями.
2. **Инкапсуляция персистентности:** Абстрагирует детали сериализации и хранения параметров от потребителей.
3. **Прямая совместимость с воркерами:** Метод `store.get` передается напрямую в фабрику конфигураций воркера (`config_factory: Callable[[], TConfig]`), обеспечивая обновление параметров без перезапуска процессов.
4. **Управление жизненным циклом:** Метод `close()` позволяет корректно закрывать дескрипторы и транзакции при завершении работы.

---

## Публичный API

```python
from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

class ConfigStore(ABC, Generic[T]):
    @abstractmethod
    def get(self) -> T: ...

    @abstractmethod
    def set(self, config: T) -> None: ...

    def close(self) -> None: ...
```
