# Match Storage Writer

Фоновый воркер асинхронной фиксации транзакций обогащенных матчей в целевом хранилище.

## Архитектура пакета

```
packages/match_storage_writer/
├── docs/
│   └── README.md            # Архитектурная документация пакета
├── transaction_source.py    # Схема StorageTransaction и контракт TransactionSource
├── config.py                # Pydantic-конфигурация MatchStorageWriterConfig
├── worker.py                # Реализация MatchStorageWriter и MatchStorageWriterState
└── __init__.py              # Публичный API пакета
```

---

## Архитектурные принципы

1. **Изоляция источника транзакций (DIP):** Воркер взаимодействует с источником исключительно через абстрактный интерфейс `TransactionSource`, не завися от конкретной реализации конвейера публичных матчей.
2. **Идемпотентность и отказоустойчивость:** Сохранение выполняется через интерфейс `Storage.insert_many()`. При сетевых или дисковых ошибках транзакция не закрывается в источнике и автоматически повторяется на следующем цикле.
3. **Двухфазная фиксация без потерь:** Метод `complete_transaction(tx.id)` вызывается строго после подтверждения успешной вставки целевым хранилищем, гарантируя целостность передачи данных между буфером конвейера и постоянным хранилищем.
4. **Управление жизненным циклом (Fail-Fast & Observability):** Интегрирован в экосистему `packages.workers` (`Worker`, `Runner`), персистирует состояние (`total_saved_matches`, `last_transaction_id`) и поддерживает динамическое изменение конфигурации задержек (`cooldown`, `idle_delay`, `error_delay`).

---

## Пример использования

```python
from packages.match_storage_sqlite import SqliteStorage
from packages.match_storage_writer import (
    MatchStorageWriter,
    MatchStorageWriterConfig,
    MatchStorageWriterState,
    StorageTransaction,
    TransactionSource,
)


class DummyTransactionSource(TransactionSource):
    def get_pending_transaction(self) -> StorageTransaction | None:
        return StorageTransaction(id=1, matches=[{"match_id": 7000000001}])

    def complete_transaction(self, transaction_id: int) -> bool:
        return True


source = DummyTransactionSource()
storage = SqliteStorage(...)
writer = MatchStorageWriter(
    source=source,
    storage=storage,
    config_factory=lambda: MatchStorageWriterConfig(),
)

state = MatchStorageWriterState()
new_state, delay = writer.execute(state)
```
