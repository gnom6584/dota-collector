# Quotas

Изолированный пакет базового контракта распределения лимитов (`Quota`).

## Архитектура

```
packages/quotas/
├── docs/
│   └── README.md            # Документация пакета
├── quota.py                 # Базовый интерфейс (Quota)
└── __init__.py              # Публичный API пакета
```

### Архитектурные принципы
1. **Минимализм интерфейса:** контракт `Quota` содержит методы `acquire()` и `reset()`, исключая привязку потребителей к алгоритмам сброса и механизмам хранения.
2. **Атомарность:** вызов `acquire()` атомарно проверяет доступность лимита и списывает запрошенный объем.

> Реализация суточной квоты для SQLite вынесена в отдельный пакет [`packages/quotas_sqlite`](../../quotas_sqlite).

---

## Пример использования

```python
from packages.quotas import Quota
from packages.quotas_sqlite import SqliteDailyQuota
import sqlite3

conn = sqlite3.connect("data/app.db", check_same_thread=False)

# Использование конкретной реализации через абстрактный контракт Quota
quota: Quota = SqliteDailyQuota(
    conn=conn,
    name="opendota_api",
    limit=50000,
)

if quota.acquire(cost=1):
    pass  # Выполняем полезное действие
```
