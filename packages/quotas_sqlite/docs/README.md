# Quotas SQLite

Изолированная SQLite-реализация суточной квоты (`SqliteDailyQuota`) с автоматическим сбросом в полночь UTC.

## Архитектура

```
packages/quotas_sqlite/
├── docs/
│   └── README.md      # Документация пакета
├── daily_quota.py     # Реализация SqliteDailyQuota(Quota)
└── __init__.py        # Публичный API пакета
```

### Архитектурные принципы
1. **Календарный сброс (00:00:00 UTC):** сброс окна рассчитывается строго по границе календарных суток UTC:
   $$\text{next\_midnight} = \left(\lfloor \text{now} / 86400 \rfloor + 1\right) \times 86400$$
2. **Изоляция транзакций (`BEGIN IMMEDIATE`):** атомарность списания и защиты от перерасхода между процессами.
3. **Методы инспекции:** `remaining()`, `used()`, `reset_at()`, а также внешний сброс `reset_name()`.

---

## DDL схемы (SQLite)

Таблица должна быть создана заранее:

```sql
CREATE TABLE IF NOT EXISTS {table_name} (
    name TEXT PRIMARY KEY,
    used INTEGER NOT NULL,
    reset_at INTEGER NOT NULL
);
```

---

## Пример использования

```python
import sqlite3
from packages.quotas_sqlite import SqliteDailyQuota

conn = sqlite3.connect("data/app.db", check_same_thread=False)

daily_quota = SqliteDailyQuota(
    conn=conn,
    name="opendota_api",
    limit=50000,
)

if daily_quota.acquire(cost=1):
    pass  # Выполняем запрос
```
