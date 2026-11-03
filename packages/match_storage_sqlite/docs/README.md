# Match Storage SQLite

SQLite-адаптер целевого хранилища обогащенных матчей Dota 2.

## Архитектура

```
packages/match_storage_sqlite/
├── docs/
│   └── README.md        # Документация пакета
├── storage.py           # Реализация SqliteStorage
└── __init__.py          # Публичный API пакета
```

---

## DDL-схема

```sql
CREATE TABLE IF NOT EXISTS enriched_matches (
    match_id INTEGER PRIMARY KEY,
    data_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);
```

---

## Пример использования

```python
import sqlite3
from packages.match_storage_sqlite import SqliteStorage

conn = sqlite3.connect("data/matches.db", check_same_thread=False)

storage = SqliteStorage(
    conn=conn,
    max_matches=100_000,
    table_name="enriched_matches",
)

free_slots = storage.get_free_space()
success = storage.insert_many([{"match_id": 7000000001, "duration": 2100}])
```
