# Workers SQLite

Изолированная SQLite-реализация бэкенда персистентности и координации воркеров (`SqliteBackend`).

## Архитектура

```
packages/workers_sqlite/
├── docs/
│   └── README.md      # Документация пакета
├── backend.py         # Реализация SqliteBackend(Backend)
└── __init__.py        # Публичный API пакета
```

### Архитектурные принципы
1. **Изоляция транзакций (`BEGIN IMMEDIATE`):** все мутирующие операции захватывают write-блокировку базы данных, исключая дедлоки (`SQLITE_BUSY`) при конкурентном доступе нескольких процессов.
2. **Type Fingerprinting:** при первом запуске фиксирует класс воркера (`worker_type`). При несовпадении типов прерывает выполнение для предотвращения порчи данных.
3. **Безопасное внешнее управление:** методы `@classmethod` (`enable_worker`, `disable_worker`) позволяют переключать флаг активности воркера из CLI без подъема инстанса раннера.

---

## DDL схемы (SQLite)

Таблицы должны быть созданы заранее:

```sql
CREATE TABLE IF NOT EXISTS {prefix}worker_control (
    worker_name TEXT PRIMARY KEY,
    worker_type TEXT,
    is_enabled INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'stopped',
    pid INTEGER,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS {prefix}worker_errors (
    worker_name TEXT PRIMARY KEY,
    error TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS {prefix}worker_states (
    worker_name TEXT PRIMARY KEY,
    state_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT DEFAULT (datetime('now'))
);
```

---

## Пример использования

```python
import sqlite3
from packages.workers import Runner, Worker
from packages.workers_sqlite import SqliteBackend


class CounterWorker(Worker[int]):
    name = "counter"

    def execute(self, state: int) -> tuple[int, float]:
        return state + 1, 1.0


conn = sqlite3.connect("data/app.db", check_same_thread=False)
backend = SqliteBackend(conn=conn)
worker = CounterWorker()

runner = Runner(
    worker=worker,
    backend=backend,
    default_state=0,
)

runner.run(auto_enable=True)
```
