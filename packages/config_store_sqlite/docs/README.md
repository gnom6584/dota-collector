# Config Store SQLite

SQLite-реализация типобезопасного персистентного хранилища конфигураций воркеров.

## Архитектура

```
packages/config_store_sqlite/
├── docs/
│   └── README.md        # Архитектурная документация пакета
├── store.py             # Реализация SqliteConfigStore[T]
└── __init__.py          # Публичный API пакета
```

---

## Архитектурные принципы

1. **Изоляция транзакций (`BEGIN IMMEDIATE`):** Обеспечивает сериализацию параллельных транзакций записи конфигураций, предотвращая возникновение конфликтов и гонок при одновременном обновлении.
2. **Идемпотентная автоинициализация (`INSERT OR IGNORE`):** При первом запуске или отсутствии записи в таблице гарантированно сохраняет значения по умолчанию.
3. **Отказоустойчивость при десериализации:** Если JSON-данные в базе повреждены или не соответствуют текущей схеме модели, ошибка логируется, а метод `get()` возвращает копию дефолтной конфигурации без падения воркера.
4. **Управление ресурсами:** Не закрывает переданное в конструктор соединение `conn`, сохраняя инвариант внешнего управления соединением.

---

## DDL-схема таблицы

```sql
CREATE TABLE IF NOT EXISTS worker_configs (
    worker_name TEXT PRIMARY KEY,
    config_json TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);
```

---

## Пример использования

```python
import sqlite3
from packages.config_store_sqlite import SqliteConfigStore
from packages.public_match_collector import PublicMatchCollectorConfig

conn = sqlite3.connect("data/matches.db", check_same_thread=False)

store = SqliteConfigStore(
    conn=conn,
    key="public_match_collector",
    model_cls=PublicMatchCollectorConfig,
    default=PublicMatchCollectorConfig(min_rank=70, cooldown=2.0),
)

# Чтение актуальной конфигурации
cfg = store.get()
print(f"Current min_rank: {cfg.min_rank}")

# Сохранение обновленной конфигурации
updated = cfg.model_copy(update={"min_rank": 80})
store.set(updated)

# Прямая передача метода get в воркер
# config_factory = store.get
```
