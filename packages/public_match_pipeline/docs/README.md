# Public Match Pipeline

Конвейер сбора, созревания, фильтрации и двухфазной фиксации публичных матчей Dota 2 (модель Push, Peek, Commit & Storage Transaction).

## Архитектура пакета

```
packages/public_match_pipeline/
├── docs/
│   └── README.md        # Архитектурная документация пакета
├── pipeline.py          # Базовый абстрактный контракт PublicMatchPipeline
├── control.py           # Контракт PipelineControl и реализация SqlitePipelineControl
├── impl.py              # SQLite-реализация конвейера PublicMatchPipelineImpl
└── __init__.py          # Публичный API пакета
```

---

## Архитектурные принципы

1. **Разделение контуров исполнения и управления (CQS/CQRS):**
   - Контур Data Plane (`PublicMatchPipeline` / `PublicMatchPipelineImpl`) отвечает за высоконагруженную обработку потока матчей (`push`, `peek`, `commit`, `get_pending_transaction`, `complete_transaction`).
   - Контур Control Plane (`PipelineControl` / `SqlitePipelineControl`) инкапсулирует динамическое изменение лимитов очереди, задержки созревания и получение метрик без прямой зависимости от Data Plane.
2. **Атомарная входная дедупликация и фильтрация (`push`):**
   - Выполняется пакетная проверка по активному регистру (`active_match_registry`) и целевому хранилищу (`enriched_matches`).
   - Новые уникальные матчи атомарно фиксируются в очереди (`public_match_queue`) и регистре.
   - При превышении максимальной вместимости транзакция полностью откатывается (`ROLLBACK`), возвращая `False` (backpressure на сборщик).
3. **Модель созревания матчей (`peek`):**
   - Из очереди извлекаются только те матчи, с момента завершения которых прошло не менее `maturation_delay_seconds` (фильтр по `finished_at <= now - delay`).
4. **Двухфазная фиксация обогащения (`commit`):**
   - Удаляет сбойные матчи из активного регистра, освобождая их для повторного сбора.
   - Проверяет буфер транзакций (`pipeline_transactions`). Если предыдущая транзакция еще не обработана воркером сохранения, возвращает `False` (backpressure на обогатитель).
   - При отсутствии блокировок атомарно формирует запись транзакции и удаляет обработанные матчи из очереди.
5. **Интеграция с воркером сохранения (DIP):**
   - `PublicMatchPipeline` реализует контракт `TransactionSource` из пакета `match_storage_writer`, изолируя внутреннее устройство конвейера от воркера записи.

---

## DDL-схема таблиц

```sql
-- Очередь созревания
CREATE TABLE IF NOT EXISTS public_match_queue (
    match_id INTEGER PRIMARY KEY,
    finished_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_public_match_queue_finished_at 
ON public_match_queue(finished_at);

-- Активный регистр матчей в обработке
CREATE TABLE IF NOT EXISTS active_match_registry (
    match_id INTEGER PRIMARY KEY,
    registered_at TEXT DEFAULT (datetime('now'))
);

-- Буфер транзакций для сохранения в БД
CREATE TABLE IF NOT EXISTS pipeline_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);
```

---

## Пример использования

```python
import sqlite3
from packages.public_match_pipeline import (
    PublicMatchPipelineImpl,
    SqlitePipelineControl,
)
from packages.opendota import PublicMatch

conn = sqlite3.connect("data/matches.db", check_same_thread=False)

control = SqlitePipelineControl(
    conn=conn,
    default_max_size=10_000,
    default_maturation_delay=21600.0,
)

pipeline = PublicMatchPipelineImpl(
    conn=conn,
    control=control,
    queue_table="public_match_queue",
    storage_table="enriched_matches",
    registry_table="active_match_registry",
    transaction_table="pipeline_transactions",
)

# 1. Прием матчей от коллектора
matches = [PublicMatch(match_id=7000000001, start_time=1700000000, duration=2400)]
accepted = pipeline.push(matches)

# 2. Выборка созревших матчей обогатителем
ready_ids = pipeline.peek(limit=50)

# 3. Фиксация результатов обогащения
committed = pipeline.commit(
    enriched=[{"match_id": 7000000001, "radiant_win": True}],
    failed=None,
)

# 4. Чтение транзакции воркером сохранения (контракт TransactionSource)
tx = pipeline.get_pending_transaction()
if tx:
    pipeline.complete_transaction(tx.id)
```
