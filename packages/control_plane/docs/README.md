# Control Plane

Конкретный фасад управления, динамического конфигурирования и мониторинга системы сбора и обогащения матчей.

## Архитектура пакета

```
packages/control_plane/
├── docs/
│   └── README.md        # Архитектурная документация пакета
├── models.py            # Модели телеметрии (WorkerStats, QuotaStats, SystemStats)
├── control_plane.py     # Конкретная реализация ControlPlane
└── __init__.py          # Публичный API пакета
```

---

## Архитектурные принципы

1. **Изоляция от слоя хранения (Zero SQL):** Класс `ControlPlane` полностью абстрагирован от конкретной СУБД и не содержит прямых SQL-запросов. Взаимодействие осуществляется строго через абстрактные интерфейсы (`Backend`, `PipelineControl`, `Storage`, `DailyQuota`, `ConfigStore`).
2. **Динамическое конфигурирование:** Предоставляет типобезопасные методы чтения и обновления параметров воркеров на лету без перезапуска процессов:
   - `public_match_collector` (`PublicMatchCollectorConfig`);
   - `match_enricher` (`MatchEnricherConfig`);
   - `match_storage_writer` (`MatchStorageWriterConfig`).
3. **Управление конвейером и емкостью:**
   - Делегирует изменение лимита очереди созревания и времени созревания в `PipelineControl`.
   - Делегирует изменение лимита целевого хранилища в `Storage`.
4. **Консолидированный мониторинг здоровья (Observability):** Метод `get_system_stats()` собирает единый снимок состояния системы (`SystemStats`), опрашивая зарегистрированные воркеры, суточные квоты, очередь созревания и постоянное хранилище.

---

## Пример использования

```python
import sqlite3
from packages.workers_sqlite import SqliteBackend
from packages.public_match_pipeline import SqlitePipelineControl
from packages.match_storage_sqlite import SqliteStorage
from packages.config_store_sqlite import SqliteConfigStore
from packages.quotas_sqlite import SqliteDailyQuota
from packages.control_plane import ControlPlane
from packages.public_match_collector import PublicMatchCollectorConfig
from packages.match_enricher import MatchEnricherConfig
from packages.match_storage_writer import MatchStorageWriterConfig

conn = sqlite3.connect("data/matches.db", check_same_thread=False)

backend = SqliteBackend(conn=conn)
pipeline_control = SqlitePipelineControl(conn=conn)
storage = SqliteStorage(conn=conn, max_matches=100_000)

collector_store = SqliteConfigStore[PublicMatchCollectorConfig](
    conn=conn,
    key="public_match_collector",
    model_cls=PublicMatchCollectorConfig,
)
enricher_store = SqliteConfigStore[MatchEnricherConfig](
    conn=conn,
    key="match_enricher",
    model_cls=MatchEnricherConfig,
)
writer_store = SqliteConfigStore[MatchStorageWriterConfig](
    conn=conn,
    key="match_storage_writer",
    model_cls=MatchStorageWriterConfig,
)

quota_opendota = SqliteDailyQuota(conn=conn, name="opendota", limit=50_000)
quota_stratz = SqliteDailyQuota(conn=conn, name="stratz", limit=10_000)

cp = ControlPlane(
    backend=backend,
    pipeline_control=pipeline_control,
    storage=storage,
    public_collector_config_store=collector_store,
    enricher_config_store=enricher_store,
    storage_writer_config_store=writer_store,
    worker_quotas={
        "public_match_collector": quota_opendota,
        "match_enricher": quota_stratz,
    },
    worker_names=["public_match_collector", "match_enricher", "match_storage_writer"],
)

# Мониторинг системы
stats = cp.get_system_stats()
print(f"Queue: {stats.queue_size}/{stats.queue_max_size}, Storage: {stats.enriched_matches_count}")

# Управление воркерами
cp.enable_worker("match_storage_writer")
cp.disable_worker("public_match_collector")
```
