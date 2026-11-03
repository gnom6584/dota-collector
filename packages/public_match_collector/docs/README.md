# Public Match Collector

Воркер сбора свежих публичных матчей Dota 2 из OpenDota API.

## Принцип работы

Воркер выполняет периодический опрос вершины ленты публичных матчей OpenDota API, обеспечивая непрерывный приток актуальных завершенных игр:

1. **Опрос вершины ленты:** на каждом шаге `execute` коллектор обращается к эндпоинту `publicMatches` без пагинации вглубь истории (`less_than_match_id`), получая срез последних публичных матчей (до 100 записей) с учетом фильтров рангов (`min_rank`, `max_rank`).
2. **Локальная фильтрация:** скачанные матчи валидируются по дополнительным критериям:
   - **Длительность матча (`min_duration`, `max_duration`):** фильтрация по диапазону продолжительности игры в секундах (например, от 900 до 3600 секунд);
   - **Кластер серверов (`cluster`):** фильтрация по множеству разрешенных ID серверов (либо `None` для отключения);
   - **Игровой режим (`game_mode`):** фильтрация по множеству разрешенных ID режимов (либо `None` для отключения).
3. **Курсор актуальности (`last_match_id`):** воркер отфильтровывает уже обработанные игры, пропуская только матчи с `match_id > last_match_id`, и обновляет курсор максимальным идентификатором из просмотренной пачки.
4. **Безопасность смены фильтров:** отсутствие движения вглубь истории гарантирует, что при изменении любых фильтров на лету коллектор не считывает устаревшие архивные матчи.
5. **Изоляция Backpressure:** при отказе приемника (переполнение очереди или базы данных) отфильтрованная пачка удерживается в персистентном состоянии `pending_matches`, а повторные сетевые вызовы приостанавливаются до освобождения места в пайплайне без расхода суточных квот.
6. **Обработка лимитов и сбоев:** воркер учитывает суточные квоты (`Quota`), корректно обрабатывает HTTP 429 (`rate_limit_delay`) и сетевые ошибки (`error_delay`).

## Архитектура

```
packages/public_match_collector/
├── docs/
│   └── README.md        # Документация пакета
├── config.py            # Конфигурация параметров сбора PublicMatchCollectorConfig
├── worker.py            # Реализация PublicMatchCollector и PublicMatchCollectorState
└── __init__.py          # Публичный API пакета
```

---

## Пример использования

```python
from packages.opendota import OpenDotaClient
from packages.public_match_collector import (
    PublicMatchCollector,
    PublicMatchCollectorConfig,
    PublicMatchCollectorState,
)
from packages.quotas import Quota


class MemoryQuota(Quota):
    def acquire(self, cost: int = 1) -> bool:
        return True


client = OpenDotaClient()
quota = MemoryQuota()

worker = PublicMatchCollector(
    client=client,
    sink=lambda matches: True,
    quota=quota,
    config_factory=lambda: PublicMatchCollectorConfig(
        min_rank=70,
        min_duration=900,
        max_duration=3600,
        cluster={181, 182},
        game_mode={1, 22},
        cooldown=2.0,
    ),
)

state = PublicMatchCollectorState()
state, delay = worker.execute(state)
```
