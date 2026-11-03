# Match Enricher

Воркер обогащения деталей матчей Dota 2 через Stratz GraphQL API с изолированной обработкой частичных сбоев.

## Архитектура

```
packages/match_enricher/
├── docs/
│   └── README.md        # Документация пакета
├── config.py            # Конфигурация параметров батчинга и задержек MatchEnricherConfig
├── worker.py            # Реализация MatchEnricher
└── __init__.py          # Публичный API пакета
```

### Архитектурные принципы
1. **Персистентность при backpressure (`Worker[MatchEnricherState]`):**
   - При отказе приёмника (`sink(...) == False`) неотправленный батч (`pending_enriched`, `pending_failed`) сохраняется в состоянии воркера.
   - На следующей итерации воркер повторяет отправку батча без повторного расхода квоты Stratz API.
2. **Абстрактные I/O границы:**
   - `source(batch_size: int) -> list[int]`: поставщик идентификаторов матчей, требующих обогащения.
   - `sink(enriched: list[dict[str, Any]], failed: dict[int, str]) -> bool`: потребитель результатов, разделяющий валидные данные матчей и причины отсева/ошибок. Возвращает True при успешном приеме или False при блокировке (backpressure).
3. **Точечная изоляция ошибок:**
   - Ошибки отдельных матчей в батчевом GraphQL-запросе не прерывают обработку валидных матчей.
   - Ненайденные, нераспарсенные или содержащие ошибки GraphQL матчи передаются в `sink` в словаре `failed` с точным описанием причины.
4. **Экономия квоты:**
   - Запрос к квоте (`quota.acquire(cost=1)`) выполняется только при наличии непустого списка ID из `source`.
   - Если очередь пуста, воркер переходит в режим ожидания на `idle_delay` без списания квоты.
5. **Устойчивость к сбоям:**
   - Исчерпание квоты: пауза `quota_retry_delay`.
   - HTTP 429: пауза `rate_limit_delay`.
   - Сетевые сбои и HTTP 5xx: пауза `error_delay`.

---

## Пример использования

```python
from packages.match_enricher import (
    MatchEnricher,
    MatchEnricherConfig,
    MatchEnricherState,
)
from packages.quotas import Quota
from packages.stratz import StratzClient


class MemoryQuota(Quota):
    def acquire(self, cost: int = 1) -> bool:
        return True


client = StratzClient(token_provider=lambda: "SECRET_TOKEN")
quota = MemoryQuota()

queue: list[int] = [7000000001, 7000000002]


def source(batch_size: int) -> list[int]:
    batch = queue[:batch_size]
    del queue[:batch_size]
    return batch


def sink(enriched: list[dict], failed: dict[int, str]) -> bool:
    for match in enriched:
        print(f"Enriched match {match['match_id']}")
    for match_id, reason in failed.items():
        print(f"Failed match {match_id}: {reason}")
    return True


worker = MatchEnricher(
    client=client,
    source=source,
    sink=sink,
    quota=quota,
    config_factory=lambda: MatchEnricherConfig(batch_size=10, cooldown=2.0),
)

state = MatchEnricherState()
state, delay = worker.execute(state)
```
