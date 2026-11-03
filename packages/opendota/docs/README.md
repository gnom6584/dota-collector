# OpenDota Client

Изолированный легковесный REST-клиент к OpenDota API (`https://api.opendota.com/api`).

## Архитектура

```
packages/opendota/
├── docs/
│   └── README.md        # Документация пакета
├── client.py            # Основной клиент OpenDotaClient
├── schemas.py           # Pydantic-схемы данных API (ProMatch, PublicMatch)
├── exceptions.py        # Иерархия ошибок API
└── __init__.py          # Публичный API пакета
```

### Архитектурные принципы
1. **Изоляция сетевого пула:** клиент инкапсулирует `requests.Session` с адаптером автоматических повторов при транзитных сетевых ошибках (500, 502, 503, 504).
2. **Прозрачность лимитов (No Silent Backoff):** при получении HTTP 429 генерируется `OpenDotaRateLimitError`. Клиент не блокирует поток скрытыми `time.sleep()`, оставляя стратегию ожидания вызывающему коду.
3. **Fail-Fast валидация контракта (Anti-Corruption Layer):** методы клиента валидируют структуру ответов через Pydantic. При несоответствии контракта выбрасывается `OpenDotaValidationError`, предотвращая утечку поврежденных данных в базу.
4. **Динамическая аутентификация:** поддержка `token_provider` для ротации ключей в рантайме без пересоздания клиента.
5. **Расширяемость:** базовые методы сбора матчей дополнены универсальным методом `get(endpoint, params)`, позволяющим запрашивать любые эндпоинты OpenDota без правок клиента.

---

## Иерархия исключений

* `OpenDotaError`
  * `OpenDotaValidationError` (несоответствие схемы ответа контракту)
  * `OpenDotaHttpError`
    * `OpenDotaRateLimitError` (HTTP 429)
    * `OpenDotaAuthError` (HTTP 401 / 403)
    * `OpenDotaNotFoundError` (HTTP 404)
    * `OpenDotaServerError` (HTTP 5xx)

---

## Пример использования

```python
from packages.opendota import (
    OpenDotaClient,
    OpenDotaRateLimitError,
    OpenDotaValidationError,
)

# Статический ключ либо динамический провайдер (ротация ключей)
client = OpenDotaClient(
    token_provider=lambda: "current_dynamic_api_key",
    timeout=10.0,
    max_retries=3,
)

try:
    # 1. Сбор пачки недавних публичных матчей (PublicMatch с фильтрацией по рангу)
    public_matches = client.get_public_matches(less_than_match_id=8000000000, min_rank=70)

    # 2. Сбор профессиональных матчей (ProMatch)
    pro_matches = client.get_pro_matches(less_than_match_id=8000000000)

    # 3. Получение детальной информации о конкретном матче (сырой dict)
    match_details = client.get_match(match_id=123456789)

    # 4. Произвольный вызов любого другого эндпоинта OpenDota (сырой JSON)
    heroes = client.get("heroes")

except OpenDotaValidationError as err:
    # Контракт API нарушен — аварийный Fail-Fast
    pass
except OpenDotaRateLimitError:
    # Лимит исчерпан — обработка ошибки на уровне вызывающего кода
    pass
finally:
    client.close()
```
