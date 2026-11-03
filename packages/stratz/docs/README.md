# Stratz GraphQL Client

Изолированный легковесный клиент к Stratz GraphQL API (`https://api.stratz.com/graphql`).

## Архитектура

```
packages/stratz/
├── docs/
│   └── README.md        # Документация пакета
├── client.py            # Основной клиент StratzClient и контейнер StratzBatchResult
├── queries.py           # Константы полей и построитель GraphQL-запросов
├── exceptions.py        # Иерархия ошибок API
└── __init__.py          # Публичный API пакета
```

### Архитектурные принципы
1. **Запрос как контракт:** GraphQL гарантирует точную форму и типы возвращаемых данных по запрошенным полям (`MATCH_QUERY_FIELDS`). Клиент возвращает чистые `dict[str, Any]` без избыточных Pydantic-схем.
2. **Изоляция сетевого пула:** клиент инкапсулирует `requests.Session` с адаптером автоматических повторов при транзитных сетевых и серверных сбоях (500, 502, 503, 504) для POST-запросов GraphQL.
3. **Прозрачность лимитов (Fail-Fast):** при получении HTTP 429 или throttling сообщений в GraphQL errors немедленно генерируется `StratzRateLimitError`. Клиент не блокирует поток скрытыми `time.sleep()`.
4. **Точечная изоляция ошибок батча:** при пакетных запросах с алиасами (`m<id>`) частичные ошибки сопоставляются с конкретными ID матчей. Невалидные матчи отсеиваются в `failed`, а валидные возвращаются в `enriched`.
5. **Динамическая аутентификация:** поддержка `token_provider` для бесшовной ротации API-токенов в рантайме.

---

## Иерархия исключений

* `StratzError`
  * `StratzGraphQLError`
  * `StratzHttpError`
    * `StratzRateLimitError` (HTTP 429 или throttling в GraphQL)
    * `StratzAuthError` (HTTP 401 / 403 или ошибка токена)
    * `StratzServerError` (HTTP 5xx)

---

## Пример использования

```python
from packages.stratz import (
    StratzClient,
    StratzRateLimitError,
    StratzAuthError,
    StratzGraphQLError,
)

client = StratzClient(
    token_provider=lambda: "current_stratz_token",
    timeout=30.0,
    max_retries=3,
)

try:
    # 1. Пакетный запрос деталей матчей (чистые dict)
    batch_result = client.get_matches([8123456789, 8123456790], batch_size=10)

    for match_dict in batch_result.enriched:
        print(f"Loaded match: {match_dict['match_id']}")

    for failed_id, reason in batch_result.failed.items():
        print(f"Match {failed_id} rejected: {reason}")

    # 2. Получение одного матча (dict | None)
    single_match = client.get_match(8123456789)

    # 3. Произвольный GraphQL-запрос
    leagues = client.execute("""
        query {
            leagues(request: { take: 10, leagueEnded: true }) {
                id
                tier
            }
        }
    """)

except StratzRateLimitError:
    pass
except StratzAuthError:
    pass
except StratzGraphQLError as err:
    pass
finally:
    client.close()
```
