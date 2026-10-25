"""Конфигурация и константы пайплайна сбора и обогащения матчей Dota 2.

Все параметры имеют разумные значения по умолчанию и могут быть
переопределены через переменные окружения или аргументы командной строки.
"""

from __future__ import annotations

import os

# --- База данных SQLite ---
DB_PATH: str = os.getenv("DB_PATH", "matches.db")
DB_TIMEOUT_SEC: float = float(os.getenv("DB_TIMEOUT_SEC", "60.0"))
DB_BUSY_TIMEOUT_MS: int = int(os.getenv("DB_BUSY_TIMEOUT_MS", "60000"))

# --- Первый этап: Сборщик сырых матчей (run_match_producer) ---
# Эндпоинт и таймаут OpenDota
OPENDOTA_API_URL: str = os.getenv("OPENDOTA_API_URL", "https://api.opendota.com/api")
OPENDOTA_TIMEOUT: float = float(os.getenv("OPENDOTA_TIMEOUT", "30.0"))

# Опциональный API ключ OpenDota (увеличивает лимиты запросов)
OPENDOTA_API_KEY: str | None = os.getenv("OPENDOTA_API_KEY") or None

# Максимальная ёмкость буфера сырых матчей в SQLite (backpressure)
PRODUCER_BUFFER_SIZE: int = int(os.getenv("PRODUCER_BUFFER_SIZE", "1000"))

# Пауза между подзапросами к OpenDota в секундах
PRODUCER_DELAY: float = float(os.getenv("PRODUCER_DELAY", "1.2"))

# Время сна в состоянии Idle (когда буфер БД полон) в секундах
PRODUCER_IDLE_DELAY: float = float(os.getenv("PRODUCER_IDLE_DELAY", "2.0"))

# Время сна при превышении лимитов OpenDota (HTTP 429) в секундах
PRODUCER_LIMIT_SLEEP: float = float(os.getenv("PRODUCER_LIMIT_SLEEP", "60.0"))

# Фильтры по рангу матчей (опционально)
PRODUCER_MIN_RANK: int | None = int(os.getenv("PRODUCER_MIN_RANK")) if os.getenv("PRODUCER_MIN_RANK") else None
PRODUCER_MAX_RANK: int | None = int(os.getenv("PRODUCER_MAX_RANK")) if os.getenv("PRODUCER_MAX_RANK") else None

# --- Второй этап: Обогатитель матчей (run_match_enricher) ---
# API токен Stratz (Bearer) и эндпоинт
STRATZ_TOKEN: str = os.getenv("STRATZ_TOKEN", "")
STRATZ_API_URL: str = os.getenv("STRATZ_API_URL", "https://api.stratz.com/graphql")

# Минимальный возраст сырого матча в часах для начала обработки (время на парсинг реплеев)
ENRICHER_MIN_AGE_HOURS: float = float(os.getenv("ENRICHER_MIN_AGE_HOURS", "6.0"))

# Размер порции созревших матчей для одного сетевого запроса к Stratz GraphQL
ENRICHER_CHUNK_SIZE: int = int(os.getenv("ENRICHER_CHUNK_SIZE", "15"))

# Интервал проверки базы данных в секундах при ожидании созревших матчей
ENRICHER_POLL_INTERVAL: float = float(os.getenv("ENRICHER_POLL_INTERVAL", "30.0"))

# Время ожидания при исчерпании лимитов внешнего API (LimitError) в секундах
ENRICHER_LIMIT_SLEEP: float = float(os.getenv("ENRICHER_LIMIT_SLEEP", "60.0"))

# Пауза между чанками запросов к Stratz в секундах
ENRICHER_CHUNK_DELAY: float = float(os.getenv("ENRICHER_CHUNK_DELAY", "0.5"))

# Таймаут запросов к Stratz GraphQL API в секундах
STRATZ_TIMEOUT: float = float(os.getenv("STRATZ_TIMEOUT", "30.0"))

# Максимальное количество хранимых обогащённых матчей в БД (0 — без лимита)
ENRICHED_MAX_MATCHES: int = int(os.getenv("ENRICHED_MAX_MATCHES", "10000"))
ENRICHER_MAX_MATCHES: int = ENRICHED_MAX_MATCHES


# --- Третий этап: Веб-сервер FastAPI и Админка ---
API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("API_PORT", "8000"))
API_DEFAULT_LIMIT: int = int(os.getenv("API_DEFAULT_LIMIT", "50"))
API_MAX_LIMIT: int = int(os.getenv("API_MAX_LIMIT", "100"))
API_KEY_HEADER_NAME: str = "X-API-Key"

# Учётные данные администратора для входа в панель управления
ADMIN_USERNAME: str = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "admin123")


