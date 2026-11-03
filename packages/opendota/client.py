from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any
from pydantic import ValidationError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .exceptions import (
    OpenDotaError,
    OpenDotaAuthError,
    OpenDotaHttpError,
    OpenDotaNotFoundError,
    OpenDotaRateLimitError,
    OpenDotaServerError,
    OpenDotaValidationError,
)
from .schemas import ProMatch, PublicMatch

logger = logging.getLogger("packages.opendota.client")


class OpenDotaClient:
    """Отказоустойчивый REST-клиент к OpenDota API.

    Архитектурные принципы:
    1. Изоляция сессии: использует собственный пул соединений `requests.Session`
       с автоматическими ретраями на временные сбои инфраструктуры (500, 502, 503, 504).
    2. Явная обработка лимитов: при получении HTTP 429 бросает `OpenDotaRateLimitError`
       без скрытых пауз внутри клиента, делегируя обработку вызывающему коду.
    3. Динамическая аутентификация: поддерживает как статический `api_key`, так и
       динамический `token_provider` для ротации ключей на лету без пересоздания клиента.
    4. Расширяемость: содержит готовые методы для матчей и низкоуровневый `get()`
       для прямого вызова любых дополнительных эндпоинтов API.
    """

    def __init__(
        self,
        token_provider: Callable[[], str | None] = lambda: None,
        base_url: str = "https://api.opendota.com/api",
        timeout: float = 30.0,
        max_retries: int = 3,
        session: requests.Session | None = None,
    ) -> None:
        self.token_provider = token_provider
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

        self._session = session or self._create_session()
        self._owns_session = session is None

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _reset_session(self) -> None:
        if self._owns_session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = self._create_session()

    def _resolve_api_key(self) -> str | None:
        return self.token_provider()

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Выполняет GET-запрос к эндпоинту OpenDota API с автовосстановлением соединения.

        Args:
            endpoint: Путь эндпоинта относительно base_url (например, 'matches/123').
            params: Query-параметры запроса.

        Returns:
            Any: Распарсенный JSON-ответ.

        Raises:
            OpenDotaRateLimitError: При получении 429 Too Many Requests.
            OpenDotaAuthError: При 401 Unauthorized или 403 Forbidden.
            OpenDotaNotFoundError: При 404 Not Found.
            OpenDotaServerError: При 5xx ошибках серверов после исчерпания ретраев.
            OpenDotaHttpError: При прочих HTTP ошибках.
            OpenDotaError: При сбоях декодирования JSON или исчерпании сетевых попыток.
        """
        clean_endpoint = endpoint.lstrip("/")
        url = f"{self.base_url}/{clean_endpoint}"

        query_params: dict[str, Any] = {}
        api_key = self._resolve_api_key()
        if api_key:
            query_params["api_key"] = api_key
        if params:
            query_params.update(params)

        headers = {
            "User-Agent": "match-collector/1.0",
            "Accept": "application/json",
            "Connection": "close",
        }

        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._session.get(
                    url,
                    params=query_params,
                    headers=headers,
                    timeout=self.timeout,
                )
            except (requests.RequestException, OSError) as e:
                last_error = e
                if attempt < self.max_retries:
                    backoff = min(6.0, 0.5 * (2 ** (attempt - 1)))
                    logger.warning(
                        "OpenDota transport error (%s). Recreating session and retrying (%d/%d in %.1fs)...",
                        e,
                        attempt,
                        self.max_retries,
                        backoff,
                    )
                    self._reset_session()
                    time.sleep(backoff)
                    continue
                raise OpenDotaError(
                    f"Network error querying OpenDota API [{url}] after {self.max_retries} attempts: {e}"
                ) from e

            if response.status_code == 429:
                raise OpenDotaRateLimitError(
                    response.status_code, "Rate limit exceeded.", response.text
                )
            if response.status_code in (401, 403):
                raise OpenDotaAuthError(
                    response.status_code, "Authentication failed or access denied.", response.text
                )
            if response.status_code == 404:
                raise OpenDotaNotFoundError(
                    response.status_code, "Resource not found.", response.text
                )
            if 500 <= response.status_code < 600:
                if attempt < self.max_retries:
                    backoff = min(6.0, 0.5 * (2 ** (attempt - 1)))
                    logger.warning(
                        "OpenDota server error %d. Retrying (%d/%d in %.1fs)...",
                        response.status_code,
                        attempt,
                        self.max_retries,
                        backoff,
                    )
                    self._reset_session()
                    time.sleep(backoff)
                    continue
                raise OpenDotaServerError(
                    response.status_code,
                    f"OpenDota server error {response.status_code} after {self.max_retries} attempts.",
                    response.text,
                )
            if not response.ok:
                raise OpenDotaHttpError(
                    response.status_code,
                    f"Unexpected HTTP response code {response.status_code}.",
                    response.text,
                )

            try:
                return response.json()
            except ValueError as e:
                raise OpenDotaError(f"Failed to decode JSON from OpenDota response: {e}") from e

        if last_error is not None:
            raise OpenDotaError(
                f"OpenDota request failed after {self.max_retries} attempts: {last_error}"
            ) from last_error

    def get_public_matches(
        self,
        less_than_match_id: int | None = None,
        min_rank: int | None = None,
        max_rank: int | None = None,
    ) -> list[PublicMatch]:
        """Получает и валидирует список публичных матчей (около 100 записей за вызов).

        Args:
            less_than_match_id: Пагинация: матчи с ID меньше указанного.
            min_rank: Минимальный ранг матча (10-15: Herald, 20-25: Guardian, 30-35: Crusader,
                      40-45: Archon, 50-55: Legend, 60-65: Ancient, 70-75: Divine, 80: Immortal).
            max_rank: Максимальный ранг матча (аналогично min_rank).

        Returns:
            list[PublicMatch]: Список валидированных объектов публичных матчей.

        Raises:
            OpenDotaValidationError: Если структура ответа не соответствует схеме PublicMatch.
        """
        params: dict[str, Any] = {}
        if less_than_match_id is not None:
            params["less_than_match_id"] = less_than_match_id
        if min_rank is not None:
            params["min_rank"] = min_rank
        if max_rank is not None:
            params["max_rank"] = max_rank

        result = self.get("publicMatches", params=params)
        if not isinstance(result, list):
            raise OpenDotaValidationError(f"Expected list response for publicMatches, got {type(result)}.")

        try:
            return [PublicMatch.model_validate(item) for item in result]
        except ValidationError as e:
            raise OpenDotaValidationError(f"Invalid publicMatches schema: {e}") from e

    def get_pro_matches(
        self,
        less_than_match_id: int | None = None,
    ) -> list[ProMatch]:
        """Получает и валидирует список профессиональных матчей с турниров.

        Args:
            less_than_match_id: Пагинация: матчи с ID меньше указанного.

        Returns:
            list[ProMatch]: Список валидированных объектов профессиональных матчей.

        Raises:
            OpenDotaValidationError: Если структура ответа не соответствует схеме ProMatch.
        """
        params: dict[str, Any] = {}
        if less_than_match_id is not None:
            params["less_than_match_id"] = less_than_match_id

        result = self.get("proMatches", params=params)
        if not isinstance(result, list):
            raise OpenDotaValidationError(f"Expected list response for proMatches, got {type(result)}.")

        try:
            return [ProMatch.model_validate(item) for item in result]
        except ValidationError as e:
            raise OpenDotaValidationError(f"Invalid proMatches schema: {e}") from e

    def close(self) -> None:
        """Освобождает ресурсы сессии."""
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> OpenDotaClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
