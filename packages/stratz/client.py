from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .exceptions import (
    StratzAuthError,
    StratzError,
    StratzGraphQLError,
    StratzHttpError,
    StratzRateLimitError,
    StratzServerError,
)
from .queries import MATCH_QUERY_FIELDS, build_batch_matches_query

logger = logging.getLogger("packages.stratz.client")


@dataclass
class StratzBatchResult:
    """Контейнер результатов пакетного запроса матчей к Stratz GraphQL API.

    Attributes:
        enriched: Список успешно полученных словарей данных матчей.
        failed: Словарь соответствия {match_id: причина_ошибки} для ненайденных или отклонённых матчей.
    """

    enriched: list[dict[str, Any]] = field(default_factory=list)
    failed: dict[int, str] = field(default_factory=dict)


class StratzClient:
    """Отказоустойчивый GraphQL-клиент к Stratz API (https://api.stratz.com/graphql).

    Архитектурные принципы:
    1. Изоляция сессии: использует собственный пул соединений requests.Session
       с автоматическими ретраями на временные сетевые и серверные сбои (500, 502, 503, 504).
    2. Прозрачность лимитов: при получении HTTP 429 или throttling сообщений в GraphQL errors
       немедленно бросает StratzRateLimitError без скрытых блокирующих пауз.
    3. Динамическая аутентификация: поддерживает статический api_token и token_provider
       для бесшовной ротации ключей в рантайме.
    4. Точечная изоляция ошибок в батче: для мульти-матчевых GraphQL-запросов сопоставляет
       частичные ошибки GraphQL (path: ['m<id>', ...]) с конкретным match_id, позволяя
       успешно забрать валидные матчи из той же пачки.
    5. Универсальность: метод execute() позволяет выполнять любые произвольные GraphQL-запросы.
    """

    def __init__(
        self,
        token_provider: Callable[[], str],
        base_url: str = "https://api.stratz.com/graphql",
        timeout: float = 30.0,
        max_retries: int = 3,
        session: requests.Session | None = None,
    ) -> None:
        self.token_provider = token_provider
        self.base_url = base_url
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
            allowed_methods=["POST"],
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

    def _resolve_token(self) -> str:
        token = self.token_provider()
        if not token or not token.strip():
            raise StratzAuthError("Stratz API token is missing or empty")
        return token.strip()

    def execute_raw(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Выполняет GraphQL-запрос и возвращает полный JSON-ответ (data и errors).

        Args:
            query: Строка запроса GraphQL.
            variables: Словарь переменных запроса.

        Returns:
            dict[str, Any]: Необработанный ответ API, содержащий ключи 'data' и/или 'errors'.

        Raises:
            StratzRateLimitError: При HTTP 429 или throttling сообщении в ответе.
            StratzAuthError: При HTTP 401/403 или ошибке авторизации токена.
            StratzServerError: При HTTP 5xx ошибках серверов после исчерпания попыток.
            StratzHttpError: При прочих HTTP ошибках.
            StratzGraphQLError: При отсутствии 'data' и наличии ошибок в 'errors'.
            StratzError: При сетевых сбоях и ошибках парсинга JSON.
        """
        token = self._resolve_token()
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": "STRATZ_API",
            "Authorization": f"Bearer {token}",
            "Connection": "close",
        }

        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        last_error: Exception | None = None
        resp: requests.Response | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.post(
                    self.base_url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                )
            except (requests.RequestException, OSError) as err:
                last_error = err
                if attempt < self.max_retries:
                    backoff = min(6.0, 0.5 * (2 ** (attempt - 1)))
                    logger.warning(
                        "Stratz transport error (%s). Recreating session and retrying (%d/%d in %.1fs)...",
                        err,
                        attempt,
                        self.max_retries,
                        backoff,
                    )
                    self._reset_session()
                    time.sleep(backoff)
                    continue
                raise StratzError(
                    f"Connection failure while querying Stratz after {self.max_retries} attempts: {err}"
                ) from err

            match resp.status_code:
                case 200:
                    break
                case 429:
                    raise StratzRateLimitError(
                        message="HTTP 429 Rate Limit Exceeded",
                        status_code=429,
                        body=resp.text,
                    )
                case 401 | 403:
                    raise StratzAuthError(
                        message=f"Authentication failed with status {resp.status_code}",
                        status_code=resp.status_code,
                        body=resp.text,
                    )
                case code if 500 <= code <= 599:
                    if attempt < self.max_retries:
                        backoff = min(6.0, 0.5 * (2 ** (attempt - 1)))
                        logger.warning(
                            "Stratz server error %d. Retrying (%d/%d in %.1fs)...",
                            code,
                            attempt,
                            self.max_retries,
                            backoff,
                        )
                        self._reset_session()
                        time.sleep(backoff)
                        continue
                    raise StratzServerError(
                        status_code=code,
                        message=f"Stratz server error {code} after {self.max_retries} attempts",
                        body=resp.text,
                    )
                case code:
                    raise StratzHttpError(
                        status_code=code,
                        message=f"Stratz request failed with status {code}",
                        body=resp.text,
                    )

        if resp is None:
            raise StratzError(f"Stratz request failed after {self.max_retries} attempts: {last_error}")

        try:
            data = resp.json()
        except ValueError as err:
            raise StratzError(f"Failed to decode Stratz JSON response: {err}") from err

        if not isinstance(data, dict):
            raise StratzError(f"Unexpected response format: expected JSON object, got {type(data).__name__}")

        if "data" not in data or data["data"] is None:
            errors = data.get("errors") or []
            err_msgs = [str(e.get("message") if isinstance(e, dict) else e) for e in errors]
            combined = "; ".join(err_msgs)
            combined_lower = combined.lower()

            if any(kw in combined_lower for kw in ("rate limit", "quota", "throttle", "429")):
                raise StratzRateLimitError(message=f"Stratz rate limit: {combined}")
            if any(kw in combined_lower for kw in ("unauthorized", "unauthenticated", "forbidden")):
                raise StratzAuthError(message=f"Stratz authentication error: {combined}")

            raise StratzGraphQLError(message=f"Stratz GraphQL query failed: {combined}", errors=errors)

        return data

    def execute(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Выполняет GraphQL-запрос и возвращает блок данных 'data'.

        Args:
            query: Строка запроса GraphQL.
            variables: Словарь переменных запроса.

        Returns:
            dict[str, Any]: Содержимое поля 'data'.
        """
        payload = self.execute_raw(query=query, variables=variables)
        return payload.get("data") or {}

    def get_matches(
        self,
        match_ids: list[int],
        batch_size: int = 10,
        fields: str = MATCH_QUERY_FIELDS,
    ) -> StratzBatchResult:
        """Выполняет пакетный запрос детализированной информации о матчах.

        Делит список match_ids на чанки размера batch_size, формирует алиасированные
        GraphQL-запросы и парсит результаты с точечной локализацией ошибок по каждому матчу.

        Args:
            match_ids: Список идентификаторов матчей Dota 2.
            batch_size: Количество матчей в одном GraphQL-запросе (рекомендуется 10-15).
            fields: Строка полей GraphQL для выборки данных матча.

        Returns:
            StratzBatchResult: Объект со списками успешно обогащенных матчей и описанием ошибок.
        """
        if not match_ids:
            return StratzBatchResult()

        enriched_results: list[dict[str, Any]] = []
        failed_results: dict[int, str] = {}

        for i in range(0, len(match_ids), batch_size):
            chunk = match_ids[i : i + batch_size]
            query = build_batch_matches_query(chunk, fields=fields)
            resp_json = self.execute_raw(query)

            errors_by_match: dict[int, str] = {}
            general_errors: list[str] = []

            for err in resp_json.get("errors") or []:
                if not isinstance(err, dict):
                    general_errors.append(str(err))
                    continue

                msg = str(err.get("message") or "Unknown GraphQL error")
                path = err.get("path")
                if path and isinstance(path, list) and len(path) > 0:
                    alias = str(path[0])
                    if alias.startswith("m") and alias[1:].isdigit():
                        mid = int(alias[1:])
                        if mid in errors_by_match:
                            errors_by_match[mid] = f"{errors_by_match[mid]}; {msg}"
                        else:
                            errors_by_match[mid] = msg
                        continue
                general_errors.append(msg)

            data = resp_json.get("data") or {}

            for mid in chunk:
                match_data = data.get(f"m{mid}")
                if (
                    isinstance(match_data, dict)
                    and match_data.get("didRadiantWin") is not None
                    and isinstance(match_data.get("players"), list)
                    and len(match_data["players"]) > 0
                ):
                    match_data["match_id"] = mid
                    match_data.setdefault("id", mid)
                    enriched_results.append(match_data)
                else:
                    err_msg = errors_by_match.get(mid)
                    if not err_msg:
                        if general_errors:
                            err_msg = f"GraphQL Error: {'; '.join(general_errors)}"
                        elif match_data is None:
                            err_msg = "Match returned null (not found or replay not parsed)"
                        elif not isinstance(match_data, dict):
                            err_msg = f"Unexpected match data type: {type(match_data).__name__}"
                        elif match_data.get("didRadiantWin") is None:
                            err_msg = "Match unparsed (didRadiantWin is null)"
                        elif not match_data.get("players"):
                            err_msg = "Match unparsed (players list is empty)"
                        else:
                            err_msg = "Match failed structural validation"
                    failed_results[mid] = err_msg

        return StratzBatchResult(enriched=enriched_results, failed=failed_results)

    def get_match(
        self,
        match_id: int,
        fields: str = MATCH_QUERY_FIELDS,
    ) -> dict[str, Any] | None:
        """Получает детализированную информацию об одном матче.

        Args:
            match_id: Идентификатор матча Dota 2.
            fields: Строка полей GraphQL.

        Returns:
            dict[str, Any] | None: Словарь данных матча или None, если матч не распарсен/не найден.
        """
        result = self.get_matches([match_id], batch_size=1, fields=fields)
        if result.enriched:
            return result.enriched[0]
        return None

    def close(self) -> None:
        """Освобождает ресурсы HTTP-сессии."""
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> StratzClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
