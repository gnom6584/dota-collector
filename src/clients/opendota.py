"""OpenDota API Client.

Модуль для получения списка публичных матчей Dota 2 через OpenDota API.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict
import requests

try:
    from src import config
except ImportError:
    import config  # type: ignore


class PublicMatch(TypedDict):
    """Схема объекта матча, возвращаемого эндпоинтом GET /publicMatches."""

    match_id: int
    match_seq_num: int
    radiant_win: bool
    start_time: int
    duration: int
    lobby_type: int
    game_mode: int
    avg_rank_tier: Optional[int]
    num_rank_tier: Optional[int]
    cluster: int
    radiant_team: List[int]
    dire_team: List[int]


class OpenDotaError(Exception):
    """Базовое исключение для ошибок OpenDota API."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response: Optional[requests.Response] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class OpenDota:
    """Клиент OpenDota для получения публичных матчей."""

    BASE_URL: str = getattr(config, "OPENDOTA_API_URL", "https://api.opendota.com/api")

    def __init__(
        self,
        api_key: Optional[str] = getattr(config, "OPENDOTA_API_KEY", None),
        session: Optional[requests.Session] = None,
        timeout: float = getattr(config, "OPENDOTA_TIMEOUT", 30.0),
    ) -> None:
        """Инициализация клиента.

        :param api_key: Опциональный API-ключ OpenDota (увеличивает лимиты).
        :param session: Опциональная сессия requests.Session.
        :param timeout: Таймаут запросов в секундах.
        """
        self.api_key = api_key
        self.timeout = timeout
        self.session = session or requests.Session()
        self._owns_session = session is None

        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    def __enter__(self) -> OpenDota:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """Закрыть HTTP-сессию."""
        if self._owns_session:
            self.session.close()

    def public_matches(
        self,
        less_than_match_id: Optional[int] = None,
        min_rank: Optional[int] = None,
        max_rank: Optional[int] = None,
    ) -> List[PublicMatch]:
        """Получить список случайно выбранных публичных рейтинговых матчей (GET /publicMatches).

        :param less_than_match_id: Получить матчи с ID меньше указанного (для пагинации назад по истории).
        :param min_rank: Минимальный ранг матча (10-15: Herald, 20-25: Guardian, ..., 80: Immortal).
        :param max_rank: Максимальный ранг матча.
        :return: Список структурированных словарей PublicMatch.
        """
        params: Dict[str, Any] = {
            "less_than_match_id": less_than_match_id,
            "min_rank": min_rank,
            "max_rank": max_rank,
        }
        clean_params = {k: v for k, v in params.items() if v is not None}

        if self.api_key and "api_key" not in clean_params:
            clean_params["api_key"] = self.api_key

        url = f"{self.BASE_URL}/publicMatches"

        try:
            response = self.session.get(url, params=clean_params, timeout=self.timeout)
            response.raise_for_status()
            data: List[PublicMatch] = response.json()
            return data
        except requests.HTTPError as err:
            raise OpenDotaError(
                f"HTTP {err.response.status_code}: {err.response.text}",
                status_code=err.response.status_code,
                response=err.response,
            ) from err
        except requests.RequestException as err:
            raise OpenDotaError(f"Сетевая ошибка OpenDota: {err}") from err
