"""Stratz API Client.

Простая обёртка над requests, которая таскает API-токен и даёт
готовый метод для получения данных по матчам из вашего сниппета.
Поддерживает детальный разбор ошибок на каждый матч в батче при HTTP 200.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import requests

try:
    from src import config
except ImportError:
    import config  # type: ignore


# Шаблон полей матча из вашего сниппета
MATCH_QUERY_FIELDS = """
didRadiantWin
durationSeconds
startDateTime
endDateTime
lobbyType
numHumanPlayers
gameMode
actualRank
averageRank
analysisOutcome
bottomLaneOutcome
midLaneOutcome
topLaneOutcome
pickBans {
  isPick
  heroId
  bannedHeroId
  isRadiant
  wasBannedSuccessfully
  order
}
players {
  heroId
  isRadiant
  position
  roleBasic
  role
  lane
  kills
  deaths
  assists
  item0Id
  item1Id
  item2Id
  item3Id
  item4Id
  item5Id
  level
  goldPerMinute
  experiencePerMinute
  networth
  numLastHits
  numDenies
  heroDamage
  towerDamage
  intentionalFeeding
  award
  imp
}
"""


@dataclass
class StratzBatchResult:
    """Результат пакетного запроса к Stratz GraphQL.

    - enriched: успешно обогащённые валидные матчи.
    - failed: словарь {match_id: причина_ошибки} для отклонённых/не распарсенных матчей.
    """

    enriched: List[Dict[str, Any]] = field(default_factory=list)
    failed: Dict[int, str] = field(default_factory=dict)


class Stratz:
    """Простая обёртка для работы со Stratz."""

    URL: str = getattr(config, "STRATZ_API_URL", "https://api.stratz.com/graphql")

    def __init__(
        self,
        token: Optional[str] = None,
        timeout: float = getattr(config, "STRATZ_TIMEOUT", 30.0),
    ) -> None:
        """Хранит токен и переиспользует сессию с нужными заголовками."""
        self.token = token or getattr(config, "STRATZ_TOKEN", "")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "User-Agent": "STRATZ_API",
            }
        )

    def query_raw(self, query_str: str) -> Dict[str, Any]:
        """Отправить любой GraphQL запрос и получить полный JSON-ответ (data и errors)."""
        response = self.session.post(self.URL, json={"query": query_str}, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}

    def query(self, query_str: str) -> Dict[str, Any]:
        """Отправить любой GraphQL запрос и получить 'data' (совместимость)."""
        data = self.query_raw(query_str)
        if "errors" in data and not data.get("data"):
            raise ValueError(f"Stratz GraphQL Error: {data['errors']}")
        return data.get("data") or {}

    def get_matches_info(self, match_ids: List[int], batch_size: int = config.ENRICHER_CHUNK_SIZE) -> StratzBatchResult:
        """Пакетный запрос матчей: сопоставляет валидные матчи и ошибки по каждому ID при HTTP 200.

        Извлекает ошибки из GraphQL errors по path: ['m{mid}', ...] и валидирует наличие данных.
        """
        if not match_ids:
            return StratzBatchResult()

        enriched_results: List[Dict[str, Any]] = []
        failed_results: Dict[int, str] = {}

        # Бьём по batch_size, чтобы Stratz не захлебнулся от слишком длинного запроса
        for i in range(0, len(match_ids), batch_size):
            chunk = match_ids[i : i + batch_size]
            fragments = [f"m{mid}: match(id: {mid}) {{\n{MATCH_QUERY_FIELDS}\n}}" for mid in chunk]
            full_query = "query {\n" + "\n".join(fragments) + "\n}"

            resp_json = self.query_raw(full_query)

            # Если объект 'data' отсутствует вовсе — это глобальный сбой выполнения запроса, а не проблемы отдельных матчей!
            if resp_json.get("data") is None:
                errors = resp_json.get("errors") or []
                err_msgs = [str(e.get("message") or e) for e in errors]
                combined = "; ".join(err_msgs)
                combined_lower = combined.lower()
                if any(kw in combined_lower for kw in ("rate limit", "quota", "throttle", "429")):
                    dummy_resp = requests.Response()
                    dummy_resp.status_code = 429
                    raise requests.HTTPError(f"Stratz GraphQL Rate Limit: {combined}", response=dummy_resp)
                raise ValueError(f"Stratz GraphQL Error (no data): {combined}")

            # 1. Извлекаем ошибки на каждый элемент из GraphQL 'errors'
            errors_by_match: Dict[int, str] = {}
            general_errors: List[str] = []
            for err in resp_json.get("errors") or []:
                msg = str(err.get("message") or "Unknown GraphQL error")
                path = err.get("path")
                if path and isinstance(path, list) and len(path) > 0:
                    alias = str(path[0])
                    if alias.startswith("m") and alias[1:].isdigit():
                        mid = int(alias[1:])
                        # Если для этого матча уже была ошибка, объединяем через точку с запятой
                        if mid in errors_by_match:
                            errors_by_match[mid] = f"{errors_by_match[mid]}; {msg}"
                        else:
                            errors_by_match[mid] = msg
                else:
                    general_errors.append(msg)

            data = resp_json.get("data") or {}

            # 2. Проверяем каждый матч в батче
            for mid in chunk:
                match_data = data.get(f"m{mid}")

                # Валидным считаем только непустой, распарсенный матч (есть победитель и игроки)
                if (
                    isinstance(match_data, dict)
                    and match_data.get("didRadiantWin") is not None
                    and isinstance(match_data.get("players"), list)
                    and len(match_data["players"]) > 0
                ):
                    match_data["match_id"] = mid
                    enriched_results.append(match_data)
                else:
                    # Матч вернул null, ошибку или пустые поля
                    err_msg = errors_by_match.get(mid)
                    if not err_msg:
                        if general_errors:
                            err_msg = f"GraphQL Error: {'; '.join(general_errors)}"
                        elif match_data is None:
                            err_msg = "Match returned null (not found or replay not parsed)"
                        elif not isinstance(match_data, dict):
                            err_msg = f"Unexpected match data type: {type(match_data)}"
                        elif match_data.get("didRadiantWin") is None:
                            err_msg = "Match unparsed (didRadiantWin is null)"
                        elif not match_data.get("players"):
                            err_msg = "Match unparsed (players list is empty)"
                        else:
                            err_msg = "Match failed validation"
                    failed_results[mid] = err_msg

        return StratzBatchResult(enriched=enriched_results, failed=failed_results)
