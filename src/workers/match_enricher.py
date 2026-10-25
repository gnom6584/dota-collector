"""Стейт-машина обогатителя матчей (Match Enricher).

Состояния:
- `Idle`: ожидание появления созревших матчей в `raw_matches` (возраст >= min_age_hours).
- `Enriching`: активное пошаговое обогащение созревших матчей через Stratz GraphQL API.
- `WaitForLimits`: ожидание остывания лимитов API Stratz (HTTP 429).

Логика переходов реализована через `match state:`. Воркер сам сохраняет обогащённые матчи
в `enriched_matches`, удаляет обработанные из `raw_matches` и применяет FIFO-ротацию.
Внутри нет вызовов `time.sleep` — сон контролируется раннером снаружи.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Tuple, Union

import requests

try:
    from src import config
    from src.clients.stratz import Stratz, StratzBatchResult
    from src.api.database import check_daily_limit, increment_daily_requests, record_api_request
except ImportError:
    import config  # type: ignore
    from stratz import Stratz, StratzBatchResult  # type: ignore
    try:
        from api.database import check_daily_limit, increment_daily_requests, record_api_request  # type: ignore
    except ImportError:
        def record_api_request(*args, **kwargs):  # type: ignore
            pass
        def check_daily_limit(*args, **kwargs):  # type: ignore
            return False, 0, None
        def increment_daily_requests(*args, **kwargs):  # type: ignore
            return 0

logger = logging.getLogger("match_enricher")


class EnricherStatus(str, Enum):
    """Типы состояний стейт-машины обогатителя."""

    IDLE = "IDLE"
    ENRICHING = "ENRICHING"
    WAIT_FOR_LIMITS = "WAIT_FOR_LIMITS"
    DAILY_LIMIT_EXCEEDED = "DAILY_LIMIT_EXCEEDED"


@dataclass
class BaseEnricherState:
    """Базовый класс состояния обогатителя."""

    status: EnricherStatus = EnricherStatus.IDLE

    def to_json(self) -> str:
        """Сериализация состояния в JSON строку."""
        return json.dumps(
            {
                "status": self.status.value,
            },
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, json_str: str) -> EnricherState:
        """Десериализация JSON в соответствующий класс состояния."""
        if not json_str or json_str == "{}":
            return Idle()
        try:
            data: Dict[str, Any] = json.loads(json_str)
            status_str = data.get("status", EnricherStatus.IDLE.value)

            match status_str:
                case EnricherStatus.ENRICHING.value:
                    return Enriching()
                case EnricherStatus.WAIT_FOR_LIMITS.value:
                    return WaitForLimits()
                case EnricherStatus.DAILY_LIMIT_EXCEEDED.value:
                    return DailyLimitExceeded()
                case _:
                    return Idle()
        except Exception:
            return Idle()


@dataclass
class Idle(BaseEnricherState):
    """Дефолтное состояние ожидания созревших матчей в таблице raw_matches."""

    status: EnricherStatus = EnricherStatus.IDLE


@dataclass
class Enriching(BaseEnricherState):
    """Состояние активного обогащения созревших матчей."""

    status: EnricherStatus = EnricherStatus.ENRICHING


@dataclass
class WaitForLimits(BaseEnricherState):
    """Состояние ожидания при исчерпании лимитов API Stratz (HTTP 429)."""

    status: EnricherStatus = EnricherStatus.WAIT_FOR_LIMITS


@dataclass
class DailyLimitExceeded(BaseEnricherState):
    """Состояние ожидания при исчерпании внутреннего дневного лимита запросов к Stratz."""

    status: EnricherStatus = EnricherStatus.DAILY_LIMIT_EXCEEDED


EnricherState = Union[Idle, Enriching, WaitForLimits, DailyLimitExceeded]


def get_ripe_matches(
    conn: sqlite3.Connection,
    min_age_seconds: float,
    limit: int = config.ENRICHER_CHUNK_SIZE,
) -> List[Tuple[int, str]]:
    """Получить самые старые созревшие матчи из таблицы raw_matches.

    Матч считается созревшим, если разница между текущим временем UTC и
    created_at >= min_age_seconds (время на парсинг реплеев в Dota 2).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=min_age_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        """
        SELECT match_id, data FROM raw_matches
        WHERE created_at <= ?
        ORDER BY created_at ASC, match_id ASC
        LIMIT ?
        """,
        (cutoff, max(1, limit)),
    ).fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def enforce_enriched_limit(
    conn: sqlite3.Connection,
    max_records: int = config.ENRICHED_MAX_MATCHES,
) -> int:
    """Удаляет старейшие записи из enriched_matches (FIFO-ротация), если превышен лимит.

    Лимит 0 означает хранить ровно 0 записей (полная очистка).
    """
    if max_records < 0:
        return 0

    with conn:
        if max_records == 0:
            cursor = conn.execute("DELETE FROM enriched_matches")
            deleted = cursor.rowcount
            if deleted > 0:
                logger.info("Лимит enriched_matches = 0: удалены все %d записей.", deleted)
            return deleted

        cursor = conn.execute(
            """
            DELETE FROM enriched_matches
            WHERE match_id NOT IN (
                SELECT match_id FROM enriched_matches
                ORDER BY created_at DESC, match_id DESC
                LIMIT ?
            )
            """,
            (max_records,),
        )
        deleted = cursor.rowcount
        if deleted > 0:
            logger.info("FIFO-ротация enriched_matches: удалено %d старых записей (лимит: %d).", deleted, max_records)
        return deleted


def delete_raw_matches(conn: sqlite3.Connection, match_ids: List[int]) -> None:
    """Удалить матчи из очереди raw_matches."""
    if not match_ids:
        return
    placeholders = ",".join("?" * len(match_ids))
    with conn:
        conn.execute(
            f"DELETE FROM raw_matches WHERE match_id IN ({placeholders})",
            match_ids,
        )


def save_enriched_and_pop_raw(
    conn: sqlite3.Connection,
    enriched_chunk: List[Dict[str, Any]],
    requested_ids: List[int],
    max_enriched_matches: int = config.ENRICHED_MAX_MATCHES,
) -> None:
    """Атомарно сохраняет валидные обогащённые матчи в enriched_matches и удаляет запрошенные ID из raw_matches."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    records = [
        (m["match_id"], json.dumps(m, ensure_ascii=False), now_utc)
        for m in enriched_chunk
        if isinstance(m.get("match_id"), int)
    ]

    with conn:
        # В enriched_matches сохраняются только валидные распарсенные матчи
        if records:
            conn.executemany(
                """
                INSERT OR REPLACE INTO enriched_matches (match_id, data, created_at)
                VALUES (?, ?, ?)
                """,
                records,
            )
            enforce_enriched_limit(conn, max_enriched_matches)

        # Удаляем все запрошенные ID из очереди raw_matches
        if requested_ids:
            placeholders = ",".join("?" * len(requested_ids))
            conn.execute(
                f"DELETE FROM raw_matches WHERE match_id IN ({placeholders})",
                requested_ids,
            )


def step(
    state: EnricherState,
    client: Stratz,
    conn: sqlite3.Connection,
    min_age_hours: float = config.ENRICHER_MIN_AGE_HOURS,
    chunk_size: int = config.ENRICHER_CHUNK_SIZE,
    max_enriched_matches: int = config.ENRICHED_MAX_MATCHES,
) -> EnricherState:
    """Один шаг стейт-машины обогатителя через match/case.

    - В Idle проверяет наличие созревших матчей в raw_matches: если есть, переходит в Enriching.
    - В WaitForLimits возобновляет обогащение (раннер уже поспал снаружи).
    - В Enriching берёт порцию созревших матчей, запрашивает Stratz, сохраняет валидные в enriched_matches
      и удаляет запрошенные из raw_matches. При ошибке Stratz сразу сбрасывает батч, чтобы не зависнуть.
    """
    min_age_seconds = max(0.0, min_age_hours * 3600.0)
    chunk_size = max(1, chunk_size)

    # 0. Проверяем внутренний дневной лимит запросов воркера к Stratz
    is_daily_exceeded, daily_used, daily_lim = check_daily_limit(conn, "enricher_config")
    if is_daily_exceeded:
        if not isinstance(state, DailyLimitExceeded):
            logger.warning(
                "Достигнут внутренний дневной лимит запросов к Stratz (%d/%d). Переход в DAILY_LIMIT_EXCEEDED.",
                daily_used,
                daily_lim,
            )
        return DailyLimitExceeded()

    # Если мы были в DailyLimitExceeded, но лимит был сброшен в UI или наступил новый день:
    if isinstance(state, DailyLimitExceeded):
        logger.info("Дневной лимит запросов Stratz сброшен/возобновлён. Переход в Enriching.")
        return Enriching()

    match state:
        case DailyLimitExceeded():
            return state

        case Idle():
            ripe = get_ripe_matches(conn, min_age_seconds, limit=chunk_size)
            if not ripe:
                return state  # Всё ещё ждём созревания
            logger.info("Обнаружены созревшие матчи (найдено %d). Переход Idle -> Enriching.", len(ripe))
            return Enriching()

        case WaitForLimits():
            # Раннер уже выдержал паузу снаружи, возобновляем обогащение
            logger.info("Пауза лимитов Stratz завершена. Переход WaitForLimits -> Enriching.")
            return Enriching()

        case Enriching():
            if not client.token or not client.token.strip():
                logger.warning("STRATZ_TOKEN не задан. Ожидание указания токена в конфигурации...")
                return Idle()

            ripe = get_ripe_matches(conn, min_age_seconds, limit=chunk_size)
            if not ripe:
                logger.info("Созревших матчей больше нет. Переход Enriching -> Idle.")
                return Idle()

            chunk_ids = [m[0] for m in ripe]

            # Сетевой запрос к Stratz GraphQL
            try:
                batch_result: StratzBatchResult = client.get_matches_info(chunk_ids, batch_size=len(chunk_ids))
                increment_daily_requests(conn, "enricher_config")
                record_api_request(conn, "stratz", status_code=200)
            except requests.HTTPError as err:
                status_code = getattr(err.response, "status_code", 500)
                record_api_request(conn, "stratz", status_code=status_code, error=str(err))
                if status_code == 429:
                    logger.warning("Упёрлись в лимит Stratz (HTTP 429). Переход в WaitForLimits.")
                    return WaitForLimits()

                if status_code in (401, 403):
                    # Ошибка авторизации (неверный токен) — ни в коем случае НЕ удаляем матчи!
                    logger.error("Ошибка авторизации Stratz API (код %s). Проверьте STRATZ_TOKEN в конфигурации. Матчи остаются в очереди.", status_code)
                    return state

                if status_code and 400 <= status_code < 500:
                    # Stratz явно отказался давать информацию по запросу (400 Bad Request, 404 и т.д.)
                    logger.error("Stratz отклонил запрос (код %s): %s. Удаляем проблемный батч %s из очереди.", status_code, err, chunk_ids)
                    delete_raw_matches(conn, chunk_ids)
                    return Enriching()

                # Ошибки 5xx (500, 502, 503, 504) — временный сбой серверов Stratz, матчи НЕ удаляем!
                logger.error("Временный сбой серверов Stratz (код %s): %s. Матчи остаются в очереди.", status_code, err)
                return state
            except (requests.RequestException, OSError) as err:
                # Ошибка сети, DNS или таймаут — ни в коем случае НЕ удаляем матчи!
                record_api_request(conn, "stratz", status_code=0, error=str(err))
                logger.error("Сетевой сбой при обращении к Stratz: %s. Матчи остаются в очереди.", err)
                return state
            except Exception as err:
                # Любой другой непредвиденный сбой — матчи НЕ удаляем
                record_api_request(conn, "stratz", status_code=0, error=str(err))
                logger.error("Непредвиденная ошибка при запросе к Stratz: %s. Матчи остаются в очереди.", err)
                return state

            # Дополняем обогащённые матчи метаданными OpenDota (ранг, длительность, режим и т.д.) для сквозной фильтрации
            raw_meta_map: Dict[int, Dict[str, Any]] = {}
            for mid, raw_str in ripe:
                try:
                    if raw_str:
                        raw_meta_map[mid] = json.loads(raw_str)
                except Exception:
                    pass

            for item in batch_result.enriched:
                mid = item.get("match_id")
                raw_info = raw_meta_map.get(mid)
                if raw_info:
                    item.setdefault("opendota", raw_info)
                    if "avg_rank_tier" not in item and "avg_rank_tier" in raw_info:
                        item["avg_rank_tier"] = raw_info["avg_rank_tier"]
                    if "duration" not in item and "duration" in raw_info:
                        item["duration"] = raw_info["duration"]
                    elif "duration" not in item and "durationSeconds" in item:
                        item["duration"] = item["durationSeconds"]
                    if "radiant_win" not in item:
                        item["radiant_win"] = raw_info.get("radiant_win") if "radiant_win" in raw_info else item.get("didRadiantWin")
                    if "lobby_type" not in item:
                        item["lobby_type"] = raw_info.get("lobby_type") if raw_info.get("lobby_type") is not None else item.get("lobbyType")
                    if "game_mode" not in item:
                        item["game_mode"] = raw_info.get("game_mode") if raw_info.get("game_mode") is not None else item.get("gameMode")
                    if "start_time" not in item:
                        item["start_time"] = raw_info.get("start_time") if raw_info.get("start_time") is not None else item.get("startDateTime")

            # Сохраняем валидные матчи и удаляем запрошенные из raw_matches
            save_enriched_and_pop_raw(
                conn=conn,
                enriched_chunk=batch_result.enriched,
                requested_ids=chunk_ids,
                max_enriched_matches=max_enriched_matches,
            )

            # Логируем отклонённые матчи
            if batch_result.failed:
                for mid, err_msg in batch_result.failed.items():
                    logger.warning("Матч #%d отклонён Stratz: %s", mid, err_msg)

            logger.info(
                "Батч обработан: +%d обогащено, %d отклонено (запрошено %d).",
                len(batch_result.enriched),
                len(batch_result.failed),
                len(chunk_ids),
            )

            return Enriching()
