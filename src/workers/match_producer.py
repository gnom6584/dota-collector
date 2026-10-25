"""Стейт-машина сборщика публичных матчей (Match Producer).

Состояния:
- `Idle`: ожидание освобождения места в БД (буфер `raw_matches` заполнен >= buffer_size).
- `Collecting`: активный сбор матчей из OpenDota API и сохранение в таблицу `raw_matches`.
- `WaitForLimits`: ожидание остывания лимитов API OpenDota (HTTP 429).

Логика переходов реализована через `match state:`. Воркер сам сохраняет матчи в БД
и не содержит логики вычисления сна (сон контролируется раннером снаружи).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

try:
    from src import config
    from src.clients.opendota import OpenDota, OpenDotaError, PublicMatch
except ImportError:
    import config  # type: ignore
    from opendota import OpenDota, OpenDotaError, PublicMatch  # type: ignore

logger = logging.getLogger("match_producer")


class ProducerStatus(str, Enum):
    """Типы состояний стейт-машины продюсера."""

    IDLE = "IDLE"
    COLLECTING = "COLLECTING"
    WAIT_FOR_LIMITS = "WAIT_FOR_LIMITS"
    DAILY_LIMIT_EXCEEDED = "DAILY_LIMIT_EXCEEDED"


@dataclass
class BaseState:
    """Базовый класс состояния продюсера."""

    status: ProducerStatus = ProducerStatus.IDLE
    last_match_id: Optional[int] = None

    def to_json(self) -> str:
        """Сериализация состояния в JSON строку."""
        return json.dumps(
            {
                "status": self.status.value,
                "last_match_id": self.last_match_id,
            },
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, json_str: str) -> ProducerState:
        """Десериализация JSON в соответствующий класс состояния."""
        if not json_str or json_str == "{}":
            return Idle()
        try:
            data: Dict[str, Any] = json.loads(json_str)
            status_str = data.get("status", ProducerStatus.IDLE.value)

            raw_last_id = data.get("last_match_id")
            try:
                last_id = int(raw_last_id) if raw_last_id is not None else None
            except (ValueError, TypeError):
                last_id = None

            match status_str:
                case ProducerStatus.COLLECTING.value:
                    return Collecting(last_match_id=last_id)
                case ProducerStatus.WAIT_FOR_LIMITS.value:
                    return WaitForLimits(last_match_id=last_id)
                case ProducerStatus.DAILY_LIMIT_EXCEEDED.value:
                    return DailyLimitExceeded(last_match_id=last_id)
                case _:
                    return Idle(last_match_id=last_id)
        except Exception:
            return Idle()


@dataclass
class Idle(BaseState):
    """Дефолтное состояние ожидания слота в буфере raw_matches."""

    status: ProducerStatus = ProducerStatus.IDLE


@dataclass
class Collecting(BaseState):
    """Состояние активного сбора матчей в буфер БД."""

    status: ProducerStatus = ProducerStatus.COLLECTING


@dataclass
class WaitForLimits(BaseState):
    """Состояние ожидания при исчерпании лимитов API OpenDota (HTTP 429)."""

    status: ProducerStatus = ProducerStatus.WAIT_FOR_LIMITS


@dataclass
class DailyLimitExceeded(BaseState):
    """Состояние ожидания при исчерпании внутреннего дневного лимита запросов воркера."""

    status: ProducerStatus = ProducerStatus.DAILY_LIMIT_EXCEEDED


ProducerState = Union[Idle, Collecting, WaitForLimits, DailyLimitExceeded]


def get_raw_matches_count(conn: sqlite3.Connection) -> int:
    """Количество сырых матчей в буфере raw_matches."""
    row = conn.execute("SELECT COUNT(*) FROM raw_matches").fetchone()
    return int(row[0]) if row else 0


def save_raw_matches(conn: sqlite3.Connection, matches: List[PublicMatch]) -> int:
    """Сохранить сырые матчи в таблицу raw_matches (дубликаты игнорируются)."""
    records = [
        (m["match_id"], json.dumps(m, ensure_ascii=False))
        for m in matches
        if isinstance(m.get("match_id"), int) and m["match_id"] > 0
    ]
    if not records:
        return 0

    before = conn.total_changes
    with conn:
        conn.executemany("INSERT OR IGNORE INTO raw_matches (match_id, data) VALUES (?, ?)", records)
    return max(0, conn.total_changes - before)


def trim_raw_matches(conn: sqlite3.Connection, buffer_size: int) -> int:
    """Обрезает избыточные матчи в raw_matches, если их количество превышает buffer_size.
    
    Удаляет самые свежие излишки, не помещающиеся в буфер.
    """
    if buffer_size <= 0:
        return 0
    count = get_raw_matches_count(conn)
    excess = count - buffer_size
    if excess <= 0:
        return 0

    with conn:
        cursor = conn.execute(
            """
            DELETE FROM raw_matches
            WHERE match_id IN (
                SELECT match_id FROM raw_matches
                ORDER BY created_at DESC, match_id DESC
                LIMIT ?
            )
            """,
            (excess,),
        )
        deleted = cursor.rowcount
        if deleted > 0:
            logger.info("Буфер raw_matches обрезан до %d (удалено %d лишних матчей).", buffer_size, deleted)
        return deleted


try:
    from src.api.database import check_daily_limit, increment_daily_requests, record_api_request
except ImportError:
    try:
        from api.database import check_daily_limit, increment_daily_requests, record_api_request
    except ImportError:
        record_api_request = None
        def check_daily_limit(*args, **kwargs):  # type: ignore
            return False, 0, None
        def increment_daily_requests(*args, **kwargs):  # type: ignore
            return 0


def filter_match(
    m: PublicMatch,
    min_rank: Optional[int] = None,
    max_rank: Optional[int] = None,
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None,
    lobby_type: Optional[int] = None,
    game_mode: Optional[int] = None,
    min_start_time: Optional[int] = None,
    validator: Optional[Callable[[PublicMatch], bool]] = None,
) -> bool:
    """Проверяет соответствие матча OpenDota критериям фильтрации."""
    dur = m.get("duration")
    if min_duration is not None and min_duration > 0:
        if dur is None or dur < min_duration:
            return False
    if max_duration is not None and max_duration > 0:
        if dur is None or dur > max_duration:
            return False
    if lobby_type is not None and lobby_type >= 0:
        if m.get("lobby_type") != lobby_type:
            return False
    if game_mode is not None and game_mode >= 0:
        if m.get("game_mode") != game_mode:
            return False
    st = m.get("start_time")
    if min_start_time is not None and min_start_time > 0:
        if st is None or st < min_start_time:
            return False
    if validator:
        try:
            return bool(validator(m))
        except Exception as val_err:
            logger.warning("Ошибка валидатора для матча %s: %s", m.get("match_id"), val_err)
            return False
    return True


_filter_match = filter_match


def step(
    state: ProducerState,
    client: OpenDota,
    conn: sqlite3.Connection,
    buffer_size: int = config.PRODUCER_BUFFER_SIZE,
    min_rank: Optional[int] = config.PRODUCER_MIN_RANK,
    max_rank: Optional[int] = config.PRODUCER_MAX_RANK,
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None,
    lobby_type: Optional[int] = None,
    game_mode: Optional[int] = None,
    min_start_time: Optional[int] = None,
    validator: Optional[Callable[[PublicMatch], bool]] = None,
) -> ProducerState:
    """Один шаг стейт-машины продюсера через match/case.

    - В Idle проверяет место в БД: если свободно (< buffer_size), переходит в Collecting.
    - В WaitForLimits возобновляет сбор (раннер уже выдержал паузу снаружи).
    - В Collecting запрашивает матчи у OpenDota API, фильтрует по рангу, времени и параметрам,
      сразу сохраняет в raw_matches. Если буфер полон — переходит в Idle.
    """
    buffer_size = max(1, buffer_size)
    trim_raw_matches(conn, buffer_size)

    # 0. Проверяем внутренний дневной лимит запросов воркера к OpenDota
    is_daily_exceeded, daily_used, daily_lim = check_daily_limit(conn, "producer_config")
    if is_daily_exceeded:
        if not isinstance(state, DailyLimitExceeded):
            logger.warning(
                "Достигнут внутренний дневной лимит запросов к OpenDota (%d/%d). Переход в DAILY_LIMIT_EXCEEDED.",
                daily_used,
                daily_lim,
            )
        return DailyLimitExceeded(last_match_id=state.last_match_id)

    # Если мы были в DailyLimitExceeded, но лимит был сброшен в UI или наступил новый день:
    if isinstance(state, DailyLimitExceeded):
        logger.info("Дневной лимит запросов OpenDota сброшен/возобновлён. Переход в Collecting.")
        return Collecting(last_match_id=state.last_match_id)

    match state:
        case DailyLimitExceeded():
            return state

        case Idle(last_match_id=last_id):
            # Проверяем, освободилось ли место в буфере raw_matches
            if get_raw_matches_count(conn) >= buffer_size:
                return state  # Всё ещё ждём освобождения буфера
            logger.info("Буфер raw_matches свободен (< %d). Переход Idle -> Collecting.", buffer_size)
            return Collecting(last_match_id=last_id)

        case WaitForLimits(last_match_id=last_id):
            # Раннер уже выдержал паузу снаружи, проверяем БД и возобновляем сбор
            if get_raw_matches_count(conn) >= buffer_size:
                logger.info("Пауза лимитов завершена, но буфер raw_matches полон. Переход WaitForLimits -> Idle.")
                return Idle(last_match_id=last_id)
            logger.info("Пауза лимитов завершена. Переход WaitForLimits -> Collecting.")
            return Collecting(last_match_id=last_id)

        case Collecting(last_match_id=last_id):
            # 1. Если буфер БД уже заполнен — уходим в Idle
            current_count = get_raw_matches_count(conn)
            if current_count >= buffer_size:
                logger.info("Буфер raw_matches заполнен (%d/%d матчей). Переход Collecting -> Idle.", current_count, buffer_size)
                return Idle(last_match_id=last_id)

            # 2. Сетевой запрос к OpenDota API (с серверной фильтрацией по рангу)
            req_min_rank = min_rank if (min_rank is not None and min_rank > 0) else None
            req_max_rank = max_rank if (max_rank is not None and max_rank > 0) else None
            try:
                raw_matches = client.public_matches(
                    less_than_match_id=last_id,
                    min_rank=req_min_rank,
                    max_rank=req_max_rank,
                )
                increment_daily_requests(conn, "producer_config")
                if record_api_request:
                    record_api_request(conn, "opendota", status_code=200)
            except OpenDotaError as err:
                if record_api_request:
                    record_api_request(
                        conn,
                        "opendota",
                        status_code=err.status_code,
                        is_rate_limit=(err.status_code == 429),
                        is_error=(err.status_code != 429),
                    )
                if err.status_code == 429:
                    logger.warning("Упёрлись в лимит OpenDota (HTTP 429). Переход в WaitForLimits.")
                    return WaitForLimits(last_match_id=last_id)
                logger.error("Ошибка OpenDota API (код %s): %s", err.status_code, err)
                return state  # Ошибка на стейт не влияет, остаёмся в Collecting для повтора
            except Exception as err:
                if record_api_request:
                    record_api_request(conn, "opendota", is_error=True)
                logger.error("Сетевой сбой OpenDota: %s", err)
                return state  # Ошибка сети не ломает стейт

            if not raw_matches:
                logger.warning("Пустой ответ от OpenDota API.")
                return state

            # 3. Вычисление и валидация курсора по ВСЕМ полученным матчам (чтобы не застрять)
            match_ids = [m["match_id"] for m in raw_matches if isinstance(m.get("match_id"), int) and m["match_id"] > 0]
            if not match_ids:
                logger.warning("В ответе OpenDota нет валидных match_id.")
                return state
            new_last_id = min(match_ids)
            if last_id is not None and new_last_id >= last_id:
                logger.warning("Курсор OpenDota не уменьшился (%s >= %s). Принудительный сдвиг.", new_last_id, last_id)
                new_last_id = last_id - 1

            # 4. Фильтрация матчей по рангу, времени и параметрам OpenDota
            valid = [
                m for m in raw_matches
                if filter_match(
                    m,
                    min_rank=min_rank,
                    max_rank=max_rank,
                    min_duration=min_duration,
                    max_duration=max_duration,
                    lobby_type=lobby_type,
                    game_mode=game_mode,
                    min_start_time=min_start_time,
                    validator=validator,
                )
            ]

            # 5. Сохранение валидных матчей сразу в SQLite raw_matches с контролем емкости буфера
            current_count = get_raw_matches_count(conn)
            available_slots = max(0, buffer_size - current_count)
            if len(valid) > available_slots:
                valid = valid[:available_slots]

            inserted = save_raw_matches(conn, valid) if valid else 0
            trim_raw_matches(conn, buffer_size)
            new_count = get_raw_matches_count(conn)

            logger.info(
                "Получено %d матчей (под фильтры подошло: %d, новых: +%d). Всего в raw_matches: %d/%d (курсор < %s)",
                len(raw_matches),
                len(valid),
                inserted,
                new_count,
                buffer_size,
                new_last_id,
            )

            # 6. Если буфер заполнился — уходим в Idle
            if new_count >= buffer_size:
                logger.info("Буфер raw_matches полон (%d/%d). Переход Collecting -> Idle.", new_count, buffer_size)
                return Idle(last_match_id=new_last_id)

            return Collecting(last_match_id=new_last_id)
