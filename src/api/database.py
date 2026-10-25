"""Вспомогательные функции работы с SQLite базой данных для API и админки."""

from __future__ import annotations

import json
import logging
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src import config

logger = logging.getLogger("api_db")


def create_connection(db_path: str = config.DB_PATH) -> sqlite3.Connection:
    """Создать прямое подключение к SQLite с поддержкой многопоточности FastAPI."""
    dirname = os.path.dirname(db_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=config.DB_TIMEOUT_SEC,
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute(f"PRAGMA busy_timeout = {config.DB_BUSY_TIMEOUT_MS}")
    return conn


def get_db(db_path: str = config.DB_PATH):
    """FastAPI Dependency: открывает соединение и автоматически закрывает после завершения запроса."""
    conn = create_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()



def init_api_db(conn: sqlite3.Connection) -> None:
    """Инициализация всех таблиц базы данных и создание начальных конфигураций."""
    with conn:
        # Таблицы очередей и матчей
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_matches (
                match_id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS enriched_matches (
                match_id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )

        # Таблицы стейтов воркеров
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS producer_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute("INSERT OR IGNORE INTO producer_state (id, state_json) VALUES (1, '{}')")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS enricher_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute("INSERT OR IGNORE INTO enricher_state (id, state_json) VALUES (1, '{}')")

        # Таблицы конфигураций воркеров (по умолчанию на паузе running = 0)
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS producer_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                running INTEGER NOT NULL DEFAULT 0,
                api_key TEXT,
                min_rank INTEGER,
                max_rank INTEGER,
                buffer_size INTEGER NOT NULL DEFAULT {config.PRODUCER_BUFFER_SIZE},
                delay REAL NOT NULL DEFAULT {config.PRODUCER_DELAY},
                idle_delay REAL NOT NULL DEFAULT {config.PRODUCER_IDLE_DELAY},
                limit_sleep REAL NOT NULL DEFAULT {config.PRODUCER_LIMIT_SLEEP},
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO producer_config (
                id, running, api_key, min_rank, max_rank,
                buffer_size, delay, idle_delay, limit_sleep
            ) VALUES (1, 0, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                config.OPENDOTA_API_KEY,
                config.PRODUCER_MIN_RANK,
                config.PRODUCER_MAX_RANK,
                config.PRODUCER_BUFFER_SIZE,
                config.PRODUCER_DELAY,
                config.PRODUCER_IDLE_DELAY,
                config.PRODUCER_LIMIT_SLEEP,
            ),
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS enricher_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                running INTEGER NOT NULL DEFAULT 0,
                stratz_token TEXT,
                min_age_hours REAL NOT NULL DEFAULT {config.ENRICHER_MIN_AGE_HOURS},
                chunk_size INTEGER NOT NULL DEFAULT {config.ENRICHER_CHUNK_SIZE},
                poll_interval REAL NOT NULL DEFAULT {config.ENRICHER_POLL_INTERVAL},
                limit_sleep REAL NOT NULL DEFAULT {config.ENRICHER_LIMIT_SLEEP},
                chunk_delay REAL NOT NULL DEFAULT {config.ENRICHER_CHUNK_DELAY},
                max_enriched_matches INTEGER NOT NULL DEFAULT {config.ENRICHED_MAX_MATCHES},
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO enricher_config (
                id, running, stratz_token, min_age_hours, chunk_size,
                poll_interval, limit_sleep, chunk_delay, max_enriched_matches
            ) VALUES (1, 0, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                config.STRATZ_TOKEN,
                config.ENRICHER_MIN_AGE_HOURS,
                config.ENRICHER_CHUNK_SIZE,
                config.ENRICHER_POLL_INTERVAL,
                config.ENRICHER_LIMIT_SLEEP,
                config.ENRICHER_CHUNK_DELAY,
                config.ENRICHED_MAX_MATCHES,
            ),
        )

        # Таблицы авторизации API и админ-сессий
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                is_active INTEGER NOT NULL DEFAULT 1
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_sessions (
                token TEXT PRIMARY KEY,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )

        # Таблица подсчёта внешних обращений к API (OpenDota и Stratz)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS api_metrics (
                service TEXT PRIMARY KEY,
                total_requests INTEGER NOT NULL DEFAULT 0,
                success_requests INTEGER NOT NULL DEFAULT 0,
                error_requests INTEGER NOT NULL DEFAULT 0,
                rate_limit_hits INTEGER NOT NULL DEFAULT 0,
                last_request_at TEXT
            )
            """
        )
        conn.execute("INSERT OR IGNORE INTO api_metrics (service) VALUES ('opendota')")
        conn.execute("INSERT OR IGNORE INTO api_metrics (service) VALUES ('stratz')")

        # Автоматическая миграция колонок фильтрации для producer_config
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(producer_config)").fetchall()}
        for col_name, col_type in [
            ("min_rank", "INTEGER"),
            ("max_rank", "INTEGER"),
            ("min_duration", "INTEGER"),
            ("max_duration", "INTEGER"),
            ("lobby_type", "INTEGER"),
            ("game_mode", "INTEGER"),
            ("min_start_time", "INTEGER"),
            ("daily_request_limit", "INTEGER"),
            ("daily_requests_count", "INTEGER"),
            ("daily_requests_date", "TEXT"),
        ]:
            if col_name not in existing_cols:
                conn.execute(f"ALTER TABLE producer_config ADD COLUMN {col_name} {col_type} DEFAULT NULL")

        # Автоматическая миграция колонок дневного лимита для enricher_config
        existing_e_cols = {row[1] for row in conn.execute("PRAGMA table_info(enricher_config)").fetchall()}
        for col_name, col_type in [
            ("daily_request_limit", "INTEGER"),
            ("daily_requests_count", "INTEGER"),
            ("daily_requests_date", "TEXT"),
        ]:
            if col_name not in existing_e_cols:
                conn.execute(f"ALTER TABLE enricher_config ADD COLUMN {col_name} {col_type} DEFAULT NULL")

        # Если таблица пуста, создаём начальный мастер-ключ
        row = conn.execute("SELECT COUNT(*) FROM api_keys").fetchone()
        if not row or row[0] == 0:
            initial_key = f"dota_key_{secrets.token_hex(16)}"
            conn.execute(
                """
                INSERT INTO api_keys (key, name, created_at, is_active)
                VALUES (?, 'Default API Key', datetime('now'), 1)
                """,
                (initial_key,),
            )
            logger.info("Сгенерирован стартовый API ключ: %s", initial_key)


def record_api_request(
    conn: sqlite3.Connection,
    service: str,
    status_code: Optional[int] = None,
    is_rate_limit: bool = False,
    is_error: bool = False,
    error: Optional[Any] = None,
) -> None:
    """Увеличить счётчики вызовов внешнего API (OpenDota / Stratz)."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    is_limit = is_rate_limit or status_code == 429
    is_err = is_error or (status_code is not None and (status_code >= 400 or status_code == 0) and not is_limit) or (error is not None and not is_limit)
    is_succ = not is_limit and not is_err

    with conn:
        conn.execute(
            """
            INSERT INTO api_metrics (service, total_requests, success_requests, error_requests, rate_limit_hits, last_request_at)
            VALUES (?, 1, ?, ?, ?, ?)
            ON CONFLICT(service) DO UPDATE SET
                total_requests = total_requests + 1,
                success_requests = success_requests + excluded.success_requests,
                error_requests = error_requests + excluded.error_requests,
                rate_limit_hits = rate_limit_hits + excluded.rate_limit_hits,
                last_request_at = excluded.last_request_at
            """,
            (service, 1 if is_succ else 0, 1 if is_err else 0, 1 if is_limit else 0, now_utc),
        )


def get_api_metrics(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """Получить агрегированную статистику запросов к внешним API."""
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    rows = cur.execute("SELECT * FROM api_metrics").fetchall()
    res = {
        "opendota": {"total_requests": 0, "success_requests": 0, "successful_requests": 0, "error_requests": 0, "rate_limit_hits": 0, "last_request_at": None},
        "stratz": {"total_requests": 0, "success_requests": 0, "successful_requests": 0, "error_requests": 0, "rate_limit_hits": 0, "last_request_at": None},
    }
    for r in rows:
        svc = r["service"]
        res[svc] = {
            "total_requests": r["total_requests"],
            "success_requests": r["success_requests"],
            "successful_requests": r["success_requests"],
            "error_requests": r["error_requests"],
            "rate_limit_hits": r["rate_limit_hits"],
            "last_request_at": r["last_request_at"],
        }
    return res


def reset_api_metrics(conn: sqlite3.Connection, provider: Optional[str] = None, service: Optional[str] = None) -> None:
    """Сбросить счётчики запросов к API."""
    target = provider or service
    with conn:
        if target:
            conn.execute(
                "UPDATE api_metrics SET total_requests = 0, success_requests = 0, error_requests = 0, rate_limit_hits = 0, last_request_at = NULL WHERE service = ?",
                (target,),
            )
        else:
            conn.execute(
                "UPDATE api_metrics SET total_requests = 0, success_requests = 0, error_requests = 0, rate_limit_hits = 0, last_request_at = NULL",
            )


def check_daily_limit(conn: sqlite3.Connection, table_name: str) -> Tuple[bool, int, Optional[int]]:
    """Проверяет дневной лимит запросов для указанной таблицы (producer_config / enricher_config).

    Возвращает (is_exceeded, current_count, daily_limit).
    При смене календарного дня (UTC) автоматически сбрасывает счётчик на 0.
    """
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with conn:
        row = conn.execute(
            f"SELECT daily_request_limit, daily_requests_count, daily_requests_date FROM {table_name} WHERE id = 1"
        ).fetchone()
        if not row:
            return False, 0, None

        daily_limit = row[0]
        daily_count = row[1] or 0
        daily_date = row[2]

        if daily_date != today_utc:
            # Новый день: сбрасываем счётчик за день
            daily_count = 0
            conn.execute(
                f"UPDATE {table_name} SET daily_requests_count = 0, daily_requests_date = ? WHERE id = 1",
                (today_utc,),
            )

        if daily_limit is not None and daily_limit > 0 and daily_count >= daily_limit:
            return True, daily_count, daily_limit

        return False, daily_count, daily_limit


def increment_daily_requests(conn: sqlite3.Connection, table_name: str) -> int:
    """Увеличивает счётчик запросов за текущий день."""
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with conn:
        conn.execute(
            f"""
            UPDATE {table_name} SET
                daily_requests_count = CASE WHEN daily_requests_date = ? THEN daily_requests_count + 1 ELSE 1 END,
                daily_requests_date = ?
            WHERE id = 1
            """,
            (today_utc, today_utc),
        )
        row = conn.execute(f"SELECT daily_requests_count FROM {table_name} WHERE id = 1").fetchone()
        return row[0] if row else 1


def reset_worker_daily_requests(conn: sqlite3.Connection, worker_type: str) -> None:
    """Сбросить дневной счётчик запросов конкретного воркера и вывести его из DAILY_LIMIT_EXCEEDED."""
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    table_name = "producer_config" if worker_type == "producer" else "enricher_config"
    state_table = "producer_state" if worker_type == "producer" else "enricher_state"

    with conn:
        conn.execute(
            f"UPDATE {table_name} SET daily_requests_count = 0, daily_requests_date = ? WHERE id = 1",
            (today_utc,),
        )
        # Если воркер находился в стейте DAILY_LIMIT_EXCEEDED, сбрасываем его в IDLE, чтобы он сразу продолжил работу
        row = conn.execute(f"SELECT state_json FROM {state_table} WHERE id = 1").fetchone()
        if row and row[0]:
            try:
                st = json.loads(row[0])
                if st.get("status") == "DAILY_LIMIT_EXCEEDED":
                    st["status"] = "IDLE"
                    conn.execute(
                        f"UPDATE {state_table} SET state_json = ?, updated_at = datetime('now') WHERE id = 1",
                        (json.dumps(st, ensure_ascii=False),),
                    )
            except Exception:
                pass


def create_admin_session(conn: sqlite3.Connection) -> str:
    """Создать сессионный токен администратора."""
    token = secrets.token_hex(32)
    with conn:
        conn.execute(
            "INSERT INTO admin_sessions (token, created_at) VALUES (?, datetime('now'))",
            (token,),
        )
    return token


def validate_admin_session(conn: sqlite3.Connection, token: str) -> bool:
    """Проверить валидность сессионного токена администратора."""
    if not token or not token.strip():
        return False
    row = conn.execute(
        "SELECT 1 FROM admin_sessions WHERE token = ?",
        (token.strip(),),
    ).fetchone()
    return bool(row)


def delete_admin_session(conn: sqlite3.Connection, token: str) -> None:
    """Удалить сессию администратора при логауте."""
    if token:
        with conn:
            conn.execute("DELETE FROM admin_sessions WHERE token = ?", (token.strip(),))



def validate_api_key(conn: sqlite3.Connection, key: str) -> bool:
    """Проверить валидность и активность API-ключа."""
    if not key or not key.strip():
        return False
    row = conn.execute(
        "SELECT 1 FROM api_keys WHERE key = ? AND is_active = 1",
        (key.strip(),),
    ).fetchone()
    return bool(row)


def list_api_keys(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Список всех API ключей."""
    rows = conn.execute(
        """
        SELECT key, name, created_at, is_active
        FROM api_keys
        ORDER BY created_at DESC
        """
    ).fetchall()
    return [
        {
            "key": r[0],
            "name": r[1],
            "created_at": r[2],
            "is_active": bool(r[3]),
        }
        for r in rows
    ]


def create_api_key(conn: sqlite3.Connection, name: str = "New API Key") -> Dict[str, Any]:
    """Сгенерировать новый уникальный API-ключ."""
    clean_name = name.strip() if name and name.strip() else "New API Key"
    new_key = f"dota_key_{secrets.token_hex(16)}"
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with conn:
        conn.execute(
            """
            INSERT INTO api_keys (key, name, created_at, is_active)
            VALUES (?, ?, ?, 1)
            """,
            (new_key, clean_name, now_utc),
        )

    return {
        "key": new_key,
        "name": clean_name,
        "created_at": now_utc,
        "is_active": True,
    }


def delete_api_key(conn: sqlite3.Connection, key: str) -> bool:
    """Удалить API-ключ по его значению."""
    with conn:
        cursor = conn.execute("DELETE FROM api_keys WHERE key = ?", (key.strip(),))
        return cursor.rowcount > 0


def _build_match_filter_query(
    min_rank: Optional[int] = None,
    max_rank: Optional[int] = None,
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None,
    radiant_win: Optional[bool] = None,
    lobby_type: Optional[int] = None,
    game_mode: Optional[int] = None,
    start_after: Optional[int] = None,
    start_before: Optional[int] = None,
) -> Tuple[str, List[Any]]:
    """Построение параметризованного WHERE фильтра для таблицы enriched_matches."""
    where_clauses = []
    params: List[Any] = []

    if min_rank is not None:
        where_clauses.append("COALESCE(json_extract(data, '$.avg_rank_tier'), json_extract(data, '$.averageRank'), json_extract(data, '$.actualRank')) >= ?")
        params.append(min_rank)
    if max_rank is not None:
        where_clauses.append("COALESCE(json_extract(data, '$.avg_rank_tier'), json_extract(data, '$.averageRank'), json_extract(data, '$.actualRank')) <= ?")
        params.append(max_rank)
    if min_duration is not None:
        where_clauses.append("COALESCE(json_extract(data, '$.duration'), json_extract(data, '$.durationSeconds')) >= ?")
        params.append(min_duration)
    if max_duration is not None:
        where_clauses.append("COALESCE(json_extract(data, '$.duration'), json_extract(data, '$.durationSeconds')) <= ?")
        params.append(max_duration)
    if radiant_win is not None:
        bool_int = 1 if radiant_win else 0
        where_clauses.append("(COALESCE(json_extract(data, '$.radiant_win'), json_extract(data, '$.didRadiantWin')) = ? OR COALESCE(json_extract(data, '$.radiant_win'), json_extract(data, '$.didRadiantWin')) = ?)")
        params.extend([bool_int, radiant_win])
    if lobby_type is not None:
        where_clauses.append("COALESCE(json_extract(data, '$.lobby_type'), json_extract(data, '$.lobbyType')) = ?")
        params.append(lobby_type)
    if game_mode is not None:
        where_clauses.append("COALESCE(json_extract(data, '$.game_mode'), json_extract(data, '$.gameMode')) = ?")
        params.append(game_mode)
    if start_after is not None:
        where_clauses.append("COALESCE(json_extract(data, '$.start_time'), json_extract(data, '$.startDateTime')) >= ?")
        params.append(start_after)
    if start_before is not None:
        where_clauses.append("COALESCE(json_extract(data, '$.start_time'), json_extract(data, '$.startDateTime')) <= ?")
        params.append(start_before)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return where_sql, params


def get_enriched_matches(
    conn: sqlite3.Connection,
    limit: int = config.API_DEFAULT_LIMIT,
    offset: int = 0,
    min_rank: Optional[int] = None,
    max_rank: Optional[int] = None,
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None,
    radiant_win: Optional[bool] = None,
    lobby_type: Optional[int] = None,
    game_mode: Optional[int] = None,
    start_after: Optional[int] = None,
    start_before: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Получить обогащённые матчи с пагинацией и гибкими фильтрами (от свежих к старым)."""
    limit = max(1, min(limit, config.API_MAX_LIMIT))
    offset = max(0, offset)

    where_sql, params = _build_match_filter_query(
        min_rank=min_rank,
        max_rank=max_rank,
        min_duration=min_duration,
        max_duration=max_duration,
        radiant_win=radiant_win,
        lobby_type=lobby_type,
        game_mode=game_mode,
        start_after=start_after,
        start_before=start_before,
    )

    query = f"""
        SELECT match_id, data, created_at
        FROM enriched_matches
        {where_sql}
        ORDER BY created_at DESC, match_id DESC
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(query, tuple(params + [limit, offset])).fetchall()

    results = []
    for r in rows:
        match_id, raw_json, created_at = r[0], r[1], r[2]
        try:
            match_data = json.loads(raw_json)
        except Exception:
            match_data = {"match_id": match_id}

        if isinstance(match_data, dict):
            match_data.setdefault("match_id", match_id)
            match_data.setdefault("collected_at", created_at)
            results.append(match_data)

    return results


def count_filtered_enriched_matches(
    conn: sqlite3.Connection,
    min_rank: Optional[int] = None,
    max_rank: Optional[int] = None,
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None,
    radiant_win: Optional[bool] = None,
    lobby_type: Optional[int] = None,
    game_mode: Optional[int] = None,
    start_after: Optional[int] = None,
    start_before: Optional[int] = None,
) -> int:
    """Подсчитать общее количество матчей, соответствующих фильтрам."""
    where_sql, params = _build_match_filter_query(
        min_rank=min_rank,
        max_rank=max_rank,
        min_duration=min_duration,
        max_duration=max_duration,
        radiant_win=radiant_win,
        lobby_type=lobby_type,
        game_mode=game_mode,
        start_after=start_after,
        start_before=start_before,
    )
    return conn.execute(f"SELECT COUNT(*) FROM enriched_matches {where_sql}", tuple(params)).fetchone()[0]


def get_system_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Собрать полную статистику базы данных, очередей, метрик API и состояний сервисов."""
    # 1. Счётчики матчей
    raw_total = conn.execute("SELECT COUNT(*) FROM raw_matches").fetchone()[0]
    enriched_total = conn.execute("SELECT COUNT(*) FROM enriched_matches").fetchone()[0]

    # 2. Чтение конфига обогатителя для созревания
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    e_row = cur.execute("SELECT * FROM enricher_config WHERE id = 1").fetchone()

    min_age_hours = float(e_row["min_age_hours"]) if e_row and e_row["min_age_hours"] is not None else config.ENRICHER_MIN_AGE_HOURS
    max_enriched = int(e_row["max_enriched_matches"]) if e_row and e_row["max_enriched_matches"] is not None else config.ENRICHED_MAX_MATCHES
    enricher_running = bool(e_row["running"]) if e_row and e_row["running"] is not None else False
    stratz_token = str(e_row["stratz_token"]) if e_row and e_row["stratz_token"] else ""
    chunk_size = int(e_row["chunk_size"]) if e_row and e_row["chunk_size"] is not None else config.ENRICHER_CHUNK_SIZE
    poll_interval = float(e_row["poll_interval"]) if e_row and e_row["poll_interval"] is not None else config.ENRICHER_POLL_INTERVAL
    limit_sleep = float(e_row["limit_sleep"]) if e_row and e_row["limit_sleep"] is not None else config.ENRICHER_LIMIT_SLEEP
    chunk_delay = float(e_row["chunk_delay"]) if e_row and e_row["chunk_delay"] is not None else config.ENRICHER_CHUNK_DELAY

    # 3. Созревшие матчи
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=min_age_hours)).strftime("%Y-%m-%d %H:%M:%S")
    ripe_count = conn.execute("SELECT COUNT(*) FROM raw_matches WHERE created_at <= ?", (cutoff,)).fetchone()[0]
    unripe_count = max(0, raw_total - ripe_count)

    # 4. Чтение конфига продюсера
    p_row = cur.execute("SELECT * FROM producer_config WHERE id = 1").fetchone()
    producer_buffer_size = int(p_row["buffer_size"]) if p_row and p_row["buffer_size"] is not None else config.PRODUCER_BUFFER_SIZE
    producer_running = bool(p_row["running"]) if p_row and p_row["running"] is not None else False
    producer_api_key = str(p_row["api_key"]) if p_row and p_row["api_key"] else ""
    producer_delay = float(p_row["delay"]) if p_row and p_row["delay"] is not None else config.PRODUCER_DELAY
    producer_idle_delay = float(p_row["idle_delay"]) if p_row and p_row["idle_delay"] is not None else config.PRODUCER_IDLE_DELAY
    producer_limit_sleep = float(p_row["limit_sleep"]) if p_row and p_row["limit_sleep"] is not None else config.PRODUCER_LIMIT_SLEEP
    producer_min_rank = p_row["min_rank"] if p_row and "min_rank" in p_row.keys() else None
    producer_max_rank = p_row["max_rank"] if p_row and "max_rank" in p_row.keys() else None
    producer_min_duration = p_row["min_duration"] if p_row and "min_duration" in p_row.keys() else None
    producer_max_duration = p_row["max_duration"] if p_row and "max_duration" in p_row.keys() else None
    producer_lobby_type = p_row["lobby_type"] if p_row and "lobby_type" in p_row.keys() else None
    producer_game_mode = p_row["game_mode"] if p_row and "game_mode" in p_row.keys() else None
    producer_min_start_time = p_row["min_start_time"] if p_row and "min_start_time" in p_row.keys() else None

    # 5. Стейты воркеров
    p_state_row = conn.execute("SELECT state_json, updated_at FROM producer_state WHERE id = 1").fetchone()
    e_state_row = conn.execute("SELECT state_json, updated_at FROM enricher_state WHERE id = 1").fetchone()

    producer_state = {}
    if p_state_row and p_state_row[0]:
        try:
            producer_state = json.loads(p_state_row[0])
            producer_state["updated_at"] = p_state_row[1]
        except Exception:
            pass

    enricher_state = {}
    if e_state_row and e_state_row[0]:
        try:
            enricher_state = json.loads(e_state_row[0])
            enricher_state["updated_at"] = e_state_row[1]
        except Exception:
            pass

    # Расчёт процентов заполненности
    raw_buffer_pct = round((raw_total / max(1, producer_buffer_size)) * 100, 1)
    enriched_pct = round((enriched_total / max(1, max_enriched)) * 100, 1) if max_enriched > 0 else 0.0

    return {
        "counts": {
            "enriched_total": enriched_total,
            "raw_total": raw_total,
            "ripe_count": ripe_count,
            "unripe_count": unripe_count,
            "producer_buffer_size": producer_buffer_size,
            "enriched_max_matches": max_enriched,
            "raw_buffer_pct": min(100.0, raw_buffer_pct),
            "enriched_pct": min(100.0, enriched_pct) if max_enriched > 0 else (100.0 if enriched_total > 0 else 0.0),
        },
        "producer": {
            "running": producer_running,
            "status": producer_state.get("status", "IDLE"),
            "last_match_id": producer_state.get("last_match_id"),
            "updated_at": producer_state.get("updated_at"),
            "buffer_size": producer_buffer_size,
            "delay": producer_delay,
            "idle_delay": producer_idle_delay,
            "limit_sleep": producer_limit_sleep,
            "api_key": producer_api_key,
            "min_rank": producer_min_rank,
            "max_rank": producer_max_rank,
            "min_duration": producer_min_duration,
            "max_duration": producer_max_duration,
            "lobby_type": producer_lobby_type,
            "game_mode": producer_game_mode,
            "min_start_time": producer_min_start_time,
            "daily_request_limit": p_row["daily_request_limit"] if p_row and "daily_request_limit" in p_row.keys() else None,
            "daily_requests_count": p_row["daily_requests_count"] if p_row and "daily_requests_count" in p_row.keys() else 0,
            "daily_requests_date": p_row["daily_requests_date"] if p_row and "daily_requests_date" in p_row.keys() else None,
        },
        "enricher": {
            "running": enricher_running,
            "status": enricher_state.get("status", "IDLE"),
            "updated_at": enricher_state.get("updated_at"),
            "stratz_token": stratz_token,
            "has_token": bool(stratz_token and stratz_token.strip()),
            "min_age_hours": min_age_hours,
            "chunk_size": chunk_size,
            "poll_interval": poll_interval,
            "limit_sleep": limit_sleep,
            "chunk_delay": chunk_delay,
            "max_enriched_matches": max_enriched,
            "daily_request_limit": e_row["daily_request_limit"] if e_row and "daily_request_limit" in e_row.keys() else None,
            "daily_requests_count": e_row["daily_requests_count"] if e_row and "daily_requests_count" in e_row.keys() else 0,
            "daily_requests_date": e_row["daily_requests_date"] if e_row and "daily_requests_date" in e_row.keys() else None,
        },
        "metrics": get_api_metrics(conn),
    }
