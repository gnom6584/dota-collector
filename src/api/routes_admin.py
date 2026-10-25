"""Эндпоинты панели администратора (управление параметрами, ключами, мониторинг, авторизация)."""

from __future__ import annotations

import secrets
import sqlite3
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter, Cookie, Depends, Header, HTTPException, Query, Response, status
from pydantic import BaseModel, Field

from src import config
from src.api.auth import get_current_admin
from src.api.database import (
    create_admin_session,
    create_api_key,
    delete_admin_session,
    delete_api_key,
    get_db,
    get_enriched_matches,
    get_system_stats,
    list_api_keys,
    reset_api_metrics,
    reset_worker_daily_requests,
    validate_admin_session,
)
from src.workers.match_enricher import enforce_enriched_limit
from src.workers.match_producer import trim_raw_matches

router = APIRouter(prefix="/admin/api", tags=["Admin Operations"])


class LoginRequest(BaseModel):
    username: str
    password: str


class ProducerConfigUpdate(BaseModel):
    running: Optional[bool] = None
    buffer_size: Optional[int] = Field(None, ge=1)
    delay: Optional[float] = Field(None, ge=0.0)
    idle_delay: Optional[float] = Field(None, ge=0.1)
    limit_sleep: Optional[float] = Field(None, ge=0.1)
    api_key: Optional[str] = None
    min_rank: Optional[int] = None
    max_rank: Optional[int] = None
    min_duration: Optional[int] = None
    max_duration: Optional[int] = None
    lobby_type: Optional[int] = None
    game_mode: Optional[int] = None
    min_start_time: Optional[int] = None
    daily_request_limit: Optional[int] = None


class EnricherConfigUpdate(BaseModel):
    running: Optional[bool] = None
    stratz_token: Optional[str] = None
    min_age_hours: Optional[float] = Field(None, ge=0.0)
    chunk_size: Optional[int] = Field(None, ge=1)
    poll_interval: Optional[float] = Field(None, ge=0.1)
    limit_sleep: Optional[float] = Field(None, ge=0.1)
    chunk_delay: Optional[float] = Field(None, ge=0.0)
    max_enriched_matches: Optional[int] = Field(None, ge=0)
    daily_request_limit: Optional[int] = None


class CreateKeyRequest(BaseModel):
    name: str = "Admin Key"


class TestTokenRequest(BaseModel):
    token: str


@router.post("/login", summary="Авторизация администратора")
def login(
    body: LoginRequest,
    response: Response,
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Проверка логина и пароля администратора из config (.env) и создание сессии."""
    user_ok = secrets.compare_digest(body.username.strip(), config.ADMIN_USERNAME.strip())
    pass_ok = secrets.compare_digest(body.password.strip(), config.ADMIN_PASSWORD.strip())

    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль администратора.",
        )

    token = create_admin_session(db)
    response.set_cookie(
        key="admin_session",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,  # 30 дней
    )
    return {
        "status": "ok",
        "token": token,
        "username": config.ADMIN_USERNAME,
        "message": "Успешный вход в панель управления.",
    }


@router.post("/logout", summary="Выход из панели администратора")
def logout(
    response: Response,
    admin_session: Optional[str] = Cookie(default=None),
    x_admin_token: Optional[str] = Header(default=None),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Завершение сессии и удаление сессионной куки."""
    token = admin_session or x_admin_token
    if token:
        delete_admin_session(db, token)
    response.delete_cookie(key="admin_session")
    return {"status": "ok", "message": "Сессия завершена."}


@router.get("/auth-status", summary="Проверка статуса текущей сессии")
def auth_status(
    admin_session: Optional[str] = Cookie(default=None),
    x_admin_token: Optional[str] = Header(default=None),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Проверяет валидность токена в куке или заголовке X-Admin-Token."""
    token = admin_session or x_admin_token
    is_authed = bool(token and validate_admin_session(db, token))
    return {
        "authenticated": is_authed,
        "username": config.ADMIN_USERNAME if is_authed else None,
    }


@router.get("/stats", summary="Получить полную сводку и статус очередей")
def get_stats(
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    return get_system_stats(db)


@router.get("/recent-matches", summary="Получить последние обогащённые матчи для админки")
def get_recent_matches(
    limit: int = Query(default=10, ge=1, le=50),
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    matches = get_enriched_matches(db, limit=limit, offset=0)
    return {"count": len(matches), "matches": matches}


@router.post("/config/producer", summary="Обновить настройки сборщика OpenDota")
def update_producer(
    body: ProducerConfigUpdate,
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    fields = []
    values = []

    if body.running is not None:
        fields.append("running = ?")
        values.append(1 if body.running else 0)
    if body.buffer_size is not None:
        fields.append("buffer_size = ?")
        values.append(max(1, body.buffer_size))
    if body.delay is not None:
        fields.append("delay = ?")
        values.append(max(0.0, body.delay))
    if body.idle_delay is not None:
        fields.append("idle_delay = ?")
        values.append(max(0.1, body.idle_delay))
    if body.limit_sleep is not None:
        fields.append("limit_sleep = ?")
        values.append(max(0.1, body.limit_sleep))
    if body.api_key is not None:
        fields.append("api_key = ?")
        values.append(body.api_key.strip() or None)
    if body.min_rank is not None:
        fields.append("min_rank = ?")
        values.append(body.min_rank if body.min_rank > 0 else None)
    if body.max_rank is not None:
        fields.append("max_rank = ?")
        values.append(body.max_rank if body.max_rank > 0 else None)
    if body.min_duration is not None:
        fields.append("min_duration = ?")
        values.append(body.min_duration if body.min_duration > 0 else None)
    if body.max_duration is not None:
        fields.append("max_duration = ?")
        values.append(body.max_duration if body.max_duration > 0 else None)
    if body.lobby_type is not None:
        fields.append("lobby_type = ?")
        values.append(body.lobby_type if body.lobby_type >= 0 else None)
    if body.game_mode is not None:
        fields.append("game_mode = ?")
        values.append(body.game_mode if body.game_mode >= 0 else None)
    if body.min_start_time is not None:
        fields.append("min_start_time = ?")
        values.append(body.min_start_time if body.min_start_time > 0 else None)
    if body.daily_request_limit is not None:
        fields.append("daily_request_limit = ?")
        values.append(body.daily_request_limit if body.daily_request_limit > 0 else None)

    if fields:
        fields.append("updated_at = datetime('now')")
        with db:
            db.execute("INSERT OR IGNORE INTO producer_config (id) VALUES (1)")
            query = f"UPDATE producer_config SET {', '.join(fields)} WHERE id = 1"
            db.execute(query, tuple(values))

    if body.buffer_size is not None:
        trim_raw_matches(db, max(1, body.buffer_size))

    return {"status": "ok", "message": "Параметры продюсера обновлены."}


@router.post("/config/producer/reset-daily", summary="Сбросить счетчик дневных запросов продюсера")
def reset_producer_daily(
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    reset_worker_daily_requests(db, "producer")
    return {"status": "ok", "message": "Дневной счетчик запросов продюсера сброшен."}


@router.post("/config/enricher", summary="Обновить настройки обогатителя Stratz")
def update_enricher(
    body: EnricherConfigUpdate,
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    fields = []
    values = []

    if body.running is not None:
        fields.append("running = ?")
        values.append(1 if body.running else 0)
    if body.stratz_token is not None:
        fields.append("stratz_token = ?")
        values.append(body.stratz_token.strip())
    if body.min_age_hours is not None:
        fields.append("min_age_hours = ?")
        values.append(max(0.0, body.min_age_hours))
    if body.chunk_size is not None:
        fields.append("chunk_size = ?")
        values.append(max(1, body.chunk_size))
    if body.poll_interval is not None:
        fields.append("poll_interval = ?")
        values.append(max(0.1, body.poll_interval))
    if body.limit_sleep is not None:
        fields.append("limit_sleep = ?")
        values.append(max(0.1, body.limit_sleep))
    if body.chunk_delay is not None:
        fields.append("chunk_delay = ?")
        values.append(max(0.0, body.chunk_delay))
    if body.max_enriched_matches is not None:
        target_limit = max(0, body.max_enriched_matches)
        fields.append("max_enriched_matches = ?")
        values.append(target_limit)

    if body.daily_request_limit is not None:
        fields.append("daily_request_limit = ?")
        values.append(body.daily_request_limit if body.daily_request_limit > 0 else None)

    if fields:
        fields.append("updated_at = datetime('now')")
        with db:
            db.execute("INSERT OR IGNORE INTO enricher_config (id) VALUES (1)")
            query = f"UPDATE enricher_config SET {', '.join(fields)} WHERE id = 1"
            db.execute(query, tuple(values))

    if body.max_enriched_matches is not None:
        enforce_enriched_limit(db, max(0, body.max_enriched_matches))

    return {"status": "ok", "message": "Параметры обогатителя обновлены."}


@router.post("/config/enricher/reset-daily", summary="Сбросить счетчик дневных запросов обогатителя")
def reset_enricher_daily(
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    reset_worker_daily_requests(db, "enricher")
    return {"status": "ok", "message": "Дневной счетчик запросов обогатителя сброшен."}


@router.get("/keys", summary="Получить список всех API-ключей")
def get_keys(
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    keys = list_api_keys(db)
    return {"keys": keys, "count": len(keys)}


@router.post("/keys", summary="Создать новый API-ключ")
def generate_key(
    body: CreateKeyRequest,
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    new_key_info = create_api_key(db, name=body.name)
    return {"status": "ok", "key": new_key_info}


@router.delete("/keys/{key}", summary="Удалить / отозвать API-ключ")
def revoke_key(
    key: str,
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    deleted = delete_api_key(db, key=key)
    if not deleted:
        raise HTTPException(status_code=404, detail="Ключ не найден.")
    return {"status": "ok", "message": "Ключ удалён."}


@router.post("/test-token", summary="Проверить валидность Stratz токена онлайн")
def test_stratz_token(
    body: TestTokenRequest,
    _admin: str = Depends(get_current_admin),
) -> Dict[str, Any]:
    raw_token = body.token.strip()
    if not raw_token:
        return {"valid": False, "message": "Токен пуст."}

    # Очищаем от случайного префикса Bearer
    token = raw_token.removeprefix("Bearer ").strip()

    # Корректный GraphQL запрос к Stratz API
    test_query = "query { match(id: 7000000000) { id } }"
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "STRATZ_API",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            config.STRATZ_API_URL,
            json={"query": test_query},
            headers=headers,
            timeout=10.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("data") is not None:
                return {"valid": True, "message": "Токен Stratz успешно проверен и активен!"}
            if data.get("errors"):
                auth_errors = [e for e in data.get("errors", []) if "unauthorized" in str(e).lower()]
                if not auth_errors:
                    return {"valid": True, "message": "Токен Stratz активен и авторизован!"}
        if resp.status_code in (401, 403):
            return {"valid": False, "message": f"Ошибка авторизации ({resp.status_code}): Токен недействителен."}
        if resp.status_code == 400:
            try:
                err_text = resp.json().get("errors", [{}])[0].get("message", resp.text[:120])
            except Exception:
                err_text = resp.text[:120]
            return {"valid": False, "message": f"Stratz вернул 400: {err_text}"}
        return {"valid": False, "message": f"Stratz вернул статус {resp.status_code}."}
    except Exception as err:
        return {"valid": False, "message": f"Сетевой сбой при проверке токена: {err}"}


@router.post("/metrics/reset", summary="Сбросить счетчики обращений к API OpenDota и Stratz")
def reset_metrics(
    provider: Optional[str] = Query(default=None, description="opendota, stratz или сбросить все при отсутствии"),
    _admin: str = Depends(get_current_admin),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    reset_api_metrics(db, provider=provider)
    return {"status": "ok", "message": f"Счётчики запросов ({provider or 'все'}) успешно сброшены."}

