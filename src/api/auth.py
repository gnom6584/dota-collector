"""Аутентификация запросов к API через API-ключи."""

from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import Cookie, Depends, Header, HTTPException, Security, status
from fastapi.security import APIKeyHeader, APIKeyQuery

from src import config
from src.api.database import get_db, validate_api_key

api_key_header = APIKeyHeader(name=config.API_KEY_HEADER_NAME, auto_error=False)
api_key_query = APIKeyQuery(name="api_key", auto_error=False)


def get_current_api_key(
    header_key: Optional[str] = Security(api_key_header),
    query_key: Optional[str] = Security(api_key_query),
    db: sqlite3.Connection = Depends(get_db),
) -> str:
    """Проверка наличия и валидности API-ключа в заголовке или строке запроса."""
    key = header_key or query_key
    if not key or not key.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Отсутствует API-ключ. Передайте заголовок X-API-Key или параметр ?api_key=...",
        )

    clean_key = key.strip()
    is_valid = validate_api_key(db, clean_key)

    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный или отозванный API-ключ.",
        )

    return clean_key


def get_current_admin(
    admin_session: Optional[str] = Cookie(default=None),
    x_admin_token: Optional[str] = Header(default=None),
    db: sqlite3.Connection = Depends(get_db),
) -> str:
    """Проверка авторизации администратора по сессионной куке или заголовку X-Admin-Token."""
    token = admin_session or x_admin_token
    from src.api.database import validate_admin_session

    if not token or not validate_admin_session(db, token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется авторизация администратора.",
        )
    return token

