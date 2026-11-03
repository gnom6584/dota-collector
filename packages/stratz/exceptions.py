from __future__ import annotations

from typing import Any


class StratzError(Exception):
    """Базовое исключение клиента Stratz API."""


class StratzHttpError(StratzError):
    """Исключение, содержащее HTTP статус-код и тело ответа API."""

    def __init__(self, status_code: int, message: str, body: str | None = None) -> None:
        super().__init__(f"Stratz HTTP {status_code}: {message}")
        self.status_code = status_code
        self.body = body


class StratzRateLimitError(StratzHttpError):
    """Превышен лимит запросов или квота (HTTP 429 или GraphQL throttle)."""

    def __init__(self, message: str = "Rate limit exceeded", status_code: int = 429, body: str | None = None) -> None:
        super().__init__(status_code=status_code, message=message, body=body)


class StratzAuthError(StratzHttpError):
    """Ошибка аутентификации или авторизации (HTTP 401 / 403 или невалидный токен)."""

    def __init__(self, message: str = "Authentication failed", status_code: int = 401, body: str | None = None) -> None:
        super().__init__(status_code=status_code, message=message, body=body)


class StratzServerError(StratzHttpError):
    """Сбой на стороне Stratz или upstream-сервисов (HTTP 5xx)."""


class StratzGraphQLError(StratzError):
    """Ошибка выполнения или валидации GraphQL-запроса."""

    def __init__(self, message: str, errors: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


class StratzValidationError(StratzError):
    """Ответ API не соответствует ожидаемой схеме контракта данных."""
