from __future__ import annotations


class OpenDotaError(Exception):
    """Базовое исключение клиента OpenDota API."""


class OpenDotaHttpError(OpenDotaError):
    """Исключение, содержащее статус-код и тело ответа HTTP."""

    def __init__(self, status_code: int, message: str, body: str | None = None) -> None:
        super().__init__(f"OpenDota HTTP {status_code}: {message}")
        self.status_code = status_code
        self.body = body


class OpenDotaRateLimitError(OpenDotaHttpError):
    """Превышен лимит запросов (HTTP 429 Too Many Requests)."""


class OpenDotaAuthError(OpenDotaHttpError):
    """Ошибка аутентификации или авторизации (HTTP 401 / 403)."""


class OpenDotaNotFoundError(OpenDotaHttpError):
    """Запрашиваемый ресурс не найден (HTTP 404)."""


class OpenDotaServerError(OpenDotaHttpError):
    """Сбой на стороне OpenDota или Steam API (HTTP 5xx)."""


class OpenDotaValidationError(OpenDotaError):
    """Ответ API не соответствует ожидаемой схеме контракта данных."""
