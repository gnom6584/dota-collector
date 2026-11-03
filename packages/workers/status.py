from __future__ import annotations

from enum import StrEnum


class WorkerStatus(StrEnum):
    """Статусы физического состояния выполнения воркера.

    - STOPPED ('stopped'):
        Процесс воркера не запущен или завершил выполнение.
    - WAITING ('waiting'):
        Процесс запущен, но находится в режиме ожидания (деактивирован в БД, is_enabled=False).
    - SLEEPING ('sleeping'):
        Процесс активен, находится в состоянии сна между итерациями (интервал delay).
    - EXECUTING ('executing'):
        Воркер прямо сейчас выполняет полезную нагрузку метода `execute(state)`.
    """

    STOPPED = "stopped"
    WAITING = "waiting"
    SLEEPING = "sleeping"
    EXECUTING = "executing"
