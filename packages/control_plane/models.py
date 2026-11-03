from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class QuotaStats(BaseModel):
    """Снимок состояния суточной квоты."""

    name: str
    used_units: int
    max_units: int
    remaining_units: int
    reset_at: int | None = None


class WorkerStats(BaseModel):
    """Снимок статуса и метрик конкретного воркера."""

    name: str
    worker_type: str | None = None
    is_enabled: bool = False
    status: str = "stopped"
    pid: int | None = None
    last_error: str | None = None
    updated_at: str | None = None
    quota: QuotaStats | None = None
    state_summary: dict[str, Any] | None = None


class SystemStats(BaseModel):
    """Сводный снимок состояния всей системы сбора и обогащения матчей."""

    workers: list[WorkerStats] = Field(default_factory=list)
    queue_size: int = 0
    queue_max_size: int = 0
    enriched_matches_count: int = 0
    storage_max_matches: int = 0
    maturation_delay_seconds: float = 21600.0

