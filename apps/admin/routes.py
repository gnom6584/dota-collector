from __future__ import annotations

import json
import logging
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from apps.match_collector.container import ApplicationContainer
from packages.control_plane import ControlPlane
from packages.control_plane.models import SystemStats
from packages.match_enricher import MatchEnricherConfig
from packages.match_storage_writer import MatchStorageWriterConfig
from packages.public_match_collector import PublicMatchCollectorConfig

logger = logging.getLogger("apps.admin.routes")

router = APIRouter(prefix="/api", tags=["Control Plane"])


def get_container(request: Request) -> ApplicationContainer:
    """Извлекает глобальный контейнер приложения из состояния FastAPI app."""
    return request.app.state.container


def get_control_plane(
    container: ApplicationContainer = Depends(get_container),
) -> ControlPlane:
    """Предоставляет фасад Control Plane для выполнения административных команд."""
    return container.control_plane


class ActionResponse(BaseModel):
    """Стандартный ответ на управляющие команды воркеров."""

    status: str = "ok"
    worker: str
    message: str


class QuotaActionResponse(BaseModel):
    """Ответ на команду управления суточной квотой."""

    status: str = "ok"
    quota: str
    message: str


class SetQuotaLimitPayload(BaseModel):
    """Входные данные для обновления максимального лимита квоты."""

    limit: int = Field(gt=0, description="Новый максимальный лимит суточной квоты.")


class SetQuotaLimitResponse(BaseModel):
    """Ответ на успешное обновление максимального лимита квоты."""

    status: str = "ok"
    quota: str
    limit: int
    message: str


class SetCapacityPayload(BaseModel):
    """Входные данные для обновления максимальной вместимости очереди или хранилища."""

    limit: int = Field(gt=0, description="Новый максимальный предел вместимости.")


class SetMaturationTimePayload(BaseModel):
    """Входные данные для обновления времени созревания матчей."""

    minutes: float | None = Field(
        default=None,
        ge=0.0,
        description="Новое время созревания матчей в минутах.",
    )
    seconds: float | None = Field(
        default=None,
        ge=0.0,
        description="Новое время созревания матчей в секундах (для обратной совместимости).",
    )


class RecentMatchSummary(BaseModel):
    """Краткая сводка обогащенного матча для отображения в таблице мониторинга."""

    match_id: int
    created_at: str | None = None
    radiant_win: bool | None = None
    duration: int | None = None
    start_time: int | None = None
    raw_size_bytes: int = 0


class PaginatedMatchesResponse(BaseModel):
    """Схема пагинированного ответа списка обогащенных матчей."""

    items: list[RecentMatchSummary]
    total: int
    page: int
    page_size: int
    total_pages: int


@router.get("/stats", response_model=SystemStats)
def get_system_stats(
    control_plane: ControlPlane = Depends(get_control_plane),
) -> SystemStats:
    """Возвращает агрегированную телеметрию системы: статус воркеров, квоты, очередь и хранилище."""
    try:
        return control_plane.get_system_stats()
    except Exception as err:
        logger.exception("Failed to retrieve system statistics: %s", err)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve system statistics: {err}",
        )


@router.post("/workers/{worker_name}/enable", response_model=ActionResponse)
def enable_worker(
    worker_name: str,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> ActionResponse:
    """Активирует выполнение воркера в координационном хранилище."""
    try:
        control_plane.enable_worker(worker_name)
        return ActionResponse(
            status="ok",
            worker=worker_name,
            message="Worker enabled successfully.",
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to enable worker '{worker_name}': {err}",
        )


@router.post("/workers/{worker_name}/disable", response_model=ActionResponse)
def disable_worker(
    worker_name: str,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> ActionResponse:
    """Приостанавливает выполнение воркера."""
    try:
        control_plane.disable_worker(worker_name)
        return ActionResponse(
            status="ok",
            worker=worker_name,
            message="Worker disabled successfully.",
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to disable worker '{worker_name}': {err}",
        )


@router.post("/workers/{worker_name}/clear-error", response_model=ActionResponse)
def clear_worker_error(
    worker_name: str,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> ActionResponse:
    """Очищает зарегистрированную ошибку воркера в координационном хранилище."""
    try:
        control_plane.clear_worker_error(worker_name)
        return ActionResponse(
            status="ok",
            worker=worker_name,
            message="Worker error state cleared successfully.",
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear error for worker '{worker_name}': {err}",
        )


@router.post("/quotas/{quota_name}/reset", response_model=QuotaActionResponse)
def reset_quota(
    quota_name: str,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> QuotaActionResponse:
    """Сбрасывает израсходованный объем суточной квоты."""
    try:
        control_plane.reset_quota(quota_name)
        return QuotaActionResponse(
            status="ok",
            quota=quota_name,
            message=f"Quota '{quota_name}' successfully reset to 0.",
        )
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Quota '{quota_name}' not found.",
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reset quota '{quota_name}': {err}",
        )


@router.post("/quotas/{quota_name}/limit", response_model=SetQuotaLimitResponse)
def set_quota_limit(
    quota_name: str,
    payload: SetQuotaLimitPayload,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> SetQuotaLimitResponse:
    """Обновляет максимальный лимит суточной квоты."""
    try:
        control_plane.set_quota_limit(quota_name, payload.limit)
        return SetQuotaLimitResponse(
            status="ok",
            quota=quota_name,
            limit=payload.limit,
            message=f"Quota '{quota_name}' limit updated to {payload.limit}.",
        )
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Quota '{quota_name}' not found.",
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update quota '{quota_name}' limit: {err}",
        )


@router.post("/pipeline/queue/limit", response_model=ActionResponse)
def set_queue_limit(
    payload: SetCapacityPayload,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> ActionResponse:
    """Обновляет максимальную вместимость очереди созревания матчей."""
    try:
        control_plane.set_queue_max_size(payload.limit)
        return ActionResponse(
            status="ok",
            worker="public_match_pipeline",
            message=f"Queue max size updated to {payload.limit}.",
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update queue max size: {err}",
        )


@router.post("/pipeline/maturation-time", response_model=ActionResponse)
@router.post("/pipeline/maturation-delay", response_model=ActionResponse)
def set_maturation_time(
    payload: SetMaturationTimePayload,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> ActionResponse:
    """Обновляет время созревания матчей перед обогащением (в минутах или секундах)."""
    if payload.minutes is not None:
        seconds = float(payload.minutes * 60.0)
    elif payload.seconds is not None:
        seconds = float(payload.seconds)
    else:
        raise HTTPException(
            status_code=422,
            detail="Either 'minutes' or 'seconds' must be provided.",
        )

    try:
        control_plane.set_maturation_delay(seconds)
        minutes_val = seconds / 60.0
        return ActionResponse(
            status="ok",
            worker="public_match_pipeline",
            message=f"Maturation time updated to {minutes_val:.1f} min ({seconds:.0f}s).",
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update maturation time: {err}",
        )


class PipelineParamsResponse(BaseModel):
    """Сводка текущих параметров конвейера созревания матчей."""

    maturation_time_minutes: float
    maturation_delay_seconds: float
    max_queue_size: int


@router.get("/configs/pipeline", response_model=PipelineParamsResponse)
def get_pipeline_config(
    control_plane: ControlPlane = Depends(get_control_plane),
) -> PipelineParamsResponse:
    """Возвращает актуальные параметры конвейера созревания матчей."""
    try:
        delay_sec = control_plane.get_maturation_delay()
        return PipelineParamsResponse(
            maturation_time_minutes=round(delay_sec / 60.0, 2),
            maturation_delay_seconds=delay_sec,
            max_queue_size=control_plane.get_queue_max_size(),
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read pipeline params: {err}",
        )


@router.post("/storage/limit", response_model=ActionResponse)
def set_storage_limit(
    payload: SetCapacityPayload,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> ActionResponse:
    """Обновляет максимальную вместимость целевого хранилища обогащенных матчей."""
    try:
        control_plane.set_storage_max_matches(payload.limit)
        return ActionResponse(
            status="ok",
            worker="match_storage",
            message=f"Storage max matches updated to {payload.limit}.",
        )
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update storage max matches: {err}",
        )


@router.get("/configs/public-collector", response_model=PublicMatchCollectorConfig)
def get_public_collector_config(
    control_plane: ControlPlane = Depends(get_control_plane),
) -> PublicMatchCollectorConfig:
    """Возвращает актуальную персистентную конфигурацию воркера public_match_collector."""
    try:
        return control_plane.get_public_collector_config()
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read public collector config: {err}",
        )


@router.post("/configs/public-collector", response_model=PublicMatchCollectorConfig)
def set_public_collector_config(
    config: PublicMatchCollectorConfig,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> PublicMatchCollectorConfig:
    """Обновляет персистентную конфигурацию воркера public_match_collector на лету."""
    try:
        control_plane.set_public_collector_config(config)
        return config
    except Exception as err:
        logger.exception("Failed to update public collector config: %s", err)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update public collector config: {err}",
        )


@router.get("/configs/enricher", response_model=MatchEnricherConfig)
def get_enricher_config(
    control_plane: ControlPlane = Depends(get_control_plane),
) -> MatchEnricherConfig:
    """Возвращает актуальную персистентную конфигурацию воркера match_enricher."""
    try:
        return control_plane.get_enricher_config()
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read enricher config: {err}",
        )


@router.post("/configs/enricher", response_model=MatchEnricherConfig)
def set_enricher_config(
    config: MatchEnricherConfig,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> MatchEnricherConfig:
    """Обновляет персистентную конфигурацию воркера match_enricher на лету."""
    try:
        control_plane.set_enricher_config(config)
        return config
    except Exception as err:
        logger.exception("Failed to update enricher config: %s", err)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update enricher config: {err}",
        )


@router.get("/configs/storage-writer", response_model=MatchStorageWriterConfig)
def get_storage_writer_config(
    control_plane: ControlPlane = Depends(get_control_plane),
) -> MatchStorageWriterConfig:
    """Возвращает актуальную конфигурацию воркера match_storage_writer."""
    try:
        return control_plane.get_storage_writer_config()
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read storage writer config: {err}",
        )


@router.post("/configs/storage-writer", response_model=MatchStorageWriterConfig)
def set_storage_writer_config(
    config: MatchStorageWriterConfig,
    control_plane: ControlPlane = Depends(get_control_plane),
) -> MatchStorageWriterConfig:
    """Обновляет персистентную конфигурацию воркера match_storage_writer на лету."""
    try:
        control_plane.set_storage_writer_config(config)
        return config
    except Exception as err:
        logger.exception("Failed to update storage writer config: %s", err)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update storage writer config: {err}",
        )


@router.get("/matches/recent", response_model=PaginatedMatchesResponse)
def get_recent_matches(
    page: int = 1,
    page_size: int = 20,
    search: str | None = None,
    container: ApplicationContainer = Depends(get_container),
) -> PaginatedMatchesResponse:
    """Возвращает пагинированный список сохраненных обогащенных матчей с поддержкой серверного поиска."""
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    table = container.config.db.storage_table
    cursor = container.conn.cursor()
    try:
        search_filter = search.strip() if search else None
        if search_filter:
            search_param = f"%{search_filter}%"
            cursor.execute(
                f'SELECT count(*) FROM "{table}" WHERE CAST(match_id AS TEXT) LIKE ?',
                (search_param,),
            )
            total = cursor.fetchone()[0]
            offset = (page - 1) * page_size
            cursor.execute(
                f'SELECT match_id, data_json, created_at FROM "{table}" '
                f'WHERE CAST(match_id AS TEXT) LIKE ? '
                f'ORDER BY match_id DESC LIMIT ? OFFSET ?',
                (search_param, page_size, offset),
            )
        else:
            cursor.execute(f'SELECT count(*) FROM "{table}"')
            total = cursor.fetchone()[0]
            offset = (page - 1) * page_size
            cursor.execute(
                f'SELECT match_id, data_json, created_at FROM "{table}" '
                f'ORDER BY match_id DESC LIMIT ? OFFSET ?',
                (page_size, offset),
            )

        rows = cursor.fetchall()
        summaries: list[RecentMatchSummary] = []
        for match_id, data_json, created_at in rows:
            radiant_win = None
            duration = None
            start_time = None
            raw_size = len(data_json.encode("utf-8")) if data_json else 0
            if data_json:
                try:
                    parsed = json.loads(data_json)
                    radiant_win = parsed.get("radiant_win") or parsed.get("didRadiantWin")
                    duration = parsed.get("duration") or parsed.get("durationSeconds")
                    start_time = parsed.get("start_time") or parsed.get("startDateTime")
                except Exception:
                    pass
            summaries.append(
                RecentMatchSummary(
                    match_id=match_id,
                    created_at=created_at,
                    radiant_win=radiant_win,
                    duration=duration,
                    start_time=start_time,
                    raw_size_bytes=raw_size,
                )
            )
        total_pages = max(1, (total + page_size - 1) // page_size)
        return PaginatedMatchesResponse(
            items=summaries,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )
    except Exception as err:
        logger.exception("Failed to fetch recent matches: %s", err)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch recent matches: {err}",
        )
    finally:
        cursor.close()


@router.get("/matches/{match_id}")
def get_match_detail(
    match_id: int,
    container: ApplicationContainer = Depends(get_container),
) -> dict[str, Any]:
    """Возвращает полный JSON-документ обогащенного матча по его идентификатору."""
    table = container.config.db.storage_table
    cursor = container.conn.cursor()
    try:
        cursor.execute(
            f'SELECT data_json FROM "{table}" WHERE match_id = ?',
            (match_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Match with ID {match_id} not found.",
            )
        try:
            return json.loads(row[0])
        except Exception as err:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to decode match JSON data: {err}",
            )
    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch match details: {err}",
        )
    finally:
        cursor.close()
