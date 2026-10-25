"""Публичные эндпоинты API для клиентов (защищены API-ключом)."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from src import config
from src.api.auth import get_current_api_key
from src.api.database import (
    count_filtered_enriched_matches,
    get_db,
    get_enriched_matches,
)

router = APIRouter(prefix="/api/v1", tags=["Matches API"])


@router.get(
    "/matches",
    summary="Получить список обогащённых матчей с пагинацией и фильтрами",
    description="Возвращает детальную статистику распарсенных матчей Dota 2. Поддерживает фильтры по рангу, длительности, времени, режиму и победителю.",
)
def list_matches(
    limit: int = Query(
        default=config.API_DEFAULT_LIMIT,
        ge=1,
        le=config.API_MAX_LIMIT,
        description="Количество матчей в ответе (макс. 100)",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Смещение для постраничной навигации",
    ),
    min_rank: Optional[int] = Query(
        default=None,
        description="Минимальный средний ранг tier (10=Herald, 70=Divine, 80=Immortal)",
    ),
    max_rank: Optional[int] = Query(
        default=None,
        description="Максимальный средний ранг tier",
    ),
    min_duration: Optional[int] = Query(
        default=None,
        description="Минимальная длительность матча в секундах (например, 1200 для 20 мин)",
    ),
    max_duration: Optional[int] = Query(
        default=None,
        description="Максимальная длительность матча в секундах",
    ),
    radiant_win: Optional[bool] = Query(
        default=None,
        description="Победа Radiant (true/false)",
    ),
    lobby_type: Optional[int] = Query(
        default=None,
        description="Тип лобби: 7 (Ranked), 0 (Normal) и др.",
    ),
    game_mode: Optional[int] = Query(
        default=None,
        description="Режим игры: 22 (All Pick), 2 (Captains Mode), 23 (Turbo) и др.",
    ),
    start_after: Optional[int] = Query(
        default=None,
        description="Матчи, начатые после unix timestamp",
    ),
    start_before: Optional[int] = Query(
        default=None,
        description="Матчи, начатые до unix timestamp",
    ),
    _api_key: str = Depends(get_current_api_key),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    total_in_db = db.execute("SELECT COUNT(*) FROM enriched_matches").fetchone()[0]
    filtered_total = count_filtered_enriched_matches(
        db,
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
    matches = get_enriched_matches(
        db,
        limit=limit,
        offset=offset,
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
    return {
        "total_in_db": total_in_db,
        "filtered_total": filtered_total,
        "count": len(matches),
        "limit": limit,
        "offset": offset,
        "filters": {
            "min_rank": min_rank,
            "max_rank": max_rank,
            "min_duration": min_duration,
            "max_duration": max_duration,
            "radiant_win": radiant_win,
            "lobby_type": lobby_type,
            "game_mode": game_mode,
            "start_after": start_after,
            "start_before": start_before,
        },
        "matches": matches,
    }


@router.get(
    "/matches/count",
    summary="Получить количество сохранённых матчей",
    description="Возвращает общее количество доступных обогащённых матчей и состояние буфера.",
)
def count_matches(
    _api_key: str = Depends(get_current_api_key),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    enriched_count = db.execute("SELECT COUNT(*) FROM enriched_matches").fetchone()[0]
    raw_count = db.execute("SELECT COUNT(*) FROM raw_matches").fetchone()[0]
    return {
        "enriched_matches": enriched_count,
        "raw_matches_queue": raw_count,
        "status": "ready" if enriched_count > 0 else "collecting",
    }


@router.get(
    "/matches/{match_id}",
    summary="Получить конкретный матч по match_id",
)
def get_match_by_id(
    match_id: int,
    _api_key: str = Depends(get_current_api_key),
    db: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    row = db.execute(
        "SELECT data, created_at FROM enriched_matches WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Матч #{match_id} не найден в базе обогащённых матчей.",
        )
    try:
        data = json.loads(row[0])
    except Exception:
        data = {"match_id": match_id}
    data["match_id"] = match_id
    data["collected_at"] = row[1]
    return data
