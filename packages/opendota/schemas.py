from __future__ import annotations

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, field_validator


class PublicMatch(BaseModel):
    """Схема публичного матча из эндпоинта /publicMatches.

    Валидирует выборку публичных матчей по рангам.
    Неизвестные дополнительные поля отсекаются (extra='ignore').
    """

    model_config = ConfigDict(extra="ignore")

    match_id: int
    match_seq_num: int | None = None
    radiant_win: bool
    start_time: int
    duration: int
    lobby_type: int
    game_mode: int
    avg_rank_tier: int | None = None
    num_rank_tier: int | None = None
    cluster: int | None = None
    radiant_team: list[int] = Field(default_factory=list)
    dire_team: list[int] = Field(default_factory=list)

    @field_validator("radiant_team", "dire_team", mode="before")
    @classmethod
    def _parse_hero_ids(cls, v: Any) -> list[int]:
        if isinstance(v, str):
            return [int(x) for x in v.split(",") if x.strip().isdigit()]
        if isinstance(v, list):
            return [int(x) for x in v]
        return []


class ProMatch(BaseModel):
    """Схема профессионального матча из эндпоинта /proMatches.

    Валидирует турнирные данные матча (команды, лига, счет).
    Неизвестные дополнительные поля отсекаются (extra='ignore').
    """

    model_config = ConfigDict(extra="ignore")

    match_id: int
    duration: int
    start_time: int
    leagueid: int
    radiant_team_id: int | None = None
    radiant_name: str | None = None
    dire_team_id: int | None = None
    dire_name: str | None = None
    league_name: str | None = None
    series_id: int | None = None
    series_type: int | None = None
    radiant_score: int | None = None
    dire_score: int | None = None
    radiant_win: bool | None = None
    radiant: bool | None = None
