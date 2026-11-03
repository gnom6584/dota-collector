from __future__ import annotations

import logging
from collections.abc import Callable

from pydantic import BaseModel, ConfigDict, Field

from packages.opendota import (
    OpenDotaClient,
    OpenDotaError,
    OpenDotaRateLimitError,
    PublicMatch,
)
from packages.quotas import Quota
from packages.workers import Worker

from .config import PublicMatchCollectorConfig

logger = logging.getLogger("packages.worker.public_match_collector")


class PublicMatchCollectorState(BaseModel):
    """Персистентное состояние воркера public_match_collector."""

    model_config = ConfigDict(extra="ignore")

    pending_matches: list[PublicMatch] = Field(default_factory=list)


class PublicMatchCollector(Worker[PublicMatchCollectorState]):
    """Воркер сбора свежих публичных матчей Dota 2 из OpenDota API.

    Архитектурные принципы:
    1. Опрос только свежего среза: воркер обращается к вершине ленты OpenDota без пагинации
       вглубь истории (`less_than_match_id`).
    2. Постобработка и фильтрация: после получения пачки матчи валидируются по локальным
       критериям (диапазон длительности `duration`, кластеры серверов `cluster`, режимы `game_mode`).
    3. Делегирование дедупликации: воркер не использует скалярные курсоры ID, передавая
       все прошедшие фильтры матчи в пайплайн для атомарной регистрации и дедупликации.
    4. Изоляция backpressure: при отказе приемника (переполнение очереди или буфера)
       отфильтрованная пачка сохраняется в `pending_matches`, а дальнейшие сетевые запросы
       приостанавливаются без потери данных и квот.
    """

    name = "public_match_collector"

    def __init__(
        self,
        client: OpenDotaClient,
        sink: Callable[[list[PublicMatch]], bool],
        quota: Quota,
        config_factory: Callable[[], PublicMatchCollectorConfig],
    ) -> None:
        self.client = client
        self.sink = sink
        self.quota = quota
        self.config_factory = config_factory

    def execute(
        self,
        state: PublicMatchCollectorState,
    ) -> tuple[PublicMatchCollectorState, float]:
        config = self.config_factory()

        if state.pending_matches:
            if not self.sink(state.pending_matches):
                logger.warning(
                    "[%s] Sink rejected %d pending matches (backpressure). Yielding.",
                    self.name,
                    len(state.pending_matches),
                )
                return state, config.cooldown
            state = state.model_copy(update={"pending_matches": []})

        if not self.quota.acquire(cost=1):
            logger.warning(
                "[%s] Quota exhausted. Retrying in %.1f s.",
                self.name,
                config.quota_retry_delay,
            )
            return state, config.quota_retry_delay

        try:
            matches = self.client.get_public_matches(
                min_rank=config.min_rank,
                max_rank=config.max_rank,
            )
        except OpenDotaRateLimitError as err:
            logger.warning(
                "[%s] API rate limit hit (HTTP 429): %s. Sleeping for %.1f s.",
                self.name,
                err,
                config.rate_limit_delay,
            )
            return state, config.rate_limit_delay
        except OpenDotaError as err:
            logger.warning(
                "[%s] API request failed: %s. Sleeping for %.1f s.",
                self.name,
                err,
                config.error_delay,
            )
            return state, config.error_delay

        filtered_matches: list[PublicMatch] = []
        for m in matches:
            if config.min_duration is not None and m.duration < config.min_duration:
                continue
            if config.max_duration is not None and m.duration > config.max_duration:
                continue
            if config.cluster is not None and (m.cluster is None or m.cluster not in config.cluster):
                continue
            if config.game_mode is not None and m.game_mode not in config.game_mode:
                continue
            filtered_matches.append(m)

        logger.info(
            "[%s] %d of %d matches passed filters.",
            self.name,
            len(filtered_matches),
            len(matches),
        )

        if not filtered_matches:
            return state, config.cooldown

        if not self.sink(filtered_matches):
            logger.warning(
                "[%s] Sink rejected %d matches (backpressure). Storing in pending state.",
                self.name,
                len(filtered_matches),
            )
            return (
                state.model_copy(update={"pending_matches": filtered_matches}),
                config.cooldown,
            )

        logger.info(
            "[%s] Successfully forwarded %d matches to pipeline.",
            self.name,
            len(filtered_matches),
        )
        return (
            state.model_copy(update={"pending_matches": []}),
            config.cooldown,
        )

    def close(self) -> None:
        self.client.close()
