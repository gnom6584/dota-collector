from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from packages.quotas import Quota
from packages.stratz import StratzClient, StratzError, StratzRateLimitError
from packages.workers import Worker

from .config import MatchEnricherConfig

logger = logging.getLogger("packages.match_enricher.worker")


class MatchEnricherState(BaseModel):
    """Персистентное состояние воркера match_enricher."""

    pending_enriched: list[dict[str, Any]] = []
    pending_failed: dict[int, str] = {}


class MatchEnricher(Worker[MatchEnricherState]):
    """Воркер обогащения деталей матчей через Stratz GraphQL API.

    Архитектура конвейера:
    1. Проверяет наличие неотправленного батча в состоянии (`pending_enriched` / `pending_failed`).
       При наличии повторяет отправку в Sink без расхода квоты API.
    2. Запрашивает батч ID матчей из абстрактного источника (source(batch_size)).
    3. Обогащает пачку через StratzClient с точечной изоляцией частичных сбоев.
    4. Передает результат в Sink (sink(enriched, failed)). При отказе приёмника сохраняет
       батч в стейт и ожидает следующей попытки.
    """

    name = "match_enricher"

    def __init__(
        self,
        client: StratzClient,
        source: Callable[[int], list[int]],
        sink: Callable[[list[dict[str, Any]], dict[int, str]], bool],
        quota: Quota,
        config_factory: Callable[[], MatchEnricherConfig],
    ) -> None:
        self.client = client
        self.source = source
        self.sink = sink
        self.quota = quota
        self.config_factory = config_factory

    def execute(
        self,
        state: MatchEnricherState,
    ) -> tuple[MatchEnricherState, float]:
        config = self.config_factory()

        if state.pending_enriched or state.pending_failed:
            if not self.sink(state.pending_enriched, state.pending_failed):
                logger.warning(
                    "[%s] Sink rejected pending batch (%d enriched, %d failed). Yielding.",
                    self.name,
                    len(state.pending_enriched),
                    len(state.pending_failed),
                )
                return state, config.cooldown
            return MatchEnricherState(), config.cooldown

        match_ids = self.source(config.batch_size)
        if not match_ids:
            logger.info("[%s] Queue is empty or no mature matches ready. Idling for %.1fs.", self.name, config.idle_delay)
            return state, config.idle_delay

        if not self.quota.acquire(cost=1):
            logger.warning("[%s] Quota exhausted. Retrying in %.1f s.", self.name, config.quota_retry_delay)
            return state, config.quota_retry_delay

        try:
            batch_result = self.client.get_matches(match_ids, batch_size=config.batch_size)
        except StratzRateLimitError as err:
            logger.warning("[%s] Stratz rate limit hit (HTTP 429): %s. Sleeping for %.1f s.", self.name, err, config.rate_limit_delay)
            return state, config.rate_limit_delay
        except StratzError as err:
            logger.warning("[%s] Stratz request failed: %s. Sleeping for %.1f s.", self.name, err, config.error_delay)
            return state, config.error_delay

        if not self.sink(batch_result.enriched, batch_result.failed):
            logger.warning(
                "[%s] Sink rejected batch (%d enriched, %d failed). Storing in pending state.",
                self.name,
                len(batch_result.enriched),
                len(batch_result.failed),
            )
            return (
                MatchEnricherState(
                    pending_enriched=batch_result.enriched,
                    pending_failed=batch_result.failed,
                ),
                config.cooldown,
            )

        logger.info(
            "[%s] Successfully enriched %d matches (%d failed).",
            self.name,
            len(batch_result.enriched),
            len(batch_result.failed),
        )
        return MatchEnricherState(), config.cooldown

    def close(self) -> None:
        self.client.close()
