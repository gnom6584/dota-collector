from __future__ import annotations

import time
from typing import Any

from packages.config_store import ConfigStore
from packages.match_enricher import MatchEnricherConfig
from packages.match_storage import Storage
from packages.match_storage_writer import MatchStorageWriterConfig
from packages.public_match_collector import PublicMatchCollectorConfig
from packages.public_match_pipeline import PipelineControl
from packages.quotas import DailyQuota
from packages.workers import Backend, WorkerStatus

from .models import QuotaStats, SystemStats, WorkerStats


class ControlPlane:
    """Конкретный сервис управления и мониторинга системы сбора и обогащения матчей.

    Архитектурные принципы:
    1. Изоляция от слоя хранения: не зависит от конкретной СУБД и взаимодействует
       исключительно через доменные контракты (Backend, PipelineControl, Storage, DailyQuota, ConfigStore).
    2. Привязка квот к звеньям: каждая квота внешнего API привязана к конкретному воркеру
       (коллектору или обогатителю) и контролируется в контексте этого звена.
    3. Фокус на статусах здоровья: предоставляет мониторинг статусов воркеров (RUNNING, IDLE,
       WAITING, DISABLED, ERROR) и heartbeat без утечки внутреннего состояния.
    4. Динамическое конфигурирование: управляет чтением и обновлением конфигураций воркеров,
       квот и емкостей хранилищ на лету.
    """

    def __init__(
        self,
        backend: Backend,
        pipeline_control: PipelineControl,
        storage: Storage,
        public_collector_config_store: ConfigStore[PublicMatchCollectorConfig],
        enricher_config_store: ConfigStore[MatchEnricherConfig],
        worker_quotas: dict[str, DailyQuota],
        worker_names: list[str] | None = None,
        storage_writer_config_store: ConfigStore[MatchStorageWriterConfig] | None = None,
    ) -> None:
        self.backend = backend
        self.pipeline_control = pipeline_control
        self.storage = storage
        self.public_collector_config_store = public_collector_config_store
        self.enricher_config_store = enricher_config_store
        self.storage_writer_config_store = storage_writer_config_store
        self.worker_quotas = dict(worker_quotas)
        self.worker_names = list(worker_names) if worker_names else list(worker_quotas.keys())

    def get_public_collector_config(self) -> PublicMatchCollectorConfig:
        return self.public_collector_config_store.get()

    def set_public_collector_config(self, config: PublicMatchCollectorConfig) -> None:
        self.public_collector_config_store.set(config)

    def get_enricher_config(self) -> MatchEnricherConfig:
        return self.enricher_config_store.get()

    def set_enricher_config(self, config: MatchEnricherConfig) -> None:
        self.enricher_config_store.set(config)

    def get_storage_writer_config(self) -> MatchStorageWriterConfig:
        """Возвращает актуальную конфигурацию воркера match_storage_writer."""
        if self.storage_writer_config_store is None:
            return MatchStorageWriterConfig()
        return self.storage_writer_config_store.get()

    def set_storage_writer_config(self, config: MatchStorageWriterConfig) -> None:
        """Обновляет персистентную конфигурацию воркера match_storage_writer."""
        if self.storage_writer_config_store is None:
            raise RuntimeError("Storage writer config store is not registered in ControlPlane.")
        self.storage_writer_config_store.set(config)

    def get_maturation_delay(self) -> float:
        """Возвращает актуальную задержку созревания матчей в секундах."""
        return self.pipeline_control.get_maturation_delay()

    def set_maturation_delay(self, seconds: float) -> None:
        """Обновляет задержку созревания матчей в секундах."""
        self.pipeline_control.set_maturation_delay(seconds)

    def enable_worker(self, name: str) -> None:
        self.backend.enable(name)

    def disable_worker(self, name: str) -> None:
        self.backend.disable(name)

    def is_worker_enabled(self, name: str) -> bool:
        return self.backend.is_enabled(name)

    def get_worker_status(self, name: str) -> WorkerStatus:
        return self.backend.get_status(name)

    def clear_worker_error(self, name: str) -> None:
        self.backend.clear_error(name)

    def get_quota(self, name: str) -> DailyQuota:
        """Возвращает экземпляр суточной квоты по имени воркера или имени квоты."""
        if name in self.worker_quotas:
            return self.worker_quotas[name]
        for q in self.worker_quotas.values():
            if q.name == name:
                return q
        raise KeyError(f"Quota for '{name}' not found.")

    def reset_quota(self, name: str) -> None:
        """Сбрасывает израсходованный объем суточной квоты воркера."""
        quota = self.get_quota(name)
        quota.reset()

    def set_quota_limit(self, name: str, limit: int) -> None:
        """Обновляет максимальный лимит суточной квоты воркера."""
        quota = self.get_quota(name)
        quota.set_limit(limit)

    def get_queue_max_size(self) -> int:
        """Возвращает максимальную вместимость очереди созревания."""
        return self.pipeline_control.get_max_queue_size()

    def set_queue_max_size(self, max_size: int) -> None:
        """Устанавливает максимальную вместимость очереди созревания."""
        if max_size <= 0:
            raise ValueError(f"Queue max size must be positive integer, got {max_size}.")
        self.pipeline_control.set_max_queue_size(max_size)

    def get_storage_max_matches(self) -> int:
        """Возвращает максимальную вместимость целевого хранилища матчей."""
        return self.storage.max_matches

    def set_storage_max_matches(self, max_matches: int) -> None:
        """Устанавливает максимальную вместимость целевого хранилища матчей."""
        if max_matches <= 0:
            raise ValueError(f"Storage max matches must be positive integer, got {max_matches}.")
        self.storage.set_max_matches(max_matches)

    def get_system_stats(self) -> SystemStats:
        workers: list[WorkerStats] = []
        for name in self.worker_names:
            status = self.backend.get_status(name)
            status_str = status.value if hasattr(status, "value") else str(status)

            quota_stats: QuotaStats | None = None
            if name in self.worker_quotas:
                q = self.worker_quotas[name]
                quota_stats = QuotaStats(
                    name=q.name,
                    used_units=q.used(),
                    max_units=q.limit,
                    remaining_units=q.remaining(),
                    reset_at=q.reset_at(),
                )

            raw_type = self.backend.get_worker_type(name)
            clean_type = raw_type.split(".")[-1] if raw_type else name

            raw_state = self.backend.get_state(name)
            summary: dict[str, Any] | None = None
            if isinstance(raw_state, dict):
                summary = raw_state
            elif hasattr(raw_state, "model_dump"):
                summary = raw_state.model_dump(mode="json")

            workers.append(
                WorkerStats(
                    name=name,
                    worker_type=clean_type,
                    is_enabled=self.backend.is_enabled(name),
                    status=status_str,
                    pid=self.backend.get_pid(name),
                    last_error=self.backend.get_error(name),
                    updated_at=self.backend.get_updated_at(name),
                    quota=quota_stats,
                    state_summary=summary,
                )
            )

        return SystemStats(
            workers=workers,
            queue_size=self.pipeline_control.get_queue_count(),
            queue_max_size=self.pipeline_control.get_max_queue_size(),
            enriched_matches_count=self.storage.count(),
            storage_max_matches=self.storage.max_matches,
            maturation_delay_seconds=self.pipeline_control.get_maturation_delay(),
        )

    def close(self) -> None:
        pass
