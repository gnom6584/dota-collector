from __future__ import annotations

import logging
import sqlite3

from init_db_sqlite import init_database
from packages.config_store_sqlite import SqliteConfigStore
from packages.control_plane import ControlPlane
from packages.match_enricher import MatchEnricher, MatchEnricherConfig, MatchEnricherState
from packages.match_storage_sqlite import SqliteStorage
from packages.match_storage_writer import (
    MatchStorageWriter,
    MatchStorageWriterConfig,
    MatchStorageWriterState,
)
from packages.opendota import OpenDotaClient
from packages.public_match_collector import (
    PublicMatchCollector,
    PublicMatchCollectorConfig,
    PublicMatchCollectorState,
)
from packages.public_match_pipeline import (
    PublicMatchPipeline,
    PublicMatchPipelineImpl,
    SqlitePipelineControl,
)
from packages.quotas_sqlite import SqliteDailyQuota
from packages.stratz import StratzClient
from packages.workers import Runner
from packages.workers_sqlite import SqliteBackend

from .config import AppConfig

logger = logging.getLogger("apps.match_collector.container")


class ApplicationContainer:
    """Контейнер зависимостей (Composition Root) приложения сбора и обогащения матчей.

    Архитектурные принципы:
    1. Изоляция контекста процесса: инкапсулирует соединение SQLite и граф связанных
       компонентов. Для запуска воркеров в отдельных процессах каждый процесс создает
       собственный изолированный экземпляр контейнера.
    2. Централизованная сборка Data Plane: соединяет выход коллектора со входом конвейера,
       а созревшие элементы конвейера — с обогатителем и буфером транзакций.
    3. Централизованная сборка Control Plane: связывает бэкенд воркеров, хранилища
       конфигураций, квоты и конвейер в единый фасад управления без утечки деталей реализации.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        init_database(self.config.db)

        self.conn = sqlite3.connect(
            self.config.db.db_path,
            check_same_thread=False,
            timeout=30.0,
            isolation_level=None,
        )
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")

        self.backend = SqliteBackend(conn=self.conn, prefix="")
        self.storage = SqliteStorage(
            conn=self.conn,
            max_matches=self.config.max_storage_matches,
            table_name=self.config.db.storage_table,
            config_table_name=self.config.db.worker_configs_table,
        )

        self.pipeline_control = SqlitePipelineControl(
            conn=self.conn,
            default_max_size=self.config.max_queue_size,
            default_maturation_delay=self.config.maturity_cooldown_seconds,
            queue_table=self.config.db.queue_table,
            registry_table=self.config.db.registry_table,
            config_table=self.config.db.worker_configs_table,
        )

        self.pipeline = PublicMatchPipelineImpl(
            conn=self.conn,
            control=self.pipeline_control,
            queue_table=self.config.db.queue_table,
            storage_table=self.config.db.storage_table,
            registry_table=self.config.db.registry_table,
            transaction_table=self.config.db.transaction_table,
        )

        self.quota_opendota = SqliteDailyQuota(
            conn=self.conn,
            name="opendota",
            limit=self.config.opendota_daily_limit,
            table_name=self.config.db.daily_quotas_table,
        )
        self.quota_stratz = SqliteDailyQuota(
            conn=self.conn,
            name="stratz",
            limit=self.config.stratz_daily_limit,
            table_name=self.config.db.daily_quotas_table,
        )

        self.public_collector_config_store = SqliteConfigStore[PublicMatchCollectorConfig](
            conn=self.conn,
            key="public_match_collector",
            model_cls=PublicMatchCollectorConfig,
            table_name=self.config.db.worker_configs_table,
        )

        self.enricher_config_store = SqliteConfigStore[MatchEnricherConfig](
            conn=self.conn,
            key="match_enricher",
            model_cls=MatchEnricherConfig,
            table_name=self.config.db.worker_configs_table,
        )

        self.storage_writer_config_store = SqliteConfigStore[MatchStorageWriterConfig](
            conn=self.conn,
            key="match_storage_writer",
            model_cls=MatchStorageWriterConfig,
            table_name=self.config.db.worker_configs_table,
        )

        self.control_plane = ControlPlane(
            backend=self.backend,
            pipeline_control=self.pipeline_control,
            storage=self.storage,
            public_collector_config_store=self.public_collector_config_store,
            enricher_config_store=self.enricher_config_store,
            storage_writer_config_store=self.storage_writer_config_store,
            worker_quotas={
                "public_match_collector": self.quota_opendota,
                "match_enricher": self.quota_stratz,
            },
            worker_names=["public_match_collector", "match_enricher", "match_storage_writer"],
        )

        self.opendota_client = OpenDotaClient(
            token_provider=lambda: (
                self.public_collector_config_store.get().api_key
                or self.config.opendota_api_key
            ),
        )
        self.stratz_client = StratzClient(
            token_provider=lambda: (
                self.enricher_config_store.get().api_token
                or self.config.stratz_api_token
            ),
        )

        self.public_collector = PublicMatchCollector(
            client=self.opendota_client,
            sink=self.pipeline.push,
            quota=self.quota_opendota,
            config_factory=self.public_collector_config_store.get,
        )

        self.enricher = MatchEnricher(
            client=self.stratz_client,
            source=self.pipeline.peek,
            sink=self.pipeline.commit,
            quota=self.quota_stratz,
            config_factory=self.enricher_config_store.get,
        )

        self.storage_writer = MatchStorageWriter(
            source=self.pipeline,
            storage=self.storage,
            config_factory=self.storage_writer_config_store.get,
        )

    def create_public_collector_runner(self) -> Runner:
        return Runner(
            worker=self.public_collector,
            backend=self.backend,
            default_state=PublicMatchCollectorState(),
            lock_dir=self.config.db.lock_dir,
            disabled_poll_interval=self.config.db.disabled_poll_interval,
        )

    def create_enricher_runner(self) -> Runner:
        return Runner(
            worker=self.enricher,
            backend=self.backend,
            default_state=MatchEnricherState(),
            lock_dir=self.config.db.lock_dir,
            disabled_poll_interval=self.config.db.disabled_poll_interval,
        )

    def create_storage_writer_runner(self) -> Runner:
        return Runner(
            worker=self.storage_writer,
            backend=self.backend,
            default_state=MatchStorageWriterState(),
            lock_dir=self.config.db.lock_dir,
            disabled_poll_interval=self.config.db.disabled_poll_interval,
        )

    def close(self) -> None:
        """Освобождает дескрипторы внешних клиентов и соединение с базой данных."""
        try:
            self.opendota_client.close()
        except Exception as err:
            logger.error("Failed to close OpenDota client: %s", err)

        try:
            self.stratz_client.close()
        except Exception as err:
            logger.error("Failed to close Stratz client: %s", err)

        try:
            self.conn.close()
        except Exception as err:
            logger.error("Failed to close SQLite connection: %s", err)
