"""Универсальный раннер жизненного цикла воркеров (Runner)."""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any

try:
    from .backend import Backend
    from .status import WorkerStatus
    from .worker import Worker
except ImportError:
    from packages.workers.backend import Backend
    from packages.workers.status import WorkerStatus
    from packages.workers.worker import Worker

logger = logging.getLogger("packages.workers.runner")


class Runner[TState]:
    """Универсальный раннер жизненного цикла воркера (Fail-Fast модель).

    Координирует исполнение воркера через внешний Backend:
    1. Защита от параллельного запуска: кроссплатформенный файловый лок
       (msvcrt на Windows, fcntl на Linux) на уровне ОС с фиксацией PID процесса.
    2. Внешнее управление активностью: перед каждым шагом проверяет `backend.is_enabled()`.
       Если воркер отключен в БД, раннер переходит в режим циклического прерываемого ожидания.
    3. Fail-Fast обработка сбоев: раннер не маскирует и не глушит необработанные
       исключения метода `execute()`. При возникновении исключения раннер фиксирует полный
       трейсбек в `backend.set_error()`, корректно освобождает ресурсы и аварийно завершает работу.
       Вся логика локальных повторов (retry/rate limit) должна реализовываться внутри самого воркера.
    4. Персистентность стейта: стейт сохраняется в `backend.set_state()` после каждого
       успешного вызова `execute()`, а также гарантированно в секции `finally` при завершении процесса.
    5. Graceful shutdown: перехватывает сигналы SIGINT / SIGTERM в главном потоке,
       мгновенно прерывает сон через системный примитив `threading.Event` и корректно освобождает ресурсы.
    """

    def __init__(
        self,
        worker: Worker[TState],
        backend: Backend,
        default_state: TState,
        lock_dir: str | Path,
        disabled_poll_interval: float,
    ) -> None:
        if not getattr(worker, "name", None):
            raise ValueError("Worker instance must have a non-empty 'name' attribute.")
        if default_state is None:
            raise ValueError("default_state cannot be None.")
        self.worker = worker
        self.backend = backend
        self.default_state = default_state
        self.lock_dir = Path(lock_dir)
        self.disabled_poll_interval = disabled_poll_interval
        self._stop_event = threading.Event()
        self._lock_file: Any = None
        self._status: WorkerStatus = WorkerStatus.STOPPED

    @property
    def status(self) -> WorkerStatus:
        """Возвращает текущий локальный статус выполнения воркера."""
        return self._status

    @property
    def is_busy(self) -> bool:
        """Возвращает True, если воркер прямо сейчас выполняет метод execute()."""
        return self._status == WorkerStatus.EXECUTING

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def stop(self) -> None:
        self._stop_event.set()

    def _set_status(self, status: WorkerStatus, pid: int | None = None) -> None:
        self._status = status
        try:
            self.backend.set_status(self.worker.name, status, pid=pid)
        except Exception as err:
            logger.error("[%s] Failed to record status '%s' in backend: %s", self.worker.name, status, err)

    def enable(self) -> None:
        """Активирует воркер в бэкенде (разрешает выполнение execute)."""
        self.backend.enable(self.worker.name)

    def disable(self) -> None:
        """Деактивирует воркер в бэкенде (переводит воркер в режим ожидания)."""
        self.backend.disable(self.worker.name)

    def is_enabled(self) -> bool:
        """Проверяет актуальный статус активности воркера в бэкенде."""
        return self.backend.is_enabled(self.worker.name)

    def _acquire_lock(self) -> bool:
        try:
            self.lock_dir.mkdir(parents=True, exist_ok=True)
            lock_path = self.lock_dir / f"{self.worker.name}.lock"
            self._lock_file = open(lock_path, "a+b")

            if sys.platform == "win32":
                import msvcrt

                self._lock_file.seek(0, os.SEEK_END)
                if self._lock_file.tell() == 0:
                    self._lock_file.write(b" ")
                    self._lock_file.flush()
                self._lock_file.seek(0)
                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            try:
                self._lock_file.seek(0)
                self._lock_file.truncate()
                self._lock_file.write(f"{os.getpid()}\n".encode("utf-8"))
                self._lock_file.flush()
            except Exception:
                pass
            return True

        except Exception as err:
            logger.warning(
                "[%s] Failed to acquire process lock (%s). Another instance is already running.",
                self.worker.name,
                err,
            )
            if self._lock_file is not None:
                try:
                    self._lock_file.close()
                except Exception:
                    pass
                self._lock_file = None
            return False

    def _release_lock(self) -> None:
        if self._lock_file is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                self._lock_file.seek(0)
                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        finally:
            try:
                self._lock_file.close()
            except Exception:
                pass
            self._lock_file = None

    def _setup_signals(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, self._handle_signal)
        except (ValueError, AttributeError) as err:
            logger.debug("[%s] Failed to register signal handlers: %s", self.worker.name, err)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        logger.info("[%s] Termination signal received (%s). Stopping...", self.worker.name, signum)
        self.stop()

    def sleep(self, seconds: float, check_enabled: bool = False) -> bool:
        """Выполняет прерываемый квантованный сон.

        Разбивает ожидание на короткие кванты (до 2 секунд). Прерывается
        мгновенно при остановке процесса либо при деактивации воркера в БД (если check_enabled=True).

        Args:
            seconds: Общая длительность сна в секундах.
            check_enabled: Если True, проверяет backend.is_enabled() на каждом кванте.

        Returns:
            bool: True, если была запрошена остановка процесса (_stop_event).
        """
        if seconds <= 0:
            return self._stop_event.is_set()

        end_time = time.monotonic() + seconds
        chunk = min(2.0, self.disabled_poll_interval)

        while not self._stop_event.is_set():
            remaining = end_time - time.monotonic()
            if remaining <= 0:
                break
            timeout = min(remaining, chunk)
            if self._stop_event.wait(timeout=timeout):
                return True
            if check_enabled and not self.is_enabled():
                break

        return self._stop_event.is_set()

    def load_state(self) -> TState:
        try:
            state = self.backend.get_state(self.worker.name, state_cls=type(self.default_state))
            if state is not None:
                return state
        except Exception as err:
            logger.warning(
                "[%s] Failed to restore state from backend (%s). Using default state.",
                self.worker.name,
                err,
            )
        return self.default_state

    def save_state(self, state: Any) -> None:
        self.backend.set_state(self.worker.name, state)

    def close(self) -> None:
        try:
            self.worker.close()
        except Exception as err:
            logger.error("[%s] Error while closing worker: %s", self.worker.name, err)

        try:
            self.backend.close()
        except Exception as err:
            logger.error("[%s] Error while closing backend: %s", self.worker.name, err)

        self._release_lock()

    def run(self, auto_enable: bool = False) -> None:
        """Запускает основной жизненный цикл воркера (блокирующий вызов процесса).

        Args:
            auto_enable: Если True, принудительно активирует воркер в бэкенде
                         перед началом цикла (ставит is_enabled = True).
        """
        if not self._acquire_lock():
            logger.error(
                "[%s] Startup aborted: worker process is already running.",
                self.worker.name,
            )
            return

        worker_type = (
            getattr(self.worker, "worker_type", None)
            or f"{self.worker.__class__.__module__}.{self.worker.__class__.__qualname__}"
        )
        try:
            self.backend.register_worker(self.worker.name, worker_type)
        except Exception as err:
            logger.error("[%s] Worker identity check failed: %s", self.worker.name, err)
            self.close()
            raise

        if auto_enable:
            try:
                self.enable()
            except Exception as err:
                logger.error("[%s] Failed to auto-enable worker: %s", self.worker.name, err)

        was_disabled = False
        current_pid = os.getpid()
        self._setup_signals()
        state: TState = self.load_state()

        try:
            is_active: bool = False
            try:
                is_active = self.is_enabled()
                status_str = "ENABLED" if is_active else "DISABLED"
            except Exception:
                status_str = "UNKNOWN"

            logger.info(
                "[%s] Starting worker via Runner. Status: %s",
                self.worker.name,
                status_str,
            )

            initial_status = WorkerStatus.SLEEPING if is_active else WorkerStatus.WAITING
            self._set_status(initial_status, pid=current_pid)

            while not self._stop_event.is_set():
                if not self.is_enabled():
                    if not was_disabled:
                        logger.info(
                            "[%s] Worker is disabled (is_enabled=False). Waiting for activation...",
                            self.worker.name,
                        )
                        was_disabled = True
                        self._set_status(WorkerStatus.WAITING, pid=current_pid)
                    else:
                        logger.debug(
                            "[%s] Worker is disabled. Polling again in %.1f sec...",
                            self.worker.name,
                            self.disabled_poll_interval,
                        )
                    if self.sleep(self.disabled_poll_interval):
                        break
                    continue

                if was_disabled:
                    logger.info(
                        "[%s] Worker activated (is_enabled=True). Resuming execution.",
                        self.worker.name,
                    )
                    was_disabled = False

                self._set_status(WorkerStatus.EXECUTING, pid=current_pid)
                try:
                    new_state, delay = self.worker.execute(state)
                except Exception as exec_err:
                    error_trace = traceback.format_exc()
                    error_msg = f"{type(exec_err).__name__}: {exec_err}\n{error_trace}"
                    logger.critical(
                        "[%s] Fatal error in worker execution: %s. Terminating runner.",
                        self.worker.name,
                        exec_err,
                    )
                    try:
                        self.backend.set_error(self.worker.name, error_msg)
                    except Exception as save_err:
                        logger.error(
                            "[%s] Failed to record error in backend: %s",
                            self.worker.name,
                            save_err,
                        )
                    raise exec_err
                else:
                    if not self._stop_event.is_set():
                        self._set_status(WorkerStatus.SLEEPING, pid=current_pid)

                self.save_state(new_state)
                state = new_state
                self.backend.clear_error(self.worker.name)

                if self._stop_event.is_set():
                    break

                if self.sleep(delay, check_enabled=True):
                    break

        except KeyboardInterrupt:
            logger.info("[%s] Interrupted by user (KeyboardInterrupt).", self.worker.name)
        finally:
            try:
                self.save_state(state)
            except Exception as save_err:
                logger.error("[%s] Error saving final state: %s", self.worker.name, save_err)

            self._set_status(WorkerStatus.STOPPED, pid=None)
            self.close()
            logger.info("[%s] Worker stopped successfully.", self.worker.name)
