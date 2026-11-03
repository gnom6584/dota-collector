# Workers

Изолированный пакет рантайма фоновых воркеров, оркестрации жизненного цикла и абстрактного контракта персистентности.

## Архитектура

```
packages/workers/
├── docs/
│   └── README.md      # Документация пакета
├── worker.py          # Базовый контракт воркера (Worker)
├── runner.py          # Оркестратор жизненного цикла (Runner)
├── backend.py         # Абстрактный контракт персистентности (Backend)
└── __init__.py        # Публичный API пакета
```

### 1. `Worker` (worker.py)
Stateless-контракт воркера. Содержит только имя и интерфейс выполнения:
- `name: str` — уникальный строковый идентификатор воркера;
- `worker_type: str | None` — опциональный идентификатор типа воркера (по умолчанию `module.ClassName`);
- `execute(state: TState) -> tuple[TState, float]` — выполнение одной итерации, возвращающее обновленный стейт и длительность сна в секундах до следующего запуска;
- `close()` — освобождение локальных ресурсов воркера при остановке.

### 2. `Runner` (runner.py)
Оркестратор исполнения воркера:
- **Single-Instance Protection:** захват эксклюзивного файлового лока ОС (`data/locks/<name>.lock`) через `msvcrt` (Windows) / `fcntl` (Linux) с фиксацией PID;
- **Type Fingerprinting (Защита от подмены):** валидация сигнатуры `worker_type` при старте. Если под тем же именем запускается другой класс, раннер блокирует запуск с `TypeError`;
- **Управление активностью:** методы `enable()` / `disable()` / `is_enabled()`. По умолчанию воркер не активен (`is_enabled=False`) и ожидает разрешения на запуск без падения процесса;
- **Мониторинг физического статуса (`status`, `is_busy`):** непрерывная фиксация состояния воркера (`STOPPED`, `WAITING`, `SLEEPING`, `EXECUTING`) в памяти и в таблице `worker_control`;
- **Fail-Fast обработка сбоев:** перехват необработанных исключений `execute()`, фиксация трейсбека через `backend.set_error()`, гарантированное освобождение ресурсов (`close()`) и аварийное завершение процесса. Локальные ретраи реализуются внутри `execute()`;
- **Graceful Shutdown & Квантованный сон:** перехват `SIGINT` / `SIGTERM`, мгновенное прерывание сна через `threading.Event`, сон короткими квантами (до 2 сек) с проверкой флага `is_enabled()` в БД, сохранение финального стейта и освобождение блокировок.

### 3. `Backend` (backend.py)
Абстрактный интерфейс персистентности, разделяющий четыре независимых контура:
1. **Управление (`is_enabled`, `register_worker`, `enable`, `disable`)** — регистрация сигнатуры типа и контроль активности воркера (по умолчанию деактивирован);
2. **Статус (`set_status`, `get_status`)** — фиксация и чтение физического состояния выполнения и PID процесса;
3. **Состояние (`get_state`, `set_state`)** — сохранение и чтение пользовательского стейта `Any`;
4. **Ошибки (`set_error`, `clear_error`)** — фиксация и сброс исключений рантайма.

> Реализация бэкенда для SQLite вынесена в отдельный пакет [`packages/workers_sqlite`](../../workers_sqlite).

---

## Пример использования

```python
from packages.workers import Backend, Runner, Worker
from packages.workers_sqlite import SqliteBackend
import sqlite3


class CounterWorker(Worker[int]):
    name = "counter"

    def execute(self, state: int) -> tuple[int, float]:
        return state + 1, 1.0


conn = sqlite3.connect("data/app.db", check_same_thread=False)
backend = SqliteBackend(conn=conn)
worker = CounterWorker()

runner = Runner(
    worker=worker,
    backend=backend,
    default_state=0,
)

runner.run(auto_enable=True)
```
