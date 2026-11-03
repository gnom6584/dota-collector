# Match Collector Application

Приложение фонового сбора, обогащения и сохранения публичных матчей Dota 2.

## Архитектура

```
apps/match_collector/
├── docs/
│   └── README.md        # Архитектурная документация приложения
├── config.py            # AppConfig (Pydantic BaseSettings: токены, лимиты, пути)
├── container.py         # ApplicationContainer (Composition Root зависимостей)
├── runners.py           # Функции запуска воркеров в процессах ОС
├── main.py              # Точка входа CLI для запуска воркеров
└── __init__.py          # Публичный API приложения
```

---

## Архитектурные принципы

1. **Композиционный корень (Composition Root):** `ApplicationContainer` централизованно собирает все сервисы (Data Plane, Control Plane, Workers, Runners, Quotas, Storage, Pipeline) без жесткого взаимного связывания пакетов.
2. **Изоляция процессов:** Воркеры функционируют в независимых процессах операционной системы:
   - `public_match_collector`: сбор потока матчей через OpenDota API и атомарный push в конвейер;
   - `match_enricher`: опрос созревших матчей через peek, обогащение через Stratz GraphQL API и двухфазный commit;
   - `match_storage_writer`: асинхронное чтение готовых транзакций и запись в постоянное хранилище `enriched_matches`.
3. **Безопасность параллелизма:** Межпроцессные файловые блокировки `Runner` (`data/locks/*.lock`) исключают запуск дублирующих экземпляров одного и того же воркера на уровне операционной системы.
4. **Управление через Control Plane:** Активность каждого воркера динамически контролируется через `Backend.is_enabled()`. При деактивации воркер переходит в энергосберегающий режим ожидания без падения процесса.

---

## Запуск воркеров через CLI

```bash
# Запуск всех трех воркеров одновременно
python -m apps.match_collector.main --worker all

# Запуск отдельного воркера
python -m apps.match_collector.main --worker collector
python -m apps.match_collector.main --worker enricher
python -m apps.match_collector.main --worker writer
```
