"""Лаунчер пайплайна сбора и обогащения матчей Dota 2 и веб-сервера FastAPI."""

from __future__ import annotations

import signal
import subprocess
import sys
import time

SERVICES = [
    ("Producer (OpenDota)", [sys.executable, "-m", "src.workers.run_match_producer"]),
    ("Enricher (Stratz)", [sys.executable, "-m", "src.workers.run_match_enricher"]),
    ("API & Admin Panel", [sys.executable, "-m", "src.api.app"]),
]


def main() -> None:
    names = ", ".join(name for name, _ in SERVICES)
    print(f"[launcher] Запуск сервисов: {names}")
    print("[launcher] -> Панель управления: http://localhost:8000/admin")
    print("[launcher] -> Документация API:  http://localhost:8000/docs")

    procs = [(name, subprocess.Popen(cmd)) for name, cmd in SERVICES]

    def handle_signal(sig, frame):
        print(f"\n[launcher] Получен сигнал {sig}. Завершаем работу сервисов...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    try:
        # Мониторим процессы: если один упал или завершился — выходим
        while True:
            for name, p in procs:
                code = p.poll()
                if code is not None:
                    print(f"\n[launcher] Процесс '{name}' завершился (код {code}). Завершаем работу...")
                    return
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        print("\n[launcher] Остановка процессов...")
        for _, p in procs:
            if p.poll() is None:
                p.terminate()

        for _, p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

        print("[launcher] Все процессы остановлены.")


if __name__ == "__main__":
    main()
