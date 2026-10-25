"""Главное приложение FastAPI: эндпоинты матчей, админ-панель и статика."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from src import config
from src.api.database import create_connection, init_api_db
from src.api.routes_admin import router as admin_router
from src.api.routes_api import router as matches_router

logger = logging.getLogger("api_app")
STATIC_INDEX_PATH = Path(__file__).resolve().parent / "static" / "index.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Инициализация базы данных и ключей доступа при старте приложения."""
    conn = create_connection()
    try:
        init_api_db(conn)
        logger.info("API база данных инициализирована. Сервер готов к обработке запросов.")
    finally:
        conn.close()
    yield


app = FastAPI(
    title="Dota 2 Match Collector API",
    description="API выдачи распарсенных матчей Dota 2 и административная панель управления пайплайном.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Разрешаем CORS для интеграций
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение маршрутов API и Админки
app.include_router(matches_router)
app.include_router(admin_router)


@app.get("/health", summary="Health check endpoint")
def health_check():
    """Проверка жизнеспособности сервиса для Docker healthcheck."""
    conn = create_connection()
    try:
        conn.execute("SELECT 1").fetchone()
        return {"status": "ok", "service": "match_collector"}
    except Exception as err:
        logger.error("Healthcheck failure: %s", err)
        return {"status": "degraded", "error": str(err)}
    finally:
        conn.close()


@app.get("/", summary="Главная страница панели управления", include_in_schema=False)
@app.get("/admin", summary="Панель управления пайплайном", include_in_schema=False)
def serve_admin_panel():
    """Отдаёт SPA панель управления."""
    return FileResponse(STATIC_INDEX_PATH, media_type="text/html")


def run_server() -> None:
    """Точка запуска standalone сервера."""
    import uvicorn
    uvicorn.run(
        "src.api.app:app",
        host=config.API_HOST,
        port=config.API_PORT,
        reload=False,
    )


if __name__ == "__main__":
    run_server()
