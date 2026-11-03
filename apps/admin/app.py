from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from apps.match_collector.container import ApplicationContainer

from .config import AdminConfig
from .routes import router


def create_app(
    admin_config: AdminConfig | None = None,
    container: ApplicationContainer | None = None,
) -> FastAPI:
    """Создает и настраивает экземпляр веб-приложения FastAPI для панели управления.

    Архитектурные принципы:
    1. Инкапсуляция ресурсов: управляет жизненным циклом (lifespan) контейнера приложения
       (ApplicationContainer) с гарантированным закрытием подключений к БД и сессий при остановке.
    2. Разделение статики и API: предоставляет REST API под префиксом /api и монтирует
       SPA-панель управления на корневой эндпоинт и /admin.
    3. Открытость для расширения: поддерживает внедрение предварительно инициализированного
       контейнера для интеграционного тестирования.
    """
    config = admin_config or AdminConfig()
    static_dir = Path(__file__).resolve().parent / "static"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.container is None:
            app.state.container = ApplicationContainer(config.app_config)
        try:
            yield
        finally:
            if app.state.container is not None:
                app.state.container.close()

    app = FastAPI(
        title="Dota 2 Collector Control Plane",
        description="Admin dashboard and orchestration API for match collection pipeline.",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.state.admin_config = config
    app.state.container = container

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)

    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

        @app.get("/", include_in_schema=False)
        async def root():
            return FileResponse(static_dir / "index.html")

        @app.get("/admin", include_in_schema=False)
        async def admin():
            return FileResponse(static_dir / "index.html")

    return app
