from __future__ import annotations

import logging
import sys
import uvicorn

from .app import create_app
from .config import AdminConfig

logger = logging.getLogger("apps.admin")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def main() -> None:
    """Точка входа для запуска HTTP-сервера панели администратора."""
    _setup_logging()
    config = AdminConfig()
    app = create_app(admin_config=config)
    logger.info("Starting admin control plane on http://%s:%d", config.host, config.port)
    uvicorn.run(app, host=config.host, port=config.port, log_config=None)


if __name__ == "__main__":
    main()
