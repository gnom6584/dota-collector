FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    DB_PATH=/app/data/matches.db \
    LOCK_DIR=/app/data/locks \
    ADMIN_HOST=0.0.0.0 \
    ADMIN_PORT=8000

WORKDIR /app

# Установка зависимостей Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода приложения
COPY config.py init_db_sqlite.py launcher.py ./
COPY packages/ ./packages/
COPY apps/ ./apps/

# Создание директории данных под персистентный том SQLite
RUN mkdir -p /app/data/locks

VOLUME ["/app/data"]

EXPOSE 8000

CMD ["python", "launcher.py"]
