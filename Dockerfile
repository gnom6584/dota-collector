FROM python:3.12-slim

# Отключаем буферизацию логов и генерацию .pyc файлов
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    DB_PATH=/app/data/matches.db

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код приложения
COPY launcher.py ./
COPY src ./src

# Директория под SQLite базу данных
RUN mkdir -p /app/data

# Точка монтирования для сохранения БД
VOLUME ["/app/data"]

# Порт для будущего веб-сервера FastAPI и админки
EXPOSE 8000

# Точка входа: лаунчер пайплайна
CMD ["python", "launcher.py"]
