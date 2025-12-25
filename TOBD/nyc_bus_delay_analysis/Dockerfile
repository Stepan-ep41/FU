FROM python:3.9-slim

# Установка системных зависимостей (нужны для сборки некоторых python пакетов)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копирование и установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование всего кода проекта
# (В dev-режиме docker-compose мы будем монтировать код через volume, 
# но для продакшена этот шаг обязателен)
COPY . .

# Переменная для python, чтобы вывод в консоль не буферизировался
ENV PYTHONUNBUFFERED=1

CMD ["bash"]