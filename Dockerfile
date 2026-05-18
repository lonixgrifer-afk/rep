FROM python:3.11-slim

# Устанавливаем системные утилиты
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала копируем и устанавливаем зависимости из requirements.txt
COPY requirements.txt .
RUN pip install "python-telegram-bot[job-queue]"

# Принудительно ставим Playwright и качаем бинарники браузера вместе с системными либами
RUN pip install playwright
RUN playwright install --with-deps chromium

# Копируем всё остальное
COPY . .

# Команда запуска вашего бота
CMD ["python", "max.py"]
