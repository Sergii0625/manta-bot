import os
import asyncio
import logging
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.webhook.aiohttp_server import setup_application, TokenBasedRequestHandler

from telegram_bot import BotState  # Твой модуль для состояния бота
from monitoring_scanner import Scanner  # Твой модуль для сканнера

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def on_startup(app: web.Application) -> None:
    """Инициализация при старте приложения."""
    bot: Bot = app["bot"]
    dispatcher: Dispatcher = app["dispatcher"]
    webhook_secret = os.getenv("WEBHOOK_SECRET", "my-secret-token")

    # Установка webhook
    webhook_url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}/webhook"
    await bot.set_webhook(
        url=webhook_url,
        secret_token=webhook_secret,
        drop_pending_updates=True
    )
    logger.info(f"Webhook установлен: {webhook_url}")


async def on_shutdown(app: web.Application) -> None:
    """Очистка при остановке приложения."""
    bot: Bot = app["bot"]
    await bot.delete_webhook()
    await bot.session.close()
    logger.info("Webhook удалён, сессия закрыта")


async def main():
    """Основная функция для запуска бота и сервера."""
    # Инициализация бота
    bot = Bot(token=os.getenv("TELEGRAM_TOKEN"))
    dispatcher = Dispatcher()

    # Инициализация твоих модулей
    bot_state = BotState()
    scanner = Scanner()

    # Регистрация обработчиков (предполагаю, что они в telegram_bot.py)
    dispatcher["bot_state"] = bot_state
    dispatcher["scanner"] = scanner

    # Создание aiohttp приложения
    app = web.Application()
    app["bot"] = bot
    app["dispatcher"] = dispatcher

    # Настройка webhook
    setup_application(app, dispatcher, bot=bot)

    # Добавление хендлеров для проверки работоспособности
    async def health_check(request):
        return web.json_response({"status": "ok"})

    app.router.add_get("/health", health_check)

    # Регистрация startup/shutdown хендлеров
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    # Запуск сервера
    port = int(os.getenv("PORT", 10000))
    logger.info(f"Starting HTTP server on port {port}")
    return app


if __name__ == "__main__":
    # Запуск приложения через aiohttp
    app = asyncio.run(main())
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))