import logging
from aiogram import Bot, Dispatcher
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from telegram_bot import BotState, Scanner
import os
import asyncio

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)


async def main():
    logger.info("Starting bot initialization")

    # Инициализация Scanner и BotState
    scanner = Scanner()
    bot_state = BotState(scanner)

    # Создание веб-приложения
    app = web.Application()

    # Настройка webhook
    webhook_path = "/webhook"
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=bot_state.dp,
        bot=bot_state.bot,
        secret_token=os.getenv("WEBHOOK_SECRET", "my-secret-token"),
    )
    webhook_requests_handler.register(app, path=webhook_path)
    setup_application(app, bot_state.dp, bot=bot_state.bot)

    # Установка webhook при старте
    async def on_startup():
        webhook_url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}{webhook_path}"
        await bot_state.bot.set_webhook(webhook_url, secret_token=os.getenv("WEBHOOK_SECRET", "my-secret-token"))
        logger.info(f"Webhook установлен: {webhook_url}")

    # Удаление webhook при завершении
    async def on_shutdown():
        await bot_state.bot.delete_webhook()
        await scanner.close()
        logger.info("Webhook удалён, соединения закрыты")

    bot_state.dp.startup.register(on_startup)
    bot_state.dp.shutdown.register(on_shutdown)

    # Запуск веб-сервера
    port = int(os.getenv("PORT", 8080))
    logger.info(f"Starting HTTP server on port {port}")
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    asyncio.run(main())