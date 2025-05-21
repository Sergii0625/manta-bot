import os
import logging
import asyncio
from aiohttp import web
from telegram_bot import state, scanner, schedule_restart

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

# Получение переменных окружения
WEBHOOK_PATH = '/webhook'
WEBHOOK_URL = f"https://manta-bot.onrender.com{WEBHOOK_PATH}"
PORT = int(os.getenv("PORT", 8000))  # Используем PORT из Render или 8000 по умолчанию

async def webhook(request):
    """Обработка входящих обновлений от Telegram"""
    try:
        update = await request.json()
        logger.debug(f"Received update: {update}")
        await state.dp.feed_raw_update(state.bot, update)
        return web.json_response({'status': 'ok'})
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return web.json_response({'status': 'error'}, status=500)

async def start_background_tasks():
    """Запуск фоновых задач"""
    try:
        tasks = [
            asyncio.create_task(state.background_price_fetcher()),
            asyncio.create_task(schedule_restart()),
            asyncio.create_task(scanner.start_monitoring(state))
        ]
        logger.info("Background tasks started")
        return tasks
    except Exception as e:
        logger.error(f"Error in background tasks: {str(e)}")
        raise

async def init_bot(app):
    """Инициализация бота и установка webhook"""
    try:
        logger.info("Starting bot initialization")
        await state.bot.set_webhook(WEBHOOK_URL)
        logger.info(f"Webhook set to {WEBHOOK_URL}")
        app['background_tasks'] = await start_background_tasks()
    except Exception as e:
        logger.error(f"Error initializing bot: {str(e)}")
        raise

async def cleanup(app):
    """Очистка при завершении работы"""
    try:
        logger.info("Cleaning up...")
        for task in app.get('background_tasks', []):
            task.cancel()
        await state.bot.delete_webhook()
        await scanner.close()
        logger.info("Cleanup completed")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

def create_app():
    """Создание приложения aiohttp"""
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, webhook)
    app.on_startup.append(init_bot)
    app.on_cleanup.append(cleanup)
    return app

async def main():
    """Основная функция для запуска сервера"""
    try:
        app = create_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        logger.info(f"HTTP server started on port {PORT}")
        await site.start()
        await asyncio.Event().wait()  # Держим сервер запущенным
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())