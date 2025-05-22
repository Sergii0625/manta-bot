import os
import logging
import asyncio
import pytz
from datetime import datetime
from aiohttp import web
from telegram_bot import state, scanner

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

# Получение переменных окружения
WEBHOOK_PATH = '/webhook'
WEBHOOK_URL = f"https://manta-bot.onrender.com{WEBHOOK_PATH}"
PORT = int(os.getenv("PORT", 10000))  # Используем PORT из Render или 10000 по умолчанию

# Времена для сброса уведомлений (например, ежедневно в 00:00 и 12:00 по Киеву)
RESTART_TIMES = ["00:00", "12:00"]

async def health_check(request):
    """Обработчик для проверки работоспособности"""
    return web.json_response({'status': 'ok'})

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

async def reset_all_notified_levels():
    """Сброс уведомлений для всех пользователей в определённое время"""
    kyiv_tz = pytz.timezone('Europe/Kyiv')
    while True:
        try:
            now = datetime.now(kyiv_tz)
            current_time = now.strftime("%H:%M")
            if current_time in RESTART_TIMES:
                for chat_id in state.user_states:
                    await state.reset_notified_levels(chat_id)
                    logger.info(f"Reset notified levels for chat_id={chat_id} at {current_time}")
                await asyncio.sleep(60)  # Ждём минуту, чтобы избежать многократного сброса
            await asyncio.sleep(30)  # Проверяем каждые 30 секунд
        except Exception as e:
            logger.error(f"Error in reset_all_notified_levels: {str(e)}")
            await asyncio.sleep(30)

async def monitor_gas_callback(gas):
    """Callback для обработки газа для всех пользователей"""
    for user_id, _ in state.ALLOWED_USERS:
        await state.get_manta_gas(user_id)

async def start_background_tasks():
    """Запуск фоновых задач"""
    try:
        logger.info("Preparing to start background tasks")
        tasks = [
            asyncio.create_task(state.background_price_fetcher(), name="background_price_fetcher"),
            asyncio.create_task(reset_all_notified_levels(), name="reset_all_notified_levels"),
            asyncio.create_task(scanner.monitor_gas(60, monitor_gas_callback), name="monitor_gas")
        ]
        logger.info("Background tasks started: background_price_fetcher, reset_all_notified_levels, monitor_gas")
        return tasks
    except Exception as e:
        logger.error(f"Error in background tasks: {str(e)}")
        raise

async def init_bot(app):
    """Инициализация бота и установка webhook"""
    try:
        logger.info("Starting bot initialization")
        # Проверка подключения к базе данных
        if not await state.check_db_connection():
            logger.error("Cannot proceed with bot initialization due to database connection failure")
            raise Exception("Database connection failed")
        await scanner.init_session()  # Инициализация сессии aiohttp
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
            logger.debug(f"Cancelled task: {task.get_name()}")
        await state.bot.delete_webhook()
        await scanner.close()
        logger.info("Cleanup completed")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

def create_app():
    """Создание приложения aiohttp"""
    app = web.Application()
    app.router.add_get('/health', health_check)  # Маршрут для проверки работоспособности
    app.router.add_post(WEBHOOK_PATH, webhook)  # Маршрут для Telegram webhook
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
        await site.start()  # Явно запускаем сайт
        logger.info(f"HTTP server started on port {PORT}")
        await asyncio.Event().wait()  # Держим сервер запущенным
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())