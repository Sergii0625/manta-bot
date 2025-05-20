import os
import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from telegram_bot import BotState, scanner, create_main_keyboard, ALLOWED_USERS

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Настройки
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN не установлен в переменных окружения")

WEBHOOK_HOST = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}"
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 8080))

async def main():
    # Инициализация бота и диспетчера
    bot = Bot(token=TELEGRAM_TOKEN)
    dp = Dispatcher()

    # Инициализация BotState
    state = BotState(scanner, ALLOWED_USERS)
    dp["state"] = state

    # Регистрация обработчиков
    from telegram_bot import register_handlers
    register_handlers(dp)

    # Настройка webhook
    async def on_startup(app):
        try:
            await bot.set_webhook(url=WEBHOOK_URL)
            logger.info(f"Webhook установлен: {WEBHOOK_URL}")
            await state.set_menu_button()
            for user_id, _ in state.allowed_users:
                await state.init_user_state(user_id)
                state.init_user_stats(user_id)
                await state.load_user_stats(user_id)
        except Exception as e:
            logger.error(f"Ошибка в on_startup: {e}")
            raise

    async def on_shutdown(app):
        try:
            await bot.delete_webhook()
            await bot.session.close()  # Явно закрываем сессию бота
            logger.info("Webhook удалён")
            await scanner.close()
        except Exception as e:
            logger.error(f"Ошибка в on_shutdown: {e}")

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Запуск HTTP-сервера
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    # Health check для Render
    async def health_check(request):
        return web.Response(text="OK")
    app.router.add_get('/', health_check)

    # Запуск приложения
    try:
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, WEBAPP_HOST, WEBAPP_PORT)
        await site.start()
        logger.info(f"HTTP сервер запущен на порту {WEBAPP_PORT}")

        # Запуск фоновых задач
        tasks = [
            state.background_price_fetcher(),
            state.scanner.monitor_gas(state.INTERVAL, state.monitor_gas_callback),
            state.schedule_restart(),
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Ошибка при запуске сервера: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")