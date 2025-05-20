import asyncio
import logging
import os
from aiohttp import web
from telegram_bot import scanner, state, schedule_restart, monitor_gas_callback

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

# Константы
WEBHOOK_PATH = '/webhook'
PORT = int(os.getenv('PORT', 8000))
WEBHOOK_URL = 'https://manta-bot.onrender.com'  # URL для webhook
INTERVAL = 60

async def handle_webhook(request):
    """Обрабатывает входящие обновления от Telegram."""
    try:
        update = await request.json()
        await state.dp.feed_raw_update(state.bot, update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return web.Response(status=500)

async def health_check(request):
    """Обрабатывает пинг-запросы от Render/UptimeRobot."""
    return web.Response(text="OK")

async def setup_webhook():
    """Настраивает webhook для Telegram."""
    try:
        await state.bot.delete_webhook(drop_pending_updates=True)
        webhook_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        await state.bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook set to {webhook_url}")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        raise

async def main():
    logger.info("Starting bot initialization")

    # Инициализация пользователей
    for user_id, _ in state.user_states:
        await state.init_user_state(user_id)
        state.init_user_stats(user_id)
        await state.load_user_stats(user_id)

    # Настройка HTTP-сервера
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get('/', health_check)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"HTTP server started on port {PORT}")

    # Настройка webhook
    await setup_webhook()

    # Запуск фоновых задач
    tasks = [
        scanner.monitor_gas(INTERVAL, monitor_gas_callback),
        schedule_restart(),
        state.background_price_fetcher()
    ]

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error in background tasks: {e}")
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())