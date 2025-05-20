import os
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from telegram_bot import BotState, scanner, create_main_keyboard

# Настройки
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_HOST = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}"  # Домен Render
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
WEBAPP_HOST = "0.0.0.0"  # Для Render
WEBAPP_PORT = int(os.getenv("PORT", 8080))  # Порт, назначенный Render

async def main():
    # Инициализация бота и диспетчера
    bot = Bot(token=TELEGRAM_TOKEN)
    dp = Dispatcher()

    # Инициализация BotState
    state = BotState(scanner)
    dp["state"] = state  # Передаём state в диспетчер для использования в telegram_bot.py

    # Регистрация обработчиков
    from telegram_bot import register_handlers
    register_handlers(dp)

    # Настройка webhook
    async def on_startup(_):
        await bot.set_webhook(url=WEBHOOK_URL)
        print(f"Webhook установлен: {WEBHOOK_URL}")
        await state.set_menu_button()
        for user_id, _ in state.ALLOWED_USERS:
            await state.init_user_state(user_id)
            state.init_user_stats(user_id)
            await state.load_user_stats(user_id)

    async def on_shutdown(_):
        await bot.delete_webhook()
        print("Webhook удалён")
        await scanner.close()

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
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEBAPP_HOST, WEBAPP_PORT)
    await site.start()
    print(f"HTTP сервер запущен на порту {WEBAPP_PORT}")

    # Запуск фоновых задач
    tasks = [
        state.background_price_fetcher(),
        state.scanner.monitor_gas(state.INTERVAL, state.monitor_gas_callback),
        state.schedule_restart(),
    ]
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())