import asyncio
import logging
import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from decimal import Decimal
from datetime import datetime, time
import aiohttp
import pytz
from monitoring_scanner import Scanner

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

# Константы
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CMC_API_KEY = os.getenv("CMC_API_KEY")
ALLOWED_USERS = [
    (501156257, "Сергей"), (5070159060, "Васек"), (1182677771, "Толик"),
    (6322048522, "Кумец"), (1725998320, "Света"), (7009557842, "Лайф")
]
ADMIN_ID = 501156257
INTERVAL = 60
CONFIRMATION_INTERVAL = 20
CONFIRMATION_COUNT = 3
RESTART_TIMES = ["21:00"]

def is_silent_hour(user_id, now_kyiv, user_states):
    """Проверка, находится ли время в тихих часах."""
    if not user_states[user_id].get('silent_enabled', True):
        return False
    start_time, end_time = time(0, 0), time(8, 0)
    now_time = now_kyiv.time()
    return start_time <= now_time <= end_time if start_time <= end_time else now_time >= start_time or now_time <= end_time

async def schedule_restart():
    """Планировщик перезапуска задач бота."""
    while True:
        try:
            now_kyiv = datetime.now(pytz.timezone('Europe/Kyiv'))
            if now_kyiv.strftime("%H:%M") in RESTART_TIMES:
                logger.info("Scheduled restart triggered")
                for user_id, _ in ALLOWED_USERS:
                    await state.reset_notified_levels(user_id)
                await asyncio.sleep(60)
            else:
                await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"Error in schedule_restart: {str(e)}")
            await asyncio.sleep(30)

class BotState:
    def __init__(self, scanner):
        self.bot = Bot(token=TELEGRAM_TOKEN)
        self.dp = Dispatcher()
        self.scanner = scanner
        self.user_states = {}
        self.pending_commands = {}
        self.message_ids = {}
        self.l2_data_cache = None
        self.l2_data_time = None
        self.fear_greed_cache = None
        self.fear_greed_time = None
        self.fear_greed_cooldown = 300
        self.converter_cache = None
        self.converter_cache_time = None
        self.user_stats = {}
        self.is_first_run = True
        self.price_fetch_interval = 300
        logger.info("BotState initialized")

    async def check_db_connection(self):
        logger.info("No database used, connection check skipped")
        return True

    async def init_user_state(self, user_id):
        """Инициализация состояния пользователя."""
        if user_id not in self.user_states:
            self.user_states[user_id] = {
                'prev_level': None, 'last_measured_gas': None, 'current_levels': [],
                'default_levels': [], 'user_added_levels': [], 'active_level': None,
                'confirmation_states': {}, 'notified_levels': set(), 'silent_enabled': True
            }
            await self.load_or_set_default_levels(user_id)

    def init_user_stats(self, user_id):
        """Инициализация статистики пользователя."""
        if user_id not in self.user_stats:
            self.user_stats[user_id] = {}
        today = datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()
        default_stats = {
            "Газ": 0, "Manta Price": 0, "Сравнение L2": 0, "Задать Уровни": 0,
            "Уведомления": 0, "Админ": 0, "Страх и Жадность": 0, "Тихие Часы": 0,
            "Manta Конвертер": 0, "Газ Калькулятор": 0
        }
        self.user_stats[user_id][today] = self.user_stats[user_id].get(today, default_stats)

    async def check_access(self, message: types.Message):
        """Проверка доступа пользователя."""
        chat_id = message.chat.id
        if chat_id not in [user[0] for user in ALLOWED_USERS]:
            await self.bot.send_message(chat_id, "⛽ У вас нет доступа к этому боту.")
            logger.warning(f"Access denied for chat_id={chat_id}")
            return False
        await self.init_user_state(chat_id)
        self.init_user_stats(chat_id)
        return True

    async def update_message(self, chat_id, text, reply_markup=None):
        """Отправка или обновление сообщения."""
        try:
            msg = await self.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=reply_markup)
            self.message_ids[chat_id] = msg.message_id
        except Exception as e:
            logger.error(f"Failed to send message to chat_id={chat_id}: {e}")

    async def reset_notified_levels(self, chat_id):
        """Сброс уведомленных уровней."""
        self.user_states[chat_id]['notified_levels'].clear()
        logger.info(f"Cleared notified levels for chat_id={chat_id}")

    async def confirm_level_crossing(self, chat_id, initial_value, direction, target_level):
        """Подтверждение пересечения уровня газа."""
        kyiv_tz = pytz.timezone('Europe/Kyiv')
        now_kyiv = datetime.now(kyiv_tz)
        if is_silent_hour(chat_id, now_kyiv, self.user_states):
            logger.info(f"Silent hours active for chat_id={chat_id}, skipping notification")
            return

        state = self.user_states[chat_id]['confirmation_states'].setdefault(target_level, {
            'count': 0, 'values': [], 'target_level': target_level, 'direction': direction
        })
        state['count'] = 1
        state['values'] = [initial_value]

        for i in range(CONFIRMATION_COUNT - 1):
            await asyncio.sleep(CONFIRMATION_INTERVAL)
            current_slow = await self.scanner.get_current_gas()
            if current_slow is None:
                logger.error(f"Failed to get gas on attempt {i + 2} for chat_id={chat_id}")
                del self.user_states[chat_id]['confirmation_states'][target_level]
                return
            state['count'] += 1
            state['values'].append(current_slow)

        values = state['values']
        is_confirmed = all(v <= target_level for v in values) if direction == 'down' else all(v >= target_level for v in values)

        if is_confirmed and target_level not in self.user_states[chat_id]['notified_levels']:
            notification_message = (
                f"<pre>{'🟩' if direction == 'down' else '🟥'} ◆ ГАЗ {'УМЕНЬШИЛСЯ' if direction == 'down' else 'УВЕЛИЧИЛСЯ'} до: {values[-1]:.6f} Gwei\n"
                f"Уровень: {target_level:.6f} Gwei подтверждён</pre>"
            )
            for user_id, _ in ALLOWED_USERS:
                if user_id == chat_id or target_level in self.user_states[user_id]['current_levels']:
                    await self.update_message(user_id, notification_message, create_keyboard(user_id, 'main'))
                    self.user_states[user_id]['notified_levels'].add(target_level)
                    self.user_states[user_id]['active_level'] = target_level
                    self.user_states[user_id]['prev_level'] = self.user_states[user_id]['last_measured_gas']

        del self.user_states[chat_id]['confirmation_states'][target_level]

    async def get_manta_gas(self, chat_id, force_base_message=False):
        """Получение текущего газа Manta Pacific."""
        current_slow = await self.scanner.get_current_gas()
        if current_slow is None:
            await self.update_message(chat_id, "<b>⚠️ Не удалось подключиться к Manta Pacific</b>", create_keyboard(chat_id, 'main'))
            return

        gas_str = f"{current_slow:.6f}"
        zeros = len(gas_str.split('.')[1].rstrip('0')) - len(gas_str.split('.')[1].rstrip('0').lstrip('0'))
        base_message = f"<pre>⛽️ Manta Pacific Gas\n◆ <b>ТЕКУЩИЙ ГАЗ</b>:   {current_slow:.6f} Gwei  ({zeros})</pre>"

        self.user_states[chat_id]['last_measured_gas'] = current_slow
        levels = self.user_states[chat_id]['current_levels'] or await self.load_or_set_default_levels(chat_id)

        if not levels or force_base_message or self.user_states[chat_id]['prev_level'] is None:
            await self.update_message(chat_id, base_message + ("\n\nУровни не заданы." if not levels else ""), create_keyboard(chat_id, 'main'))
        else:
            kyiv_tz = pytz.timezone('Europe/Kyiv')
            if is_silent_hour(chat_id, datetime.now(kyiv_tz), self.user_states):
                self.user_states[chat_id]['prev_level'] = current_slow
                return
            sorted_levels = sorted(levels)
            closest_level = min(sorted_levels, key=lambda x: abs(x - current_slow))
            prev_level = self.user_states[chat_id]['prev_level']
            if prev_level < closest_level <= current_slow and closest_level not in self.user_states[chat_id]['confirmation_states']:
                asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'up', closest_level))
            elif prev_level > closest_level >= current_slow and closest_level not in self.user_states[chat_id]['confirmation_states']:
                asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'down', closest_level))

        self.user_states[chat_id]['active_level'] = min(levels, key=lambda x: abs(x - current_slow)) if levels else None
        self.user_states[chat_id]['prev_level'] = current_slow

    async def load_or_set_default_levels(self, user_id):
        """Установка уровней газа по умолчанию."""
        try:
            default_levels = [Decimal(str(x / 10000)) for x in range(100, 0, -5)] + [Decimal('0.000050')]
            self.user_states[user_id]['default_levels'] = default_levels
            self.user_states[user_id]['user_added_levels'] = []
            self.user_states[user_id]['current_levels'] = default_levels.copy()
            return default_levels
        except Exception as e:
            logger.error(f"Error setting default levels for user_id={user_id}: {str(e)}")
            return []

    async def set_silent_hours(self, chat_id, enable):
        """Включение/выключение тихих часов."""
        self.user_states[chat_id]['silent_enabled'] = enable
        await self.update_message(chat_id, f"Тихие Часы {'включены' if enable else 'отключены'}: 00:00–08:00", create_keyboard(chat_id, 'main'))

    async def background_price_fetcher(self):
        """Фоновая загрузка цен."""
        while True:
            try:
                await asyncio.gather(self.fetch_converter_data(), self.fetch_l2_data())
            except Exception as e:
                logger.error(f"Error in background price fetch: {str(e)}")
            await asyncio.sleep(self.price_fetch_interval)

    async def fetch_converter_data(self):
        """Получение данных для конвертера."""
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "ids": "manta-network,ethereum,bitcoin", "per_page": 3, "page": 1, "sparkline": "false"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        return self.converter_cache
                    data = await response.json()
                    self.converter_cache = {coin["id"]: coin["current_price"] for coin in data}
                    self.converter_cache_time = datetime.now(pytz.timezone('Europe/Kyiv'))
                    return self.converter_cache
        except Exception as e:
            logger.error(f"Error fetching converter data: {str(e)}")
            return self.converter_cache

    async def convert_manta(self, chat_id, amount):
        """Конвертация MANTA в другие валюты."""
        prices = self.converter_cache or await self.fetch_converter_data()
        if not prices:
            await self.update_message(chat_id, "⚠️ Данные о ценах недоступны.", create_keyboard(chat_id, 'menu'))
            return None
        try:
            manta_usd = prices.get("manta-network")
            eth_usd = prices.get("ethereum")
            btc_usd = prices.get("bitcoin")
            result = {
                "USDT": amount * manta_usd,
                "ETH": amount * manta_usd / eth_usd if eth_usd else 0,
                "BTC": amount * manta_usd / btc_usd if btc_usd else 0
            }
            message = f"<pre>Конвертация {int(amount)} MANTA:\n◆ USDT: {result['USDT']:.2f}\n◆ ETH:  {result['ETH']:.6f}\n◆ BTC:  {result['BTC']:.8f}</pre>"
            await self.update_message(chat_id, message, create_keyboard(chat_id, 'menu'))
            return True
        except Exception as e:
            logger.error(f"Error in convert_manta: {str(e)}")
            await self.update_message(chat_id, "⚠️ Ошибка при конвертации.", create_keyboard(chat_id, 'menu'))
            return None

    async def calculate_gas_cost(self, chat_id, gas_price, tx_count):
        """Расчет стоимости газа."""
        prices = self.converter_cache or await self.fetch_converter_data()
        if not prices:
            await self.update_message(chat_id, "⚠️ Данные о ценах недоступны.", create_keyboard(chat_id, 'main'))
            return None
        try:
            eth_usd = prices.get("ethereum")
            fee_per_tx_eth = gas_price * 1000000 / 10**9
            total_cost_usdt = fee_per_tx_eth * tx_count * eth_usd
            message = f"<pre>{int(tx_count)} транзакций, газ {gas_price:.6f} Gwei = {total_cost_usdt:.4f} USDT</pre>"
            await self.update_message(chat_id, message, create_keyboard(chat_id, 'main'))
            return True
        except Exception as e:
            logger.error(f"Error in calculate_gas_cost: {str(e)}")
            await self.update_message(chat_id, "⚠️ Ошибка при расчёте стоимости газа.", create_keyboard(chat_id, 'main'))
            return None

    async def fetch_l2_data(self):
        """Получение данных L2 токенов."""
        l2_tokens = {
            "MANTA": "manta-network", "Optimism": "optimism", "Arbitrum": "arbitrum",
            "Starknet": "starknet", "ZKsync": "zksync", "Scroll": "scroll",
            "Mantle": "mantle", "Taiko": "taiko"
        }
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "ids": ",".join(l2_tokens.values()), "per_page": 250, "page": 1, "sparkline": "false", "price_change_percentage": "24h,7d,30d"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        return self.l2_data_cache or {}
                    data = await response.json()
                    token_data = {}
                    token_map = {coin["id"]: coin for coin in data}
                    for name, token_id in l2_tokens.items():
                        coin = token_map.get(token_id, {})
                        price = coin.get("current_price", "Н/Д")
                        price_change_all = ((price - coin.get("ath", price)) / coin.get("ath", price) * 100) if price != "Н/Д" and coin.get("ath") else "Н/Д"
                        ath_date = datetime.strptime(coin.get("ath_date", "1970-01-01T00:00:00.000Z"), "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y") if coin.get("ath_date") else "Н/Д"
                        atl_date = datetime.strptime(coin.get("atl_date", "1970-01-01T00:00:00.000Z"), "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y") if coin.get("atl_date") else "Н/Д"
                        token_data[name] = {
                            "price": price,
                            "24h": coin.get("price_change_percentage_24h_in_currency", "Н/Д"),
                            "7d": coin.get("price_change_percentage_7d_in_currency", "Н/Д"),
                            "30d": coin.get("price_change_percentage_30d_in_currency", "Н/Д"),
                            "all": price_change_all,
                            "ath_price": coin.get("ath", "Н/Д"),
                            "ath_date": ath_date,
                            "atl_price": coin.get("atl", "Н/Д"),
                            "atl_date": atl_date
                        }
                    self.l2_data_cache = token_data
                    self.l2_data_time = datetime.now(pytz.timezone('Europe/Kyiv'))
                    return token_data
        except Exception as e:
            logger.error(f"Error fetching L2 data: {str(e)}")
            return self.l2_data_cache or {}

    async def fetch_fear_greed(self):
        """Получение индекса страха и жадности."""
        current_time = datetime.now(pytz.timezone('Europe/Kyiv'))
        if self.fear_greed_time and (current_time - self.fear_greed_time).total_seconds() < self.fear_greed_cooldown:
            return self.fear_greed_cache
        url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical"
        headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
        params = {"limit": 30}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status != 200:
                        return None
                    data = await response.json()
                    fg_data = data.get("data", [])
                    if not fg_data:
                        return None
                    current = fg_data[0]
                    year_data = fg_data[:365]
                    year_values = [(int(d["value"]), d["timestamp"], d["value_classification"]) for d in year_data]
                    max_year = max(year_values, key=lambda x: x[0]) if year_values else (0, "1970-01-01", "")
                    min_year = min(year_values, key=lambda x: x[0]) if year_values else (0, "1970-01-01", "")
                    parse_timestamp = lambda ts: datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y") if 'T' in ts else datetime.fromtimestamp(int(ts)).strftime("%d.%m.%Y")
                    fear_greed_data = {
                        "current": {"value": int(current["value"]), "category": current["value_classification"]},
                        "yesterday": {"value": int(fg_data[1]["value"]), "category": fg_data[1]["value_classification"]},
                        "week_ago": {"value": int(fg_data[7]["value"]) if len(fg_data) > 7 else 0, "category": fg_data[7]["value_classification"] if len(fg_data) > 7 else ""},
                        "month_ago": {"value": int(fg_data[-1]["value"]), "category": fg_data[-1]["value_classification"]},
                        "year_max": {"value": max_year[0], "date": parse_timestamp(max_year[1]), "category": max_year[2]},
                        "year_min": {"value": min_year[0], "date": parse_timestamp(min_year[1]), "category": min_year[2]}
                    }
                    self.fear_greed_cache = fear_greed_data
                    self.fear_greed_time = current_time
                    return fear_greed_data
        except Exception as e:
            logger.error(f"Error fetching Fear & Greed: {str(e)}")
            return None

    async def get_manta_price(self, chat_id):
        """Получение цены MANTA."""
        token_data = self.l2_data_cache or await self.fetch_l2_data()
        if not token_data:
            await self.update_message(chat_id, "⚠️ Данные о ценах недоступны.", create_keyboard(chat_id, 'main'))
            return
        manta_data = token_data["MANTA"]
        spot_volume = await self.scanner.get_manta_spot_volume()
        futures_volume = await self.scanner.get_manta_futures_volume()
        spot_volume_str = f"{round(spot_volume / Decimal('1000000'))}M$" if spot_volume else "Н/Д"
        futures_volume_str = f"{round(futures_volume / Decimal('1000000'))}M$" if futures_volume else "Н/Д"
        message = (
            f"<pre>🦎 Данные с CoinGecko:\n"
            f"◆ MANTA/USDT: ${float(manta_data['price']):.3f}\n\n"
            f"◆ ИЗМЕНЕНИЕ:\n"
            f"◆ 24 ЧАСА:     {float(manta_data['24h']):>6.2f}%\n"
            f"◆ 7 ДНЕЙ:      {float(manta_data['7d']):>6.2f}%\n"
            f"◆ МЕСЯЦ:       {float(manta_data['30d']):>6.2f}%\n"
            f"◆ ВСЕ ВРЕМЯ:   {float(manta_data['all']):>6.2f}%\n\n"
            f"◆ Binance Volume Trade 24ч:\n"
            f"◆ (Futures):   {futures_volume_str}\n"
            f"◆ (Spot):      {spot_volume_str}\n\n"
            f"◆ ${float(manta_data['ath_price']):.2f} ({manta_data['ath_date']})\n"
            f"◆ ${float(manta_data['atl_price']):.2f} ({manta_data['atl_date']})</pre>"
        )
        await self.update_message(chat_id, message, create_keyboard(chat_id, 'main'))

    async def get_l2_comparison(self, chat_id):
        """Сравнение L2 токенов."""
        token_data = self.l2_data_cache or await self.fetch_l2_data()
        if not token_data:
            await self.update_message(chat_id, "⚠️ Данные о ценах недоступны.", create_keyboard(chat_id, 'main'))
            return
        message = "<pre>🦎 Данные с CoinGecko:\n"
        for period, label in [("24h", "24 часа"), ("7d", "7 дней"), ("30d", "месяц"), ("all", "все время")]:
            message += f"◆ Сравнение L2 токенов ({label}):\n"
            sorted_data = sorted(
                token_data.items(),
                key=lambda x: float(x[1][period]) if x[1][period] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_data:
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data[period]):>6.2f}%" if data[period] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"
        message += "</pre>"
        await self.update_message(chat_id, message, create_keyboard(chat_id, 'main'))

    async def get_fear_greed(self, chat_id):
        """Получение индекса страха и жадности."""
        fg_data = await self.fetch_fear_greed()
        if not fg_data:
            await self.update_message(chat_id, "⚠️ Не удалось получить данные Fear & Greed.", create_keyboard(chat_id, 'main'))
            return
        bar_length = 20
        filled = int(fg_data["current"]["value"] / 100 * bar_length)
        progress_bar = f"🔴 {'█' * filled}{'▁' * (bar_length - filled)} 🟢"
        message = (
            f"<pre>◆ Индекс страха и жадности: {fg_data['current']['value']}\n\n"
            f"{progress_bar}\n\n"
            f"История:\n"
            f"🕒 Вчера: {fg_data['yesterday']['value']}\n"
            f"🕒 Прошлая неделя: {fg_data['week_ago']['value']}\n"
            f"🕒 Прошлый месяц: {fg_data['month_ago']['value']}\n\n"
            f"Годовые экстремумы:\n"
            f"📈 Макс: {fg_data['year_max']['value']} ({fg_data['year_max']['date']})\n"
            f"📉 Мин: {fg_data['year_min']['value']} ({fg_data['year_min']['date']})</pre>"
        )
        await self.update_message(chat_id, message, create_keyboard(chat_id, 'main'))

    async def get_admin_stats(self, chat_id):
        """Получение статистики для админа."""
        if chat_id != ADMIN_ID:
            await self.update_message(chat_id, "Доступ только для админа.", create_keyboard(chat_id, 'main'))
            return
        today = datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()
        message = "<b>Статистика использования бота за сегодня:</b>\n\n<pre>"
        has_activity = False
        for user_id, user_name in ALLOWED_USERS:
            if user_id == ADMIN_ID:
                continue
            stats = self.user_stats.get(user_id, {}).get(today, {})
            if any(stats.values()):
                message += f"{user_id} {user_name}\n" + "\n".join(f"{k} - {v}" for k, v in stats.items() if v > 0) + "\n\n"
                has_activity = True
        message += "</pre>" if has_activity else "Сегодня никто из пользователей (кроме админа) не использовал бота."
        await self.update_message(chat_id, message, create_keyboard(chat_id, 'main'))

def create_keyboard(chat_id, keyboard_type):
    """Создание клавиатуры по типу."""
    keyboards = {
        'main': [[types.KeyboardButton(text="Газ"), types.KeyboardButton(text="Меню")]] + ([[types.KeyboardButton(text="Админ")]] if chat_id == ADMIN_ID else []),
        'menu': [
            [types.KeyboardButton(text="Manta Конвертер"), types.KeyboardButton(text="Газ Калькулятор")],
            [types.KeyboardButton(text="Manta Price"), types.KeyboardButton(text="Сравнение L2")],
            [types.KeyboardButton(text="Страх и Жадность"), types.KeyboardButton(text="Тихие Часы")],
            [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Уведомления")]
        ],
        'silent_hours': [
            [types.KeyboardButton(text="Отключить Тихие Часы"), types.KeyboardButton(text="Включить Тихие Часы")],
            [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]
        ],
        'converter': [[types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]],
        'gas_calculator': [[types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]]
    }
    return types.ReplyKeyboardMarkup(keyboard=keyboards.get(keyboard_type, keyboards['main']), resize_keyboard=True, one_time_keyboard=False)

scanner = Scanner()
state = BotState(scanner)

@state.dp.message(Command("start"))
async def start_command(message: types.Message):
    """Обработка команды /start."""
    if not await state.check_access(message):
        return
    await state.update_message(message.chat.id, "<b>Бот для Manta Pacific запущен.</b>\nВыберите действие:", create_keyboard(message.chat.id, 'main'))
    try:
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete start message: {e}")

@state.dp.message(lambda message: message.text in [
    "Газ", "Manta Price", "Сравнение L2", "Страх и Жадность", "Уведомления",
    "Админ", "Тихие Часы", "Меню", "Назад", "Manta Конвертер", "Газ Калькулятор"
])
async def handle_main_button(message: types.Message):
    """Обработка основных кнопок."""
    if not await state.check_access(message):
        return
    chat_id = message.chat.id
    text = message.text
    today = datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()
    if text not in ["Меню", "Назад"]:
        state.user_stats[chat_id][today][text] += 1
    if chat_id in state.pending_commands and text not in ["Тихие Часы", "Manta Конвертер", "Газ Калькулятор"]:
        del state.pending_commands[chat_id]

    levels = state.user_states[chat_id]['current_levels']
    levels_text = "\n".join(f"◆ {level:.6f} Gwei" for level in levels) if levels else "Уровни не установлены."

    handlers = {
        "Газ": lambda: state.get_manta_gas(chat_id, force_base_message=True),
        "Manta Price": lambda: state.get_manta_price(chat_id),
        "Сравнение L2": lambda: state.get_l2_comparison(chat_id),
        "Страх и Жадность": lambda: state.get_fear_greed(chat_id),
        "Уведомления": lambda: state.update_message(
            chat_id,
            f"<b><pre>ТЕКУЩИЕ УВЕДОМЛЕНИЯ:\n\n{levels_text}</pre></b>",
            create_keyboard(chat_id, 'main')
        ),
        "Админ": lambda: state.get_admin_stats(chat_id),
        "Тихие Часы": lambda: state.update_message(chat_id, f"Тихие Часы {'включены' if state.user_states[chat_id]['silent_enabled'] else 'отключены'}: 00:00–08:00", create_keyboard(chat_id, 'silent_hours')),
        "Manta Конвертер": lambda: state.update_message(chat_id, "Введите количество MANTA для конвертации:", create_keyboard(chat_id, 'converter')),
        "Газ Калькулятор": lambda: state.update_message(chat_id, "Введите цену газа в Gwei (например, 0.0015):", create_keyboard(chat_id, 'gas_calculator')),
        "Меню": lambda: state.update_message(chat_id, "Выберите действие:", create_keyboard(chat_id, 'menu')),
        "Назад": lambda: state.update_message(chat_id, "Возврат в главное меню.", create_keyboard(chat_id, 'main'))
    }
    if text in ["Тихие Часы", "Manta Конвертер", "Газ Калькулятор"]:
        state.pending_commands[chat_id] = {'step': text.lower().replace(' ', '_') + '_input'}
    await handlers.get(text, lambda: None)()
    try:
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete message: {e}")

@state.dp.message()
async def process_value(message: types.Message):
    """Обработка текстовых вводов."""
    if not await state.check_access(message):
        return
    chat_id = message.chat.id
    text = message.text.strip()
    if chat_id not in state.pending_commands:
        await state.update_message(chat_id, "Выберите действие с помощью кнопок.", create_keyboard(chat_id, 'main'))
        return

    state_data = state.pending_commands[chat_id]
    handlers = {
        'manta_converter_input': lambda: handle_converter_input(chat_id, text),
        'gas_calculator_input': lambda: handle_gas_calculator_input(chat_id, text, state_data),
        'silent_hours_input': lambda: handle_silent_hours_input(chat_id, text)
    }

    async def handle_converter_input(chat_id, text):
        if text in ["Отмена", "Назад"]:
            await state.update_message(chat_id, "Действие отменено." if text == "Отмена" else "Возврат в меню.", create_keyboard(chat_id, 'main' if text == "Отмена" else 'menu'))
            del state.pending_commands[chat_id]
            return
        try:
            amount = float(text.replace(',', '.'))
            if amount <= 0:
                raise ValueError
            await state.convert_manta(chat_id, amount)
            del state.pending_commands[chat_id]
        except ValueError:
            await state.update_message(chat_id, "Ошибка: введите положительное число.", create_keyboard(chat_id, 'converter'))

    async def handle_gas_calculator_input(chat_id, text, state_data):
        if text in ["Отмена", "Назад"]:
            if text == "Отмена" or state_data['step'] == 'gas_calculator_gas_input':
                await state.update_message(chat_id, "Действие отменено.", create_keyboard(chat_id, 'main'))
            else:
                state_data['step'] = 'gas_calculator_gas_input'
                await state.update_message(chat_id, "Введите цену газа в Gwei (например, 0.0015):", create_keyboard(chat_id, 'gas_calculator'))
            del state.pending_commands[chat_id] if text == "Отмена" else None
            return
        try:
            if state_data['step'] == 'gas_calculator_gas_input':
                gas_price = float(text.replace(',', '.'))
                if gas_price <= 0:
                    raise ValueError
                state_data['gas_price'] = gas_price
                state_data['step'] = 'gas_calculator_tx_count_input'
                await state.update_message(chat_id, "Введите количество транзакций (например, 100):", create_keyboard(chat_id, 'gas_calculator'))
            else:
                tx_count = int(text)
                if tx_count <= 0:
                    raise ValueError
                await state.calculate_gas_cost(chat_id, state_data['gas_price'], tx_count)
                del state.pending_commands[chat_id]
        except ValueError:
            await state.update_message(chat_id, f"Ошибка: введите {'положительное число' if state_data['step'] == 'gas_calculator_gas_input' else 'целое число'}.", create_keyboard(chat_id, 'gas_calculator'))

    async def handle_silent_hours_input(chat_id, text):
        if text in ["Отмена", "Назад"]:
            await state.update_message(chat_id, "Действие отменено." if text == "Отмена" else "Возврат в главное меню.", create_keyboard(chat_id, 'main'))
            del state.pending_commands[chat_id]
            return
        if text in ["Отключить Тихие Часы", "Включить Тихие Часы"]:
            await state.set_silent_hours(chat_id, text == "Включить Тихие Часы")
            del state.pending_commands[chat_id]
        else:
            await state.update_message(chat_id, "Тихие Часы фиксированы: 00:00–08:00. Выберите действие.", create_keyboard(chat_id, 'silent_hours'))

    await handlers.get(state_data['step'], lambda: state.update_message(chat_id, "Выберите действие.", create_keyboard(chat_id, 'main')))()
    try:
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete message: {e}")

async def monitor_gas_callback(gas_value):
    """Обратный вызов для мониторинга газа."""
    if state.is_first_run:
        for user_id, _ in ALLOWED_USERS:
            state.user_states[user_id]['last_measured_gas'] = gas_value
            state.user_states[user_id]['prev_level'] = gas_value
        state.is_first_run = False
        return
    for user_id, _ in ALLOWED_USERS:
        await asyncio.sleep(1)
        await state.get_manta_gas(user_id, force_base_message=False)