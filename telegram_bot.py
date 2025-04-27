import asyncio
import logging
import os
import json
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from decimal import Decimal
from datetime import datetime
import aiohttp
from aiohttp import web
from monitoring_scanner import Scanner
import asyncpg

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
    (501156257, "Сергей"),
]
ADMIN_ID = 501156257
INTERVAL = 60
CONFIRMATION_INTERVAL = 20
CONFIRMATION_COUNT = 3

# Функции для HTTP-сервера и периодических пингов
async def handle_ping(request):
    logger.debug("Received ping request")
    return web.Response(text="Bot is alive")

async def start_web_server():
    app = web.Application()
    app.add_routes([web.get('/ping', handle_ping)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 8080))  # Render задаёт PORT
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server started on port {port}")

async def periodic_ping():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://localhost:{os.getenv('PORT', 8080)}/ping") as response:
                    if response.status == 200:
                        logger.debug("Self-ping successful")
                    else:
                        logger.warning(f"Self-ping failed with status {response.status}")
        except Exception as e:
            logger.error(f"Error during self-ping: {str(e)}")
        await asyncio.sleep(600)  # 10 минут = 600 секунд

# Функции для работы с PostgreSQL
async def init_db():
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_levels (
            user_id BIGINT PRIMARY KEY,
            levels TEXT
        );
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id BIGINT PRIMARY KEY,
            stats TEXT
        );
    """)
    return conn

async def save_levels(user_id, levels):
    conn = await init_db()
    levels_str = json.dumps([str(level) for level in levels])
    await conn.execute(
        """
        INSERT INTO user_levels (user_id, levels)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET levels = $2
        """,
        user_id, levels_str
    )
    await conn.close()

async def load_levels(user_id):
    conn = await init_db()
    result = await conn.fetchrow(
        "SELECT levels FROM user_levels WHERE user_id = $1",
        user_id
    )
    await conn.close()
    if result:
        levels = json.loads(result['levels'])
        return [Decimal(level) for level in levels]
    return None

async def save_stats(user_id, stats):
    conn = await init_db()
    stats_str = json.dumps(stats)
    await conn.execute(
        """
        INSERT INTO user_stats (user_id, stats)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET stats = $2
        """,
        user_id, stats_str
    )
    await conn.close()

async def load_stats(user_id):
    conn = await init_db()
    result = await conn.fetchrow(
        "SELECT stats FROM user_stats WHERE user_id = $1",
        user_id
    )
    await conn.close()
    if result:
        return json.loads(result['stats'])
    return None

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
        self.l2_data_cooldown = 300
        self.fear_greed_cache = None
        self.fear_greed_time = None
        self.fear_greed_cooldown = 300
        self.user_stats = {}
        logger.info("BotState initialized")

    def init_user_state(self, user_id):
        if user_id not in self.user_states:
            self.user_states[user_id] = {
                'prev_level': None,
                'last_measured_gas': None,
                'current_levels': [],
                'active_level': None,
                'confirmation_state': {'count': 0, 'values': [], 'target_level': None, 'direction': None},
                'notified_levels': set()
            }
            asyncio.create_task(self.load_or_set_default_levels(user_id))

    def init_user_stats(self, user_id):
        if user_id not in self.user_stats:
            self.user_stats[user_id] = {}
        today = datetime.now().date().isoformat()
        default_stats = {
            "Проверить газ": 0, "Manta Price": 0, "Сравнение L2": 0,
            "Задать уровни": 0, "Уведомления": 0, "Админ": 0, "Страх и Жадность": 0
        }
        if today not in self.user_stats[user_id]:
            self.user_stats[user_id][today] = default_stats.copy()
        else:
            for key in default_stats:
                if key not in self.user_stats[user_id][today]:
                    self.user_stats[user_id][today][key] = 0
        asyncio.create_task(self.save_user_stats(user_id))

    async def check_access(self, message: types.Message):
        chat_id = message.chat.id
        logger.debug(f"Checking access for chat_id={chat_id}")
        if chat_id not in [user[0] for user in ALLOWED_USERS]:
            try:
                await self.bot.send_message(chat_id, "⛽ У вас нет доступа к этому боту.")
            except Exception as e:
                logger.warning(f"Cannot notify chat_id={chat_id}: {e}")
            logger.warning(f"Access denied for chat_id={chat_id}")
            return False
        self.init_user_state(chat_id)
        self.init_user_stats(chat_id)
        return True

    async def set_menu_button(self):
        try:
            await self.bot.set_chat_menu_button(menu_button=types.MenuButtonCommands())
            logger.info("Menu button set to 'commands'")
        except Exception as e:
            logger.error(f"Failed to set menu button: {e}")

    async def load_or_set_default_levels(self, user_id):
        try:
            levels = await load_levels(user_id)
            if levels is None:
                # Устанавливаем пустой список вместо уровней по умолчанию
                levels = []
                await save_levels(user_id, levels)
            self.user_states[user_id]['current_levels'] = levels
            self.user_states[user_id]['current_levels'].sort(reverse=True)
            logger.info(f"Loaded levels for user_id={user_id}: {levels}")
        except Exception as e:
            logger.error(f"Error loading levels for user_id={user_id}: {str(e)}, setting to empty list")
            self.user_states[user_id]['current_levels'] = []
            await save_levels(user_id, self.user_states[user_id]['current_levels'])

    async def save_levels(self, user_id, levels):
        levels.sort(reverse=True)
        try:
            await save_levels(user_id, levels)
            self.user_states[user_id]['current_levels'] = levels
            logger.debug(f"Saved levels for user_id={user_id}: {levels}")
        except Exception as e:
            logger.error(f"Error saving levels for user_id={user_id}: {str(e)}")

    async def load_user_stats(self, user_id):
        try:
            stats = await load_stats(user_id)
            if stats is None:
                stats = {}
            self.user_stats[user_id] = stats
            self.init_user_stats(user_id)
        except Exception as e:
            logger.error(f"Error loading stats for user_id={user_id}: {str(e)}")
            self.init_user_stats(user_id)

    async def save_user_stats(self, user_id):
        try:
            await save_stats(user_id, self.user_stats[user_id])
            logger.debug(f"Saved stats for user_id={user_id}")
        except Exception as e:
            logger.error(f"Error saving stats for user_id={user_id}: {str(e)}")

    async def update_message(self, chat_id, text, reply_markup=None):
        try:
            msg = await self.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=reply_markup)
            self.message_ids[chat_id] = msg.message_id
            logger.debug(f"Sent new message_id={msg.message_id} for chat_id={chat_id}")
        except Exception as e:
            logger.error(f"Error sending message to chat_id={chat_id}: {str(e)}")

    async def confirm_level_crossing(self, chat_id, initial_value, direction, target_level):
        state = self.user_states[chat_id]['confirmation_state']
        state['count'] = 1
        state['values'] = [initial_value]
        state['target_level'] = target_level
        state['direction'] = direction
        logger.info(f"Starting confirmation for chat_id={chat_id}: {initial_value:.5f} Gwei, direction: {direction}, target: {target_level:.5f}")

        for i in range(CONFIRMATION_COUNT - 1):
            await asyncio.sleep(CONFIRMATION_INTERVAL)
            current_slow = await self.scanner.get_current_gas()
            if current_slow is None:
                logger.error(f"Failed to get gas on attempt {i + 2} for chat_id={chat_id}")
                state['count'] = 0
                state['values'] = []
                return
            state['count'] += 1
            state['values'].append(current_slow)
            logger.debug(f"Attempt {i + 2} for chat_id={chat_id}: {current_slow:.5f} Gwei")

        values = state['values']
        is_confirmed = False
        if direction == 'down' and all(v <= target_level for v in values):
            is_confirmed = True
        elif direction == 'up' and all(v >= target_level for v in values):
            is_confirmed = True

        if is_confirmed and target_level not in self.user_states[chat_id]['notified_levels']:
            last_measured = self.user_states[chat_id]['last_measured_gas']
            notification_message = (
                f"<pre>{'🟩' if direction == 'down' else '🟥'} ◆ ГАЗ {'УМЕНЬШИЛСЯ' if direction == 'down' else 'УВЕЛИЧИЛСЯ'} до: {values[-1]:.5f} Gwei\n"
                f"Уровень: {target_level:.5f} Gwei подтверждён</pre>"
            )
            await self.update_message(chat_id, notification_message, create_main_keyboard(chat_id))
            self.user_states[chat_id]['notified_levels'].add(target_level)
            self.user_states[chat_id]['active_level'] = target_level
            self.user_states[chat_id]['prev_level'] = last_measured
            logger.info(f"Level {target_level:.5f} confirmed for chat_id={chat_id}, notified")

        state['count'] = 0
        state['values'] = []
        state['target_level'] = None
        state['direction'] = None

    async def get_manta_gas(self, chat_id, force_base_message=False):
        try:
            current_slow = await self.scanner.get_current_gas()
            if current_slow is None:
                await self.update_message(chat_id, "<b>⚠️ Не удалось подключиться к Manta Pacific</b>", create_main_keyboard(chat_id))
                return

            logger.info(f"Gas for chat_id={chat_id}: Slow={current_slow:.6f}")
            base_message = f"<pre>⛽️ Manta Pacific Gas\n◆ <b>ТЕКУЩИЙ ГАЗ</b>:   {current_slow:.5f} Gwei</pre>"

            self.user_states[chat_id]['last_measured_gas'] = current_slow
            prev_level = self.user_states[chat_id]['prev_level']
            levels = self.user_states[chat_id]['current_levels']
            confirmation_state = self.user_states[chat_id]['confirmation_state']

            if not levels:
                await self.load_or_set_default_levels(chat_id)
                levels = self.user_states[chat_id]['current_levels']

            if not levels:
                if force_base_message:
                    await self.update_message(chat_id, base_message + "\n\nУровни не заданы. Используйте 'Задать уровни'.", create_main_keyboard(chat_id))
                else:
                    logger.info(f"No levels set for chat_id={chat_id}, skipping notification check.")
                self.user_states[chat_id]['prev_level'] = current_slow
                return

            if prev_level is None or force_base_message:
                await self.update_message(chat_id, base_message, create_main_keyboard(chat_id))
                self.user_states[chat_id]['prev_level'] = current_slow
            elif confirmation_state['count'] == 0:
                sorted_levels = sorted(levels)
                for level in sorted_levels:
                    if prev_level < level <= current_slow:
                        logger.info(f"Detected upward crossing for chat_id={chat_id}: {level:.5f}")
                        asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'up', level))
                    elif prev_level > level >= current_slow:
                        logger.info(f"Detected downward crossing for chat_id={chat_id}: {level:.5f}")
                        asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'down', level))

            self.user_states[chat_id]['active_level'] = min(levels, key=lambda x: abs(x - current_slow))

        except Exception as e:
            logger.error(f"Error for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def fetch_l2_data(self):
        l2_tokens = {
            "MANTA": "manta-network",
            "Optimism": "optimism",
            "Arbitrum": "arbitrum",
            "Starknet": "starknet",
            "ZKsync": "zksync",
            "Scroll": "scroll",
            "Mantle": "mantle",
            "Taiko": "taiko"
        }
        token_data = {}

        current_time = datetime.now()
        if self.l2_data_time and (current_time - self.l2_data_time).total_seconds() < self.l2_data_cooldown and self.l2_data_cache:
            return self.l2_data_cache

        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": ",".join(l2_tokens.values()),
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h,7d,30d"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"CoinGecko API error: {response.status}")
                    return None
                data = await response.json()

                token_map = {coin["id"]: coin for coin in data}
                for name, token_id in l2_tokens.items():
                    coin = token_map.get(token_id)
                    if coin:
                        price = coin.get("current_price", "Н/Д")
                        price_change_24h = coin.get("price_change_percentage_24h_in_currency", "Н/Д")
                        price_change_7d = coin.get("price_change_percentage_7d_in_currency", "Н/Д")
                        price_change_30d = coin.get("price_change_percentage_30d_in_currency", "Н/Д")
                        price_change_all = ((price - coin.get("ath", price)) / coin.get("ath", price) * 100) if price != "Н/Д" and coin.get("ath") else "Н/Д"
                        ath_price = coin.get("ath", "Н/Д")
                        ath_date = coin.get("ath_date", "Н/Д")
                        if ath_date != "Н/Д":
                            ath_date = datetime.strptime(ath_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                        atl_price = coin.get("atl", "Н/Д")
                        atl_date = coin.get("atl_date", "Н/Д")
                        if atl_date != "Н/Д":
                            atl_date = datetime.strptime(atl_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                        token_data[name] = {
                            "price": price,
                            "24h": price_change_24h if price_change_24h is not None else "Н/Д",
                            "7d": price_change_7d if price_change_7d is not None else "Н/Д",
                            "30d": price_change_30d if price_change_30d is not None else "Н/Д",
                            "all": price_change_all,
                            "ath_price": ath_price,
                            "ath_date": ath_date,
                            "atl_price": atl_price,
                            "atl_date": atl_date
                        }
                    else:
                        token_data[name] = {
                            "price": "Н/Д", "24h": "Н/Д", "7d": "Н/Д", "30d": "Н/Д", "all": "Н/Д",
                            "ath_price": "Н/Д", "ath_date": "Н/Д", "atl_price": "Н/Д", "atl_date": "Н/Д"
                        }

        self.l2_data_cache = token_data
        self.l2_data_time = current_time
        return token_data

    async def fetch_fear_greed(self):
        current_time = datetime.now()
        if self.fear_greed_time and (current_time - self.fear_greed_time).total_seconds() < self.fear_greed_cooldown and self.fear_greed_cache:
            return self.fear_greed_cache

        url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical"
        headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
        params = {"limit": 30}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status != 200:
                    logger.error(f"CMC Fear & Greed API error: {response.status}")
                    return None
                data = await response.json()
                if "data" not in data or not data["data"]:
                    logger.error("No data returned from Fear & Greed API")
                    return None

                fg_data = data["data"]
                current = fg_data[0]
                current_value = int(current["value"])
                current_category = current["value_classification"]

                yesterday = fg_data[1]
                yesterday_value = int(yesterday["value"])
                yesterday_category = yesterday["value_classification"]

                week_ago = fg_data[7] if len(fg_data) > 7 else fg_data[-1]
                week_ago_value = int(week_ago["value"])
                week_ago_category = week_ago["value_classification"]

                month_ago = fg_data[-1]
                month_ago_value = int(month_ago["value"])
                month_ago_category = month_ago["value_classification"]

                year_data = fg_data[:365] if len(fg_data) > 365 else fg_data
                year_values = [(int(d["value"]), d["timestamp"], d["value_classification"]) for d in year_data]
                max_year = max(year_values, key=lambda x: x[0])
                min_year = min(year_values, key=lambda x: x[0])
                max_year_value, max_year_date, max_year_category = max_year
                min_year_value, min_year_date, min_year_category = min_year

                def parse_timestamp(ts):
                    try:
                        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d.%m.%Y")
                    except ValueError:
                        return datetime.fromtimestamp(int(ts)).strftime("%d.%m.%Y")

                max_year_date = parse_timestamp(max_year_date)
                min_year_date = parse_timestamp(min_year_date)

                fear_greed_data = {
                    "current": {"value": current_value, "category": current_category},
                    "yesterday": {"value": yesterday_value, "category": yesterday_category},
                    "week_ago": {"value": week_ago_value, "category": week_ago_category},
                    "month_ago": {"value": month_ago_value, "category": month_ago_category},
                    "year_max": {"value": max_year_value, "date": max_year_date, "category": max_year_category},
                    "year_min": {"value": min_year_value, "date": min_year_date, "category": min_year_category}
                }

                self.fear_greed_cache = fear_greed_data
                self.fear_greed_time = current_time
                return fear_greed_data

    async def get_manta_price(self, chat_id):
        try:
            token_data = await self.fetch_l2_data()
            if not token_data:
                await self.update_message(chat_id, "⚠️ Не удалось получить данные от CoinGecko.", create_main_keyboard(chat_id))
                return

            manta_data = token_data["MANTA"]
            price = manta_data["price"]
            price_change_24h = manta_data["24h"]
            price_change_7d = manta_data["7d"]
            price_change_30d = manta_data["30d"]
            price_change_all = manta_data["all"]
            ath_price = manta_data["ath_price"]
            ath_date = manta_data["ath_date"]
            atl_price = manta_data["atl_price"]
            atl_date = manta_data["atl_date"]

            spot_volume = await self.scanner.get_manta_spot_volume()
            futures_volume = await self.scanner.get_manta_futures_volume()
            spot_volume_m = round(spot_volume / Decimal('1000000')) if spot_volume else 0
            futures_volume_m = round(futures_volume / Decimal('1000000')) if futures_volume else 0
            spot_volume_str = f"{spot_volume_m}M$" if spot_volume else "Н/Д"
            futures_volume_str = f"{futures_volume_m}M$" if futures_volume else "Н/Д"

            message = (
                f"<pre>"
                f"🦎 Данные с CoinGecko:\n"
                f"◆ Manta/USDT: ${float(price):.3f}\n\n"
                f"◆ ИЗМЕНЕНИЕ:\n"
                f"◆ 24 ЧАСА:     {float(price_change_24h):>6.2f}%\n"
                f"◆ 7 ДНЕЙ:      {float(price_change_7d):>6.2f}%\n"
                f"◆ МЕСЯЦ:       {float(price_change_30d):>6.2f}%\n"
                f"◆ ВСЕ ВРЕМЯ:   {float(price_change_all):>6.2f}%\n\n"
                f"◆ Binance Volume Trade 24ч:\n"
                f"◆ (Futures):   {futures_volume_str}\n"
                f"◆ (Spot):      {spot_volume_str}\n\n"
                f"◆ ${float(ath_price):.2f} ({ath_date})\n"
                f"◆ ${float(atl_price):.2f} ({atl_date})"
                f"</pre>"
            )
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))

        except Exception as e:
            logger.error(f"Error fetching price for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def get_l2_comparison(self, chat_id):
        try:
            token_data = await self.fetch_l2_data()
            if not token_data:
                await self.update_message(chat_id, "⚠️ Не удалось получить данные от CoinGecko.", create_main_keyboard(chat_id))
                return

            message = (
                f"<pre>"
                f"🦎 Данные с CoinGecko:\n"
                f"◆ Сравнение L2 токенов (24 часа):\n"
            )
            sorted_by_24h = sorted(
                token_data.items(),
                key=lambda x: float(x[1]["24h"]) if x[1]["24h"] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_by_24h:
                price = data["price"]
                price_24h = data["24h"]
                price_str = f"${float(price):.3f}" if price != "Н/Д" else "Н/Д"
                change_str = f"{float(price_24h):+.2f}%" if price_24h != "Н/Д" else "Н/Д"
                message += f"◆ {name:<10}: {price_str:>8} ({change_str:>7})\n"

            message += "</pre>"
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))

        except Exception as e:
            logger.error(f"Error fetching L2 comparison for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def get_fear_greed(self, chat_id):
        try:
            fear_greed_data = await self.fetch_fear_greed()
            if not fear_greed_data:
                await self.update_message(chat_id, "⚠️ Не удалось получить данные от CoinMarketCap.", create_main_keyboard(chat_id))
                return

            current = fear_greed_data["current"]
            yesterday = fear_greed_data["yesterday"]
            week_ago = fear_greed_data["week_ago"]
            month_ago = fear_greed_data["month_ago"]
            year_max = fear_greed_data["year_max"]
            year_min = fear_greed_data["year_min"]

            message = (
                f"<pre>"
                f"😨 Fear & Greed Index (CMC):\n"
                f"◆ Текущий:      {current['value']:>2} ({current['category']})\n"
                f"◆ Вчера:        {yesterday['value']:>2} ({yesterday['category']})\n"
                f"◆ Неделю назад: {week_ago['value']:>2} ({week_ago['category']})\n"
                f"◆ Месяц назад:  {month_ago['value']:>2} ({month_ago['category']})\n"
                f"◆ Макс за год:  {year_max['value']:>2} ({year_max['category']}, {year_max['date']})\n"
                f"◆ Мин за год:   {year_min['value']:>2} ({year_min['category']}, {year_min['date']})\n"
                f"</pre>"
            )
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))

        except Exception as e:
            logger.error(f"Error fetching Fear & Greed for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

def create_main_keyboard(chat_id):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    buttons = [
        types.KeyboardButton("⛽ Проверить газ"),
        types.KeyboardButton("🦎 Manta Price"),
        types.KeyboardButton("📊 Сравнение L2"),
        types.KeyboardButton("📈 Задать уровни"),
        types.KeyboardButton("🔔 Уведомления"),
        types.KeyboardButton("😨 Страх и Жадность")
    ]
    if chat_id == ADMIN_ID:
        buttons.append(types.KeyboardButton("🛠 Админ"))
    keyboard.add(*buttons)
    return keyboard

async def monitor_gas_callback(chat_id, gas_price):
    state = state  # Assuming global state; adjust if passed differently
    await state.get_manta_gas(chat_id)

async def main():
    logger.info("Starting bot initialization")
    scanner = Scanner()
    global state
    state = BotState(scanner)
    await state.set_menu_button()
    for user_id, _ in ALLOWED_USERS:
        state.init_user_state(user_id)
        state.init_user_stats(user_id)
        await state.load_or_set_default_levels(user_id)
        await state.load_user_stats(user_id)

    # Регистрация обработчиков команд
    @state.dp.message(Command("start"))
    async def cmd_start(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            await state.update_message(chat_id, "👋 Добро пожаловать! Выберите действие:", create_main_keyboard(chat_id))

    @state.dp.message(lambda message: message.text == "⛽ Проверить газ")
    async def check_gas(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            today = datetime.now().date().isoformat()
            state.user_stats[chat_id][today]["Проверить газ"] += 1
            await state.save_user_stats(chat_id)
            await state.get_manta_gas(chat_id, force_base_message=True)

    @state.dp.message(lambda message: message.text == "🦎 Manta Price")
    async def manta_price(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            today = datetime.now().date().isoformat()
            state.user_stats[chat_id][today]["Manta Price"] += 1
            await state.save_user_stats(chat_id)
            await state.get_manta_price(chat_id)

    @state.dp.message(lambda message: message.text == "📊 Сравнение L2")
    async def l2_comparison(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            today = datetime.now().date().isoformat()
            state.user_stats[chat_id][today]["Сравнение L2"] += 1
            await state.save_user_stats(chat_id)
            await state.get_l2_comparison(chat_id)

    @state.dp.message(lambda message: message.text == "😨 Страх и Жадность")
    async def fear_greed(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            today = datetime.now().date().isoformat()
            state.user_stats[chat_id][today]["Страх и Жадность"] += 1
            await state.save_user_stats(chat_id)
            await state.get_fear_greed(chat_id)

    @state.dp.message(lambda message: message.text == "📈 Задать уровни")
    async def set_levels(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            today = datetime.now().date().isoformat()
            state.user_stats[chat_id][today]["Задать уровни"] += 1
            await state.save_user_stats(chat_id)
            state.pending_commands[chat_id] = "set_levels"
            levels = state.user_states[chat_id]['current_levels']
            levels_str = ", ".join([f"{float(level):.5f}" for level in levels]) if levels else "не заданы"
            await state.update_message(chat_id, f"📈 Текущие уровни: {levels_str}\n\nВведите новые уровни (через запятую, например: 10, 20, 30):")

    @state.dp.message(lambda message: message.text == "🔔 Уведомления")
    async def toggle_notifications(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            today = datetime.now().date().isoformat()
            state.user_stats[chat_id][today]["Уведомления"] += 1
            await state.save_user_stats(chat_id)
            levels = state.user_states[chat_id]['current_levels']
            if not levels:
                await state.update_message(chat_id, "🔔 Уведомления неактивны: уровни не заданы. Используйте 'Задать уровни'.", create_main_keyboard(chat_id))
            else:
                state.user_states[chat_id]['notified_levels'].clear()
                await state.update_message(chat_id, "🔔 Уведомления сброшены. Вы получите новые уведомления при пересечении уровней.", create_main_keyboard(chat_id))

    @state.dp.message(lambda message: message.text == "🛠 Админ" and message.chat.id == ADMIN_ID)
    async def admin_panel(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            today = datetime.now().date().isoformat()
            state.user_stats[chat_id][today]["Админ"] += 1
            await state.save_user_stats(chat_id)
            stats_message = "<b>📊 Статистика использования:</b>\n"
            for user_id, stats in state.user_stats.items():
                user_name = next((name for uid, name in ALLOWED_USERS if uid == user_id), "Unknown")
                stats_message += f"\n<b>{user_name} (ID: {user_id})</b>:\n"
                for date, commands in stats.items():
                    stats_message += f"<b>{date}</b>:\n"
                    for cmd, count in commands.items():
                        stats_message += f"  {cmd}: {count}\n"
            await state.update_message(chat_id, stats_message, create_main_keyboard(chat_id))

    @state.dp.message()
    async def handle_message(message: types.Message):
        if await state.check_access(message):
            chat_id = message.chat.id
            if chat_id in state.pending_commands:
                command = state.pending_commands[chat_id]
                if command == "set_levels":
                    try:
                        levels = [Decimal(level.strip()) for level in message.text.split(",") if level.strip()]
                        if not levels:
                            await state.update_message(chat_id, "⚠️ Уровни не указаны. Введите уровни через запятую (например: 10, 20, 30).")
                            return
                        await state.save_levels(chat_id, levels)
                        levels_str = ", ".join([f"{float(level):.5f}" for level in levels])
                        state.user_states[chat_id]['notified_levels'].clear()
                        await state.update_message(chat_id, f"📈 Уровни успешно заданы: {levels_str}", create_main_keyboard(chat_id))
                        del state.pending_commands[chat_id]
                    except Exception as e:
                        await state.update_message(chat_id, f"⚠️ Ошибка: Неверный формат уровней. Введите числа через запятую (например: 10, 20, 30).")
                        logger.error(f"Error setting levels for chat_id={chat_id}: {str(e)}")
                else:
                    del state.pending_commands[chat_id]
                    await state.update_message(chat_id, "⚠️ Неизвестная команда. Выберите действие:", create_main_keyboard(chat_id))
            else:
                await state.update_message(chat_id, "👋 Выберите действие:", create_main_keyboard(chat_id))

    try:
        await asyncio.gather(
            state.dp.start_polling(state.bot),
            scanner.monitor_gas(INTERVAL, monitor_gas_callback),
            start_web_server(),  # Запуск HTTP-сервера
            periodic_ping(),     # Запуск периодических пингов
            return_exceptions=True
        )
    except Exception as e:
        logger.error(f"Main loop error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())