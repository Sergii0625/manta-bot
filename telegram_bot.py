import asyncio
import logging
import os
import json
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from decimal import Decimal
from datetime import datetime, time
import aiohttp
from monitoring_scanner import Scanner
import asyncpg
import pytz

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
    (5070159060, "Васек"),
    (1182677771, "Толик"),
    (6322048522, "Кумец"),
    (7009557842, "Лайф")
]
ADMIN_ID = 501156257
INTERVAL = 60
CONFIRMATION_INTERVAL = 20
CONFIRMATION_COUNT = 3
RESTART_TIMES = ["21:00"]

# Функции для работы с PostgreSQL
async def init_db():
    try:
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
            CREATE TABLE IF NOT EXISTS silent_hours (
                user_id BIGINT PRIMARY KEY,
                start_time TIME,
                end_time TIME
            );
        """)
        logger.info("Database tables initialized successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

async def check_db_connection():
    """Проверка целостности подключения к базе данных"""
    try:
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        await conn.execute("SELECT 1")
        await conn.close()
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False

async def save_silent_hours(user_id, start_time, end_time):
    conn = await init_db()
    await conn.execute(
        """
        INSERT INTO silent_hours (user_id, start_time, end_time)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id) DO UPDATE SET start_time = $2, end_time = $3
        """,
        user_id, start_time, end_time
    )
    await conn.close()
    logger.debug(f"Saved silent hours for user_id={user_id}: {start_time}-{end_time}")

async def load_silent_hours(user_id):
    conn = await init_db()
    result = await conn.fetchrow(
        "SELECT start_time, end_time FROM silent_hours WHERE user_id = $1",
        user_id
    )
    await conn.close()
    if result:
        logger.debug(f"Loaded silent hours for user_id={user_id}: {result['start_time']}-{result['end_time']}")
        return result['start_time'], result['end_time']
    logger.debug(f"No silent hours found for user_id={user_id}")
    return None, None

async def save_levels(user_id, levels):
    conn = await init_db()
    levels_str = json.dumps([str(level) for level in levels])
    try:
        await conn.execute(
            """
            INSERT INTO user_levels (user_id, levels)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET levels = $2
            """,
            user_id, levels_str
        )
        logger.debug(f"Successfully saved levels to DB for user_id={user_id}: {levels}")
    except Exception as e:
        logger.error(f"Failed to save levels to DB for user_id={user_id}: {str(e)}")
    finally:
        await conn.close()

async def load_levels(user_id):
    try:
        conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
        result = await conn.fetchrow(
            "SELECT levels FROM user_levels WHERE user_id = $1",
            user_id
        )
        await conn.close()
        if result and result['levels']:
            levels = json.loads(result['levels'])
            if not levels:
                logger.warning(f"Empty levels list in DB for user_id={user_id}")
                return None
            loaded_levels = [Decimal(level) for level in levels]
            logger.debug(f"Loaded levels from DB for user_id={user_id}: {loaded_levels}")
            return loaded_levels
        logger.warning(f"No levels found in DB for user_id={user_id}")
        return None
    except Exception as e:
        logger.error(f"Error loading levels from DB for user_id={user_id}: {str(e)}")
        return None

async def save_stats(user_id, stats):
    conn = await init_db()
    stats_str = json.dumps({k: {dk: int(dv) for dk, dv in v.items()} for k, v in stats.items()})
    await conn.execute(
        """
        INSERT INTO user_stats (user_id, stats)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET stats = $2
        """,
        user_id, stats_str
    )
    await conn.close()
    logger.debug(f"Saved stats for user_id={user_id}")

async def load_stats(user_id):
    conn = await init_db()
    result = await conn.fetchrow(
        "SELECT stats FROM user_stats WHERE user_id = $1",
        user_id
    )
    await conn.close()
    if result and result['stats']:
        stats = json.loads(result['stats'])
        logger.debug(f"Loaded stats for user_id={user_id}")
        return stats
    logger.debug(f"No stats found for user_id={user_id}")
    return None

def is_silent_hour(user_id, now_kyiv):
    start_time, end_time = state.user_states[user_id].get('silent_hours', (None, None))
    if start_time is None or end_time is None:
        return False
    now_time = now_kyiv.time()
    if start_time <= end_time:
        return start_time <= now_time <= end_time
    else:
        return now_time >= start_time or now_time <= end_time

def create_main_keyboard(chat_id):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        types.KeyboardButton("⛽ Газ"),
        types.KeyboardButton("💰 Manta Price"),
        types.KeyboardButton("🔍 Сравнение L2"),
        types.KeyboardButton("📊 Задать Уровни"),
        types.KeyboardButton("🔔 Уведомления"),
        types.KeyboardButton("😱 Страх и Жадность"),
        types.KeyboardButton("🤫 Тихие Часы"),
        types.KeyboardButton("🔄 Manta Конвертер"),
        types.KeyboardButton("🧮 Газ Калькулятор")
    ]
    if chat_id == ADMIN_ID:
        buttons.append(types.KeyboardButton("⚙️ Админ"))
    keyboard.add(*buttons)
    return keyboard

def create_menu_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        types.KeyboardButton("🔙 Вернуться в меню"),
        types.KeyboardButton("🔄 Обновить")
    ]
    keyboard.add(*buttons)
    return keyboard

def create_levels_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        types.KeyboardButton("📝 Задать свои уровни"),
        types.KeyboardButton("🔄 Сбросить уровни"),
        types.KeyboardButton("🔙 Вернуться в меню")
    ]
    keyboard.add(*buttons)
    return keyboard

def create_notification_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        types.KeyboardButton("🔔 Включить уведомления"),
        types.KeyboardButton("🔕 Выключить уведомления"),
        types.KeyboardButton("🔙 Вернуться в меню")
    ]
    keyboard.add(*buttons)
    return keyboard

def create_admin_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        types.KeyboardButton("📊 Статистика"),
        types.KeyboardButton("🔄 Перезапуск"),
        types.KeyboardButton("🔙 Вернуться в меню")
    ]
    keyboard.add(*buttons)
    return keyboard

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
        asyncio.create_task(self.check_db_on_start())  # Проверка базы данных при старте

    async def check_db_on_start(self):
        """Проверка подключения к базе данных при старте бота"""
        if not await check_db_connection():
            logger.error("Critical: Database is not accessible at bot startup")
            for user_id, _ in ALLOWED_USERS:
                self.user_states[user_id] = {
                    'prev_level': None,
                    'last_measured_gas': None,
                    'current_levels': [],  # Инициализация пустым списком
                    'active_level': None,
                    'confirmation_states': {},
                    'notified_levels': set(),
                    'silent_hours': (None, None)
                }
                await self.load_or_set_default_levels(user_id)  # Установка уровней по умолчанию
        else:
            logger.info("Database is accessible at bot startup")

    async def init_user_state(self, user_id):
        if user_id not in self.user_states:
            self.user_states[user_id] = {
                'prev_level': None,
                'last_measured_gas': None,
                'current_levels': [],
                'active_level': None,
                'confirmation_states': {},
                'notified_levels': set(),
                'silent_hours': (None, None)
            }
            await self.load_or_set_default_levels(user_id)
            if not self.user_states[user_id]['current_levels']:
                logger.warning(f"current_levels is empty for user_id={user_id} after load_or_set_default_levels")
                await self.load_or_set_default_levels(user_id)  # Повторная попытка загрузки уровней
            start_time, end_time = await load_silent_hours(user_id)
            self.user_states[user_id]['silent_hours'] = (start_time, end_time)
            logger.debug(f"Initialized user_state for user_id={user_id}, current_levels={self.user_states[user_id]['current_levels']}, silent_hours={self.user_states[user_id]['silent_hours']}")

    def init_user_stats(self, user_id):
        if user_id not in self.user_stats:
            self.user_stats[user_id] = {}
        today = datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()
        default_stats = {
            "Газ": 0, "Manta Price": 0, "Сравнение L2": 0,
            "Задать Уровни": 0, "Уведомления": 0, "Админ": 0, "Страх и Жадность": 0,
            "Тихие Часы": 0, "Manta Конвертер": 0, "Газ Калькулятор": 0
        }
        if today not in self.user_stats[user_id]:
            self.user_stats[user_id][today] = default_stats.copy()
        else:
            for key in default_stats:
                if key not in self.user_stats[user_id][today]:
                    self.user_stats[user_id][today][key] = 0
        asyncio.create_task(self.save_user_stats(user_id))
        logger.debug(f"Initialized user_stats for user_id={user_id}")

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
        await self.init_user_state(chat_id)
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
            if levels is None or not levels:
                logger.warning(f"No levels or empty levels returned for user_id={user_id}, setting default levels")
                levels = [
                    Decimal('0.010000'), Decimal('0.009500'), Decimal('0.009000'), Decimal('0.008500'),
                    Decimal('0.008000'), Decimal('0.007500'), Decimal('0.007000'), Decimal('0.006500'),
                    Decimal('0.006000'), Decimal('0.005500'), Decimal('0.005000'), Decimal('0.004500'),
                    Decimal('0.004000'), Decimal('0.003500'), Decimal('0.003000'), Decimal('0.002500'),
                    Decimal('0.002000'), Decimal('0.001500'), Decimal('0.001000'), Decimal('0.000900'),
                    Decimal('0.000800'), Decimal('0.000700'), Decimal('0.000600'), Decimal('0.000500'),
                    Decimal('0.000400'), Decimal('0.000300'), Decimal('0.000200'), Decimal('0.000100'),
                    Decimal('0.000050')
                ]
                logger.info(f"Attempting to save default levels for user_id={user_id}: {levels}")
                await save_levels(user_id, levels)
            self.user_states[user_id]['current_levels'] = levels
            self.user_states[user_id]['current_levels'].sort(reverse=True)
            logger.info(f"Loaded levels for user_id={user_id}: {self.user_states[user_id]['current_levels']}")
        except Exception as e:
            logger.error(f"Error loading or setting default levels for user_id={user_id}: {str(e)}, setting to default levels")
            levels = [
                Decimal('0.010000'), Decimal('0.009500'), Decimal('0.009000'), Decimal('0.008500'),
                Decimal('0.008000'), Decimal('0.007500'), Decimal('0.007000'), Decimal('0.006500'),
                Decimal('0.006000'), Decimal('0.005500'), Decimal('0.005000'), Decimal('0.004500'),
                Decimal('0.004000'), Decimal('0.003500'), Decimal('0.003000'), Decimal('0.002500'),
                Decimal('0.002000'), Decimal('0.001500'), Decimal('0.001000'), Decimal('0.000900'),
                Decimal('0.000800'), Decimal('0.000700'), Decimal('0.000600'), Decimal('0.000500'),
                Decimal('0.000400'), Decimal('0.000300'), Decimal('0.000200'), Decimal('0.000100'),
                Decimal('0.000050')
            ]
            self.user_states[user_id]['current_levels'] = levels
            logger.info(f"Set default levels in memory for user_id={user_id}: {levels}")
            try:
                await save_levels(user_id, levels)
                logger.info(f"Successfully saved default levels to DB for user_id={user_id}")
            except Exception as db_e:
                logger.error(f"Failed to save default levels to DB for user_id={user_id}: {str(db_e)}")

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
            logger.debug(f"Loaded user stats for user_id={user_id}")
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
            if chat_id in self.message_ids:
                try:
                    await self.bot.edit_message_text(
                        text=text,
                        chat_id=chat_id,
                        message_id=self.message_ids[chat_id],
                        parse_mode="HTML",
                        reply_markup=reply_markup
                    )
                    logger.debug(f"Edited message_id={self.message_ids[chat_id]} for chat_id={chat_id}")
                    return
                except Exception as e:
                    if "message is not modified" in str(e):
                        logger.debug(f"Message not modified for chat_id={chat_id}")
                        return
                    logger.warning(f"Failed to edit message for chat_id={chat_id}: {e}")
            msg = await self.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=reply_markup)
            self.message_ids[chat_id] = msg.message_id
            logger.debug(f"Sent new message_id={msg.message_id} for chat_id={chat_id}")
        except Exception as e:
            logger.error(f"Failed to send message to chat_id={chat_id}: {e}")
            raise

    async def reset_notified_levels(self, chat_id):
        self.user_states[chat_id]['notified_levels'].clear()
        logger.info(f"Cleared notified levels for chat_id={chat_id}")

    async def confirm_level_crossing(self, chat_id, initial_value, direction, target_level):
        kyiv_tz = pytz.timezone('Europe/Kyiv')
        now_kyiv = datetime.now(kyiv_tz)
        if is_silent_hour(chat_id, now_kyiv):
            logger.info(f"Silent hours active for chat_id={chat_id}, skipping notification for level={target_level:.6f}")
            return

        if target_level not in self.user_states[chat_id]['confirmation_states']:
            self.user_states[chat_id]['confirmation_states'][target_level] = {
                'count': 0, 'values': [], 'target_level': target_level, 'direction': direction
            }
        state = self.user_states[chat_id]['confirmation_states'][target_level]
        state['count'] = 1
        state['values'] = [initial_value]
        logger.info(f"Starting confirmation for chat_id={chat_id}: {initial_value:.6f} Gwei, direction: {direction}, target: {target_level:.6f}")

        for i in range(CONFIRMATION_COUNT - 1):
            await asyncio.sleep(CONFIRMATION_INTERVAL)
            current_slow = await self.scanner.get_current_gas()
            if current_slow is None:
                logger.error(f"Failed to get gas on attempt {i + 2} for chat_id={chat_id}")
                state['count'] = 0
                state['values'] = []
                del self.user_states[chat_id]['confirmation_states'][target_level]
                return
            state['count'] += 1
            state['values'].append(current_slow)
            logger.debug(f"Attempt {i + 2} for chat_id={chat_id}: {current_slow:.6f} Gwei")

        values = state['values']
        is_confirmed = False
        if direction == 'down' and all(v <= target_level for v in values):
            is_confirmed = True
        elif direction == 'up' and all(v >= target_level for v in values):
            is_confirmed = True

        if is_confirmed and target_level not in self.user_states[chat_id]['notified_levels']:
            logger.info(f"Confirmation successful for chat_id={chat_id}, target={target_level:.6f}, values={values}")
            last_measured = self.user_states[chat_id]['last_measured_gas']
            notification_message = (
                f"<pre>{'🟩' if direction == 'down' else '🟥'} ◆ ГАЗ {'УМЕНЬШИЛСЯ' if direction == 'down' else 'УВЕЛИЧИЛСЯ'} до: {values[-1]:.6f} Gwei\n"
                f"Уровень: {target_level:.6f} Gwei подтверждён</pre>"
            )
            await self.update_message(chat_id, notification_message, create_main_keyboard(chat_id))
            self.user_states[chat_id]['notified_levels'].add(target_level)
            self.user_states[chat_id]['active_level'] = target_level
            self.user_states[chat_id]['prev_level'] = last_measured
            logger.info(f"Level {target_level:.6f} confirmed for chat_id={chat_id}, notified")
        else:
            logger.info(f"Confirmation failed or already notified for chat_id={chat_id}, target={target_level:.6f}, is_confirmed={is_confirmed}, notified={target_level in self.user_states[chat_id]['notified_levels']}")

        state['count'] = 0
        state['values'] = []
        del self.user_states[chat_id]['confirmation_states'][target_level]

    async def get_manta_gas(self, chat_id, force_base_message=False):
        try:
            current_slow = await self.scanner.get_current_gas()
            if current_slow is None:
                await self.update_message(chat_id, "<b>⚠️ Не удалось подключиться к Manta Pacific</b>", create_main_keyboard(chat_id))
                return

            logger.info(f"Gas for chat_id={chat_id}: Slow={current_slow:.6f}")
            gas_str = f"{current_slow:.6f}"
            decimal_part = gas_str.split('.')[1] if '.' in gas_str else ''
            leading_zeros = 0
            for char in decimal_part:
                if char == '0':
                    leading_zeros += 1
                else:
                    break
            zeros_text = f"({leading_zeros})"
            base_message = f"<pre>⛽️ Manta Pacific Gas\n◆ <b>ТЕКУЩИЙ ГАЗ</b>:   {current_slow:.6f} Gwei  {zeros_text}</pre>"

            self.user_states[chat_id]['last_measured_gas'] = current_slow
            prev_level = self.user_states[chat_id]['prev_level']
            levels = self.user_states[chat_id]['current_levels']

            logger.debug(f"Checking levels for chat_id={chat_id}: current_levels={levels}, prev_level={prev_level}")

            if not levels:
                await self.load_or_set_default_levels(chat_id)
                levels = self.user_states[chat_id]['current_levels']
                logger.debug(f"Reloaded levels for chat_id={chat_id}: {levels}")

            if not levels:
                if force_base_message:
                    await self.update_message(chat_id, base_message + "\n\nУровни не заданы. Используйте 'Задать Уровни'.", create_main_keyboard(chat_id))
                else:
                    logger.info(f"No levels set for chat_id={chat_id}, skipping notification check.")
                self.user_states[chat_id]['prev_level'] = current_slow
                return

            if prev_level is None or force_base_message:
                await self.update_message(chat_id, base_message, create_main_keyboard(chat_id))
                self.user_states[chat_id]['prev_level'] = current_slow
            else:
                kyiv_tz = pytz.timezone('Europe/Kyiv')
                now_kyiv = datetime.now(kyiv_tz)
                if not is_silent_hour(chat_id, now_kyiv):
                    sorted_levels = sorted(levels)
                    closest_level = min(sorted_levels, key=lambda x: abs(x - current_slow))
                    if prev_level < closest_level <= current_slow and closest_level not in self.user_states[chat_id]['confirmation_states']:
                        logger.info(f"Detected upward crossing for chat_id={chat_id}: {closest_level:.6f}")
                        asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'up', closest_level))
                    elif prev_level > closest_level >= current_slow and closest_level not in self.user_states[chat_id]['confirmation_states']:
                        logger.info(f"Detected downward crossing for chat_id={chat_id}: {closest_level:.6f}")
                        asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'down', closest_level))

            self.user_states[chat_id]['active_level'] = min(levels, key=lambda x: abs(x - current_slow))

        except Exception as e:
            logger.error(f"Error for chat_id={chat_id}: {e}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def set_silent_hours(self, chat_id, time_range):
        try:
            start_str, end_str = time_range.split('-')
            start_time = datetime.strptime(start_str, "%H:%M").time()
            end_time = datetime.strptime(end_str, "%H:%M").time()
            await save_silent_hours(chat_id, start_time, end_time)
            self.user_states[chat_id]['silent_hours'] = (start_time, end_time)
            logger.info(f"Set silent hours for chat_id={chat_id}: {start_time}-{end_time}")
            return True, f"Тихие Часы установлены: {start_str}-{end_str}"
        except ValueError as e:
            logger.error(f"Invalid time format for chat_id={chat_id}: {time_range}, error: {str(e)}")
            return False, "Ошибка: введите время в формате ЧЧ:ММ-ЧЧ:ММ, например, 00:00-07:00"
        except Exception as e:
            logger.error(f"Error setting silent hours for chat_id={chat_id}: {str(e)}")
            return False, f"Ошибка: {str(e)}"

    async def background_price_fetcher(self):
        while True:
            try:
                logger.debug("Running background price fetch")
                await self.fetch_converter_data()
                await self.fetch_l2_data()
                logger.debug("Background price fetch completed")
            except Exception as e:
                logger.error(f"Error in background price fetch: {str(e)}")
            await asyncio.sleep(self.price_fetch_interval)

    async def fetch_converter_data(self):
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": "manta-network,ethereum,bitcoin",
            "order": "market_cap_desc",
            "per_page": 3,
            "page": 1,
            "sparkline": "false"
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.warning(f"CoinGecko API error for converter: {response.status}")
                        return self.converter_cache
                    data = await response.json()
                    prices = {coin["id"]: coin["current_price"] for coin in data}
                    self.converter_cache = prices
                    self.converter_cache_time = datetime.now(pytz.timezone('Europe/Kyiv'))
                    logger.debug("Converter data fetched and cached")
                    return prices
        except Exception as e:
            logger.error(f"Error fetching converter data: {str(e)}")
            return self.converter_cache

    async def convert_manta(self, chat_id, amount):
        try:
            prices = self.converter_cache
            if not prices:
                logger.warning(f"No converter data in cache for chat_id={chat_id}")
                await self.update_message(chat_id, "⚠️ Данные о ценах недоступны. Повторите запрос через пару минут.", create_menu_keyboard())
                return None

            manta_usd = prices.get("manta-network")
            eth_usd = prices.get("ethereum")
            btc_usd = prices.get("bitcoin")

            result = {
                "USDT": amount * manta_usd,
                "ETH": amount * manta_usd / eth_usd if eth_usd else 0,
                "BTC": amount * manta_usd / btc_usd if btc_usd else 0
            }
            message = (
                f"<pre>"
                f"Конвертация {int(amount)} MANTA:\n"
                f"◆ USDT: {result['USDT']:.2f}\n"
                f"◆ ETH:  {result['ETH']:.6f}\n"
                f"◆ BTC:  {result['BTC']:.8f}"
                f"</pre>"
            )
            await self.update_message(chat_id, message, create_menu_keyboard())
            return True

        except Exception as e:
            logger.error(f"Error in convert_manta for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, "⚠️ Ошибка при конвертации.", create_menu_keyboard())
            return None

    async def calculate_gas_cost(self, chat_id, gas_price, tx_count):
        try:
            prices = self.converter_cache
            if not prices:
                logger.warning(f"No price data in cache for gas calculator for chat_id={chat_id}")
                await self.update_message(chat_id, "⚠️ Данные о ценах недоступны. Повторите запрос через пару минут.", create_main_keyboard(chat_id))
                return None

            eth_usd = prices.get("ethereum")

            gas_units = 1000000
            fee_per_tx_eth = gas_price * gas_units / 10**9
            total_cost_usdt = fee_per_tx_eth * tx_count * eth_usd

            message = (
                f"<pre>"
                f"{int(tx_count)} транзакций, газ {gas_price:.6f} Gwei = {total_cost_usdt:.4f} USDT"
                f"</pre>"
            )
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
            return True

        except Exception as e:
            logger.error(f"Error in calculate_gas_cost for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, "⚠️ Ошибка при расчёте стоимости газа.", create_main_keyboard(chat_id))
            return None

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

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.warning(f"CoinGecko API error: {response.status}")
                        return self.l2_data_cache or token_data
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
                    self.l2_data_time = datetime.now(pytz.timezone('Europe/Kyiv'))
                    logger.debug("L2 data fetched and cached")
                    return token_data
        except Exception as e:
            logger.error(f"Error fetching L2 data: {str(e)}")
            return self.l2_data_cache or token_data

    async def fetch_fear_greed(self):
        current_time = datetime.now(pytz.timezone('Europe/Kyiv'))
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
            token_data = self.l2_data_cache
            if not token_data:
                logger.warning(f"No L2 data in cache for chat_id={chat_id}, waiting for background fetch")
                await self.update_message(chat_id, "⚠️ Данные о ценах недоступны. Пожалуйста, подождите несколько минут.", create_main_keyboard(chat_id))
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

            price_change_24h_str = f"{price_change_24h:.2f}%" if isinstance(price_change_24h, (int, float)) else price_change_24h
            price_change_7d_str = f"{price_change_7d:.2f}%" if isinstance(price_change_7d, (int, float)) else price_change_7d
            price_change_30d_str = f"{price_change_30d:.2f}%" if isinstance(price_change_30d, (int, float)) else price_change_30d
            price_change_all_str = f"{price_change_all:.2f}%" if isinstance(price_change_all, (int, float)) else price_change_all

            message = (
                f"<pre>"
                f"💰 MANTA/USDT\n"
                f"◆ Цена: ${price:.2f}\n"
                f"◆ 24ч: {price_change_24h_str}\n"
                f"◆ 7д: {price_change_7d_str}\n"
                f"◆ 30д: {price_change_30d_str}\n"
                f"◆ Всё время: {price_change_all_str}\n"
                f"◆ ATH: ${ath_price:.2f} ({ath_date})\n"
                f"◆ ATL: ${atl_price:.2f} ({atl_date})\n"
                f"◆ Объём спот (24ч): ${spot_volume:,.2f}\n"
                f"◆ Объём фьючерсы (24ч): ${futures_volume:,.2f}"
                f"</pre>"
            )
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
            self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Manta Price"] += 1
            await self.save_user_stats(chat_id)

        except Exception as e:
            logger.error(f"Error in get_manta_price for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, "⚠️ Ошибка при получении данных о цене.", create_main_keyboard(chat_id))

    async def compare_l2(self, chat_id):
        try:
            token_data = self.l2_data_cache
            if not token_data:
                logger.warning(f"No L2 data in cache for chat_id={chat_id}")
                await self.update_message(chat_id, "⚠️ Данные недоступны. Пожалуйста, подождите несколько минут.", create_main_keyboard(chat_id))
                return

            message = "<pre>🔍 Сравнение L2 токенов\n"
            for name, data in token_data.items():
                price = data["price"]
                price_24h = data["24h"]
                price_7d = data["7d"]
                price_30d = data["30d"]
                price_24h_str = f"{price_24h:.2f}%" if isinstance(price_24h, (int, float)) else price_24h
                price_7d_str = f"{price_7d:.2f}%" if isinstance(price_7d, (int, float)) else price_7d
                price_30d_str = f"{price_30d:.2f}%" if isinstance(price_30d, (int, float)) else price_30d
                message += (
                    f"◆ {name}\n"
                    f"  Цена: ${price:.2f}\n"
                    f"  24ч: {price_24h_str}\n"
                    f"  7д: {price_7d_str}\n"
                    f"  30д: {price_30d_str}\n"
                )
            message += "</pre>"

            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
            self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Сравнение L2"] += 1
            await self.save_user_stats(chat_id)

        except Exception as e:
            logger.error(f"Error in compare_l2 for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, "⚠️ Ошибка при сравнении L2 токенов.", create_main_keyboard(chat_id))

    async def handle_levels(self, chat_id):
        levels = self.user_states[chat_id]['current_levels']
        levels_str = ", ".join([f"{level:.6f}" for level in sorted(levels, reverse=True)])
        message = f"<pre>📊 Текущие уровни газа:\n{levels_str}</pre>"
        await self.update_message(chat_id, message, create_levels_keyboard())
        self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Задать Уровни"] += 1
        await self.save_user_stats(chat_id)

    async def handle_custom_levels(self, chat_id, levels_text):
        try:
            levels = [Decimal(level.strip()) for level in levels_text.split(',')]
            levels = [level for level in levels if level > 0]
            if not levels:
                await self.update_message(chat_id, "⚠️ Уровни не заданы или содержат ошибки.", create_levels_keyboard())
                return

            await self.save_levels(chat_id, levels)
            await self.reset_notified_levels(chat_id)
            levels_str = ", ".join([f"{level:.6f}" for level in sorted(levels, reverse=True)])
            message = f"<pre>📊 Уровни газа обновлены:\n{levels_str}</pre>"
            await self.update_message(chat_id, message, create_levels_keyboard())
            logger.info(f"Custom levels set for chat_id={chat_id}: {levels}")

        except Exception as e:
            logger.error(f"Error setting custom levels for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, "⚠️ Ошибка: уровни должны быть числами, разделёнными запятыми.", create_levels_keyboard())

    async def handle_notifications(self, chat_id):
        is_enabled = bool(self.user_states[chat_id]['current_levels'])
        message = (
            f"<pre>🔔 Уведомления: {'Включены' if is_enabled else 'Выключены'}\n"
            f"Текущие уровни: {', '.join([f'{level:.6f}' for level in sorted(self.user_states[chat_id]['current_levels'], reverse=True)]) if is_enabled else 'Отсутствуют'}</pre>"
        )
        await self.update_message(chat_id, message, create_notification_keyboard())
        self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Уведомления"] += 1
        await self.save_user_stats(chat_id)

    async def handle_fear_greed(self, chat_id):
        try:
            fear_greed_data = await self.fetch_fear_greed()
            if not fear_greed_data:
                await self.update_message(chat_id, "⚠️ Данные Fear & Greed недоступны.", create_main_keyboard(chat_id))
                return

            message = (
                f"<pre>😱 Fear & Greed Index\n"
                f"◆ Текущий: {fear_greed_data['current']['value']} ({fear_greed_data['current']['category']})\n"
                f"◆ Вчера: {fear_greed_data['yesterday']['value']} ({fear_greed_data['yesterday']['category']})\n"
                f"◆ Неделя назад: {fear_greed_data['week_ago']['value']} ({fear_greed_data['week_ago']['category']})\n"
                f"◆ Месяц назад: {fear_greed_data['month_ago']['value']} ({fear_greed_data['month_ago']['category']})\n"
                f"◆ Макс за год: {fear_greed_data['year_max']['value']} ({fear_greed_data['year_max']['category']}, {fear_greed_data['year_max']['date']})\n"
                f"◆ Мин за год: {fear_greed_data['year_min']['value']} ({fear_greed_data['year_min']['category']}, {fear_greed_data['year_min']['date']})"
                f"</pre>"
            )
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
            self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Страх и Жадность"] += 1
            await self.save_user_stats(chat_id)

        except Exception as e:
            logger.error(f"Error in handle_fear_greed for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, "⚠️ Ошибка при получении Fear & Greed.", create_main_keyboard(chat_id))

    async def handle_admin(self, chat_id):
        if chat_id != ADMIN_ID:
            await self.update_message(chat_id, "⚠️ Доступ только для администратора.", create_main_keyboard(chat_id))
            return
        await self.update_message(chat_id, "<pre>⚙️ Админ-панель</pre>", create_admin_keyboard())
        self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Админ"] += 1
        await self.save_user_stats(chat_id)

    async def handle_statistics(self, chat_id):
        if chat_id != ADMIN_ID:
            await self.update_message(chat_id, "⚠️ Доступ только для администратора.", create_main_keyboard(chat_id))
            return

        message = "<pre>📊 Статистика использования\n"
        user_names = {user_id: name for user_id, name in ALLOWED_USERS}
        for user_id, stats in self.user_stats.items():
            user_name = user_names.get(user_id, f"ID {user_id}")
            message += f"Пользователь: {user_name}\n"
            for date, commands in stats.items():
                message += f"  Дата: {date}\n"
                for cmd, count in commands.items():
                    if count > 0:
                        message += f"    {cmd}: {count}\n"
        message += "</pre>"

        await self.update_message(chat_id, message, create_admin_keyboard())

    async def handle_restart(self, chat_id):
        if chat_id != ADMIN_ID:
            await self.update_message(chat_id, "⚠️ Доступ только для администратора.", create_main_keyboard(chat_id))
            return
        await self.update_message(chat_id, "🔄 Перезапуск инициирован.", create_admin_keyboard())
        logger.info(f"Restart initiated by admin chat_id={chat_id}")
        import os
        os._exit(0)

    async def setup_handlers(self):
        @self.dp.message(Command("start"))
        async def cmd_start(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.update_message(chat_id, "<pre>⛽ Добро пожаловать в Manta Gas Bot!</pre>", create_main_keyboard(chat_id))
            await self.set_menu_button()

        @self.dp.message(lambda message: message.text == "⛽ Газ")
        async def handle_gas(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.get_manta_gas(chat_id, force_base_message=True)
            self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Газ"] += 1
            await self.save_user_stats(chat_id)

        @self.dp.message(lambda message: message.text == "💰 Manta Price")
        async def handle_price(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.get_manta_price(chat_id)

        @self.dp.message(lambda message: message.text == "🔍 Сравнение L2")
        async def handle_l2_compare(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.compare_l2(chat_id)

        @self.dp.message(lambda message: message.text == "📊 Задать Уровни")
        async def handle_set_levels(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.handle_levels(chat_id)

        @self.dp.message(lambda message: message.text == "📝 Задать свои уровни")
        async def handle_custom_levels_prompt(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.update_message(chat_id, "Введите уровни газа через запятую (например: 0.01, 0.005, 0.001):", create_levels_keyboard())
            self.pending_commands[chat_id] = "set_custom_levels"

        @self.dp.message(lambda message: message.text == "🔄 Сбросить уровни")
        async def handle_reset_levels(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.load_or_set_default_levels(chat_id)
            await self.reset_notified_levels(chat_id)
            levels = self.user_states[chat_id]['current_levels']
            levels_str = ", ".join([f"{level:.6f}" for level in sorted(levels, reverse=True)])
            message = f"<pre>📊 Уровни газа сброшены:\n{levels_str}</pre>"
            await self.update_message(chat_id, message, create_levels_keyboard())

        @self.dp.message(lambda message: message.text == "🔔 Уведомления")
        async def handle_notification_settings(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.handle_notifications(chat_id)

        @self.dp.message(lambda message: message.text == "🔔 Включить уведомления")
        async def handle_enable_notifications(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            if not self.user_states[chat_id]['current_levels']:
                await self.load_or_set_default_levels(chat_id)
            await self.handle_notifications(chat_id)

        @self.dp.message(lambda message: message.text == "🔕 Выключить уведомления")
        async def handle_disable_notifications(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            self.user_states[chat_id]['current_levels'] = []
            await self.save_levels(chat_id, [])
            await self.reset_notified_levels(chat_id)
            await self.handle_notifications(chat_id)

        @self.dp.message(lambda message: message.text == "😱 Страх и Жадность")
        async def handle_fear_greed_command(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.handle_fear_greed(chat_id)

        @self.dp.message(lambda message: message.text == "🤫 Тихие Часы")
        async def handle_silent_hours(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            start_time, end_time = self.user_states[chat_id]['silent_hours']
            current_hours = f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}" if start_time and end_time else "Не установлены"
            await self.update_message(chat_id, f"Текущие тихие часы: {current_hours}\nВведите новые часы в формате ЧЧ:ММ-ЧЧ:ММ (например, 00:00-07:00):", create_menu_keyboard())
            self.pending_commands[chat_id] = "set_silent_hours"
            self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Тихие Часы"] += 1
            await self.save_user_stats(chat_id)

        @self.dp.message(lambda message: message.text == "🔄 Manta Конвертер")
        async def handle_converter(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.update_message(chat_id, "Введите количество MANTA для конвертации:", create_menu_keyboard())
            self.pending_commands[chat_id] = "convert_manta"
            self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Manta Конвертер"] += 1
            await self.save_user_stats(chat_id)

        @self.dp.message(lambda message: message.text == "🧮 Газ Калькулятор")
        async def handle_gas_calculator(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.update_message(chat_id, "Введите цену газа (Gwei) и количество транзакций (через пробел, например: 0.01 10):", create_menu_keyboard())
            self.pending_commands[chat_id] = "calculate_gas_cost"
            self.user_stats[chat_id][datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()]["Газ Калькулятор"] += 1
            await self.save_user_stats(chat_id)

        @self.dp.message(lambda message: message.text == "⚙️ Админ")
        async def handle_admin_command(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.handle_admin(chat_id)

        @self.dp.message(lambda message: message.text == "📊 Статистика")
        async def handle_stats_command(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.handle_statistics(chat_id)

        @self.dp.message(lambda message: message.text == "🔄 Перезапуск")
        async def handle_restart_command(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            await self.handle_restart(chat_id)

        @self.dp.message(lambda message: message.text == "🔙 Вернуться в меню")
        async def handle_back_to_menu(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            self.pending_commands.pop(chat_id, None)
            await self.get_manta_gas(chat_id, force_base_message=True)

        @self.dp.message(lambda message: message.text == "🔄 Обновить")
        async def handle_refresh(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            pending = self.pending_commands.get(chat_id)
            if pending == "convert_manta":
                await self.convert_manta(chat_id, float(message.text))
            elif pending == "set_silent_hours":
                success, msg = await self.set_silent_hours(chat_id, message.text)
                await self.update_message(chat_id, msg, create_main_keyboard(chat_id) if success else create_menu_keyboard())
                if success:
                    self.pending_commands.pop(chat_id, None)
            elif pending == "calculate_gas_cost":
                try:
                    gas_price, tx_count = map(float, message.text.split())
                    await self.calculate_gas_cost(chat_id, gas_price, tx_count)
                except ValueError:
                    await self.update_message(chat_id, "⚠️ Ошибка: введите цену газа и количество транзакций через пробел.", create_menu_keyboard())
            else:
                await self.get_manta_gas(chat_id, force_base_message=True)

        @self.dp.message()
        async def handle_text(message: types.Message):
            if not await self.check_access(message):
                return
            chat_id = message.chat.id
            text = message.text.strip()
            pending = self.pending_commands.get(chat_id)

            if pending == "set_custom_levels":
                await self.handle_custom_levels(chat_id, text)
                self.pending_commands.pop(chat_id, None)
            elif pending == "set_silent_hours":
                success, msg = await self.set_silent_hours(chat_id, text)
                await self.update_message(chat_id, msg, create_main_keyboard(chat_id) if success else create_menu_keyboard())
                if success:
                    self.pending_commands.pop(chat_id, None)
            elif pending == "convert_manta":
                try:
                    amount = float(text)
                    await self.convert_manta(chat_id, amount)
                except ValueError:
                    await self.update_message(chat_id, "⚠️ Ошибка: введите число.", create_menu_keyboard())
            elif pending == "calculate_gas_cost":
                try:
                    gas_price, tx_count = map(float, text.split())
                    await self.calculate_gas_cost(chat_id, gas_price, tx_count)
                except ValueError:
                    await self.update_message(chat_id, "⚠️ Ошибка: введите цену газа и количество транзакций через пробел.", create_menu_keyboard())
            else:
                await self.get_manta_gas(chat_id, force_base_message=True)

state = None

async def monitor_gas(state):
    async def callback(gas_value):
        for user_id, _ in ALLOWED_USERS:
            await state.get_manta_gas(user_id)
    await state.scanner.monitor_gas(INTERVAL, callback)

async def main():
    global state
    scanner = Scanner()
    await scanner.init_session()
    state = BotState(scanner)
    await state.setup_handlers()
    for user_id, _ in ALLOWED_USERS:
        await state.load_user_stats(user_id)
    asyncio.create_task(state.background_price_fetcher())
    asyncio.create_task(monitor_gas(state))
    await state.dp.start_polling(state.bot)