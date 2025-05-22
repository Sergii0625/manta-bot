# telegram_bot.py
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
import pytz
import aiofiles

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
LEVELS_FILE = "user_levels.json"

def is_silent_hour(user_id, now_kyiv, user_states):
    silent_enabled = user_states[user_id].get('silent_enabled', True)
    if not silent_enabled:
        return False
    start_time = time(0, 0)  # 00:00
    end_time = time(8, 0)   # 08:00
    now_time = now_kyiv.time()
    if start_time <= end_time:
        return start_time <= now_time <= end_time
    else:
        return now_time >= start_time or now_time <= end_time

async def schedule_restart():
    """Планировщик перезапуска задач бота в указанное время."""
    while True:
        try:
            now_kyiv = datetime.now(pytz.timezone('Europe/Kyiv'))
            current_time = now_kyiv.strftime("%H:%M")
            if current_time in RESTART_TIMES:
                logger.info("Scheduled restart triggered at %s", current_time)
                # Здесь можно добавить логику перезапуска, например, очистку кэшей или перезапуск задач
                for user_id, _ in ALLOWED_USERS:
                    await state.reset_notified_levels(user_id)
                    logger.info(f"Reset notified levels for user_id={user_id} during scheduled restart")
                # Задержка на минуту, чтобы избежать многократного срабатывания в ту же минуту
                await asyncio.sleep(60)
            else:
                # Проверяем каждые 30 секунд
                await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"Error in schedule_restart: {str(e)}")
            await asyncio.sleep(30)  # Задержка перед повторной попыткой

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
        if user_id not in self.user_states:
            self.user_states[user_id] = {
                'prev_level': None,
                'last_measured_gas': None,
                'current_levels': [],
                'default_levels': [],
                'user_added_levels': [],
                'active_level': None,
                'confirmation_states': {},
                'notified_levels': set(),
                'silent_enabled': True
            }
            await self.load_or_set_default_levels(user_id)
            logger.debug(f"Initialized user_state for user_id={user_id}, current_levels={self.user_states[user_id]['current_levels']}, silent_enabled={self.user_states[user_id]['silent_enabled']}")

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
            default_levels, user_added_levels = self.load_levels_from_file(user_id)
            if not default_levels or not user_added_levels:
                logger.warning(f"No levels or empty levels for user_id={user_id}, setting default levels")
                default_levels = [
                    Decimal('0.010000'), Decimal('0.009500'), Decimal('0.009000'), Decimal('0.008500'),
                    Decimal('0.008000'), Decimal('0.007500'), Decimal('0.007000'), Decimal('0.006500'),
                    Decimal('0.006000'), Decimal('0.005500'), Decimal('0.005000'), Decimal('0.004500'),
                    Decimal('0.004000'), Decimal('0.003500'), Decimal('0.003000'), Decimal('0.002500'),
                    Decimal('0.002000'), Decimal('0.001500'), Decimal('0.001000'), Decimal('0.000900'),
                    Decimal('0.000800'), Decimal('0.000700'), Decimal('0.000600'), Decimal('0.000500'),
                    Decimal('0.000400'), Decimal('0.000300'), Decimal('0.000200'), Decimal('0.000100'),
                    Decimal('0.000050')
                ]
                user_added_levels = []
                levels_data = {
                    'default_levels': default_levels,
                    'user_added_levels': user_added_levels
                }
                await self.save_levels_to_file(user_id, levels_data)
                logger.info(f"Default levels saved for user_id={user_id}: {default_levels}")
            self.user_states[user_id]['default_levels'] = default_levels
            self.user_states[user_id]['user_added_levels'] = user_added_levels
            self.user_states[user_id]['current_levels'] = sorted(set(default_levels + user_added_levels), reverse=True)
            logger.info(f"Loaded levels for user_id={user_id}: {self.user_states[user_id]['current_levels']}")
        except Exception as e:
            logger.error(f"Error loading levels for user_id={user_id}: {str(e)}, setting to default levels")
            default_levels = [
                Decimal('0.010000'), Decimal('0.009500'), Decimal('0.009000'), Decimal('0.008500'),
                Decimal('0.008000'), Decimal('0.007500'), Decimal('0.007000'), Decimal('0.006500'),
                Decimal('0.006000'), Decimal('0.005500'), Decimal('0.005000'), Decimal('0.004500'),
                Decimal('0.004000'), Decimal('0.003500'), Decimal('0.003000'), Decimal('0.002500'),
                Decimal('0.002000'), Decimal('0.001500'), Decimal('0.001000'), Decimal('0.000900'),
                Decimal('0.000800'), Decimal('0.000700'), Decimal('0.000600'), Decimal('0.000500'),
                Decimal('0.000400'), Decimal('0.000300'), Decimal('0.000200'), Decimal('0.000100'),
                Decimal('0.000050')
            ]
            self.user_states[user_id]['default_levels'] = default_levels
            self.user_states[user_id]['user_added_levels'] = []
            self.user_states[user_id]['current_levels'] = default_levels.copy()
            try:
                await self.save_levels_to_file(user_id, {'default_levels': default_levels, 'user_added_levels': []})
                logger.info(f"Default levels saved to file after error for user_id={user_id}")
            except Exception as e:
                logger.error(f"Failed to save default levels for user_id={user_id}: {str(e)}")
            logger.info(f"Set default levels due to error for user_id={user_id}: {self.user_states[user_id]['current_levels']}")

    async def save_levels_to_file(self, user_id, levels_data):
        try:
            async with aiofiles.open(LEVELS_FILE, 'r') as f:
                all_levels = json.loads(await f.read())
        except (FileNotFoundError, json.JSONDecodeError):
            all_levels = {}
        all_levels[str(user_id)] = {
            'default_levels': [str(level) for level in levels_data['default_levels']],
            'user_added_levels': [str(level) for level in levels_data['user_added_levels']]
        }
        async with aiofiles.open(LEVELS_FILE, 'w') as f:
            await f.write(json.dumps(all_levels, indent=2))
        logger.debug(f"Saved levels to file for user_id={user_id}: {levels_data}")

    def load_levels_from_file(self, user_id):
        file_path = "user_levels.json"
        default_levels = [Decimal(str(x)) for x in [0.010000, 0.009500, 0.009000, 0.008500, 0.008000, 0.007500, 0.007000, 0.006500, 0.006000, 0.005500, 0.005000, 0.004500, 0.004000, 0.003500, 0.003000, 0.002500, 0.002000, 0.001500, 0.001000, 0.000900, 0.000800, 0.000700, 0.000600, 0.000500, 0.000400, 0.000300, 0.000200, 0.000100, 0.000050]]

        logger.debug(f"Checking {file_path} for user_id={user_id}")
        if not os.path.exists(file_path):
            logger.info(f"File {file_path} not found, creating new")
            with open(file_path, 'w') as f:
                json.dump({}, f)

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                user_data = data.get(str(user_id), {})
                return (
                    [Decimal(str(x)) for x in user_data.get("default_levels", default_levels)],
                    [Decimal(str(x)) for x in user_data.get("user_added_levels", [])]
                )
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error reading {file_path}: {e}")
            return default_levels, []

    async def save_levels(self, user_id, levels):
        default_levels = self.user_states[user_id]['default_levels']
        user_added_levels = [level for level in levels if level not in default_levels]
        self.user_states[user_id]['user_added_levels'] = user_added_levels
        self.user_states[user_id]['current_levels'] = sorted(set(default_levels + user_added_levels), reverse=True)
        try:
            await self.save_levels_to_file(user_id, {
                'default_levels': default_levels,
                'user_added_levels': user_added_levels
            })
            logger.debug(f"Saved levels for user_id={user_id}: {self.user_states[user_id]['current_levels']}")
        except Exception as e:
            logger.error(f"Error saving levels for user_id={user_id}: {str(e)}")

    async def update_message(self, chat_id, text, reply_markup=None):
        try:
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
        if is_silent_hour(chat_id, now_kyiv, self.user_states):
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
            # Отправка уведомления в зависимости от типа уровня
            if target_level in self.user_states[chat_id]['default_levels']:
                # Уведомление для всех пользователей
                for user_id, _ in ALLOWED_USERS:
                    if user_id == chat_id or target_level in self.user_states[user_id]['current_levels']:
                        try:
                            await self.update_message(user_id, notification_message, create_main_keyboard(user_id))
                            self.user_states[user_id]['notified_levels'].add(target_level)
                            self.user_states[user_id]['active_level'] = target_level
                            self.user_states[user_id]['prev_level'] = last_measured
                            logger.info(f"Level {target_level:.6f} notified to user_id={user_id}")
                        except Exception as e:
                            logger.error(f"Failed to notify user_id={user_id}: {str(e)}")
            else:
                # Уведомление только для пользователя, добавившего уровень
                await self.update_message(chat_id, notification_message, create_main_keyboard(chat_id))
                self.user_states[chat_id]['notified_levels'].add(target_level)
                self.user_states[chat_id]['active_level'] = target_level
                self.user_states[chat_id]['prev_level'] = last_measured
                logger.info(f"Level {target_level:.6f} notified to chat_id={chat_id} (user-added level)")
        else:
            logger.info(f"Confirmation failed or already notified for chat_id={chat_id}, target={target_level:.6f}, is_confirmed={is_confirmed}, notified={target_level in self.user_states[chat_id]['notified_levels']}")

        state['count'] = 0
        state['values'] = []
        del self.user_states[chat_id]['confirmation_states'][target_level]

    async def get_manta_gas(self, chat_id, force_base_message=False):
        try:
            logger.debug(f"get_manta_gas called for chat_id={chat_id}, force_base_message={force_base_message}")
            current_slow = await self.scanner.get_current_gas()
            if current_slow is None:
                logger.error(f"Failed to fetch gas price for chat_id={chat_id}")
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
            logger.debug(f"get_manta_gas for chat_id={chat_id}: current_levels={levels}, prev_level={prev_level}")

            if not levels:
                logger.warning(f"No levels set for chat_id={chat_id}, attempting to load default levels")
                await self.load_or_set_default_levels(chat_id)
                levels = self.user_states[chat_id]['current_levels']
                logger.debug(f"After reload, current_levels for chat_id={chat_id}: {levels}")

            if not levels:
                logger.info(f"No levels set for chat_id={chat_id}, skipping notification check")
                if force_base_message:
                    await self.update_message(chat_id, base_message + "\n\nУровни не заданы. Используйте 'Задать Уровни'.", create_main_keyboard(chat_id))
                self.user_states[chat_id]['prev_level'] = current_slow
                return

            if prev_level is None or force_base_message:
                logger.debug(f"Initial or forced message for chat_id={chat_id}, prev_level={prev_level}")
                await self.update_message(chat_id, base_message, create_main_keyboard(chat_id))
                self.user_states[chat_id]['prev_level'] = current_slow
            else:
                kyiv_tz = pytz.timezone('Europe/Kyiv')
                now_kyiv = datetime.now(kyiv_tz)
                if is_silent_hour(chat_id, now_kyiv, self.user_states):
                    logger.info(f"Silent hours active for chat_id={chat_id}, skipping notification check")
                    self.user_states[chat_id]['prev_level'] = current_slow
                    return

                sorted_levels = sorted(levels)
                closest_level = min(sorted_levels, key=lambda x: abs(x - current_slow))
                logger.debug(f"Checking level crossing for chat_id={chat_id}: current={current_slow:.6f}, prev={prev_level:.6f}, closest_level={closest_level:.6f}")
                if prev_level < closest_level <= current_slow and closest_level not in self.user_states[chat_id]['confirmation_states']:
                    logger.info(f"Detected upward crossing for chat_id={chat_id}: {closest_level:.6f}")
                    asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'up', closest_level))
                elif prev_level > closest_level >= current_slow and closest_level not in self.user_states[chat_id]['confirmation_states']:
                    logger.info(f"Detected downward crossing for chat_id={chat_id}: {closest_level:.6f}")
                    asyncio.create_task(self.confirm_level_crossing(chat_id, current_slow, 'down', closest_level))

            self.user_states[chat_id]['active_level'] = min(levels, key=lambda x: abs(x - current_slow))
            self.user_states[chat_id]['prev_level'] = current_slow
            logger.debug(f"Updated state for chat_id={chat_id}: active_level={self.user_states[chat_id]['active_level']:.6f}, prev_level={current_slow:.6f}")

        except Exception as e:
            logger.error(f"Error in get_manta_gas for chat_id={chat_id}: {e}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def set_silent_hours(self, chat_id, enable):
        self.user_states[chat_id]['silent_enabled'] = enable
        logger.info(f"Silent hours {'enabled' if enable else 'disabled'} for chat_id={chat_id}")

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
            spot_volume_m = round(spot_volume / Decimal('1000000')) if spot_volume else 0
            futures_volume_m = round(futures_volume / Decimal('1000000')) if futures_volume else 0
            spot_volume_str = f"{spot_volume_m}M$" if spot_volume else "Н/Д"
            futures_volume_str = f"{futures_volume_m}M$" if futures_volume else "Н/Д"
            message = (
                f"<pre>"
                f"🦎 Данные с CoinGecko:\n"
                f"◆ MANTA/USDT: ${float(price):.3f}\n\n"
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
            token_data = self.l2_data_cache
            if not token_data:
                logger.warning(f"No L2 data in cache for chat_id={chat_id}, waiting for background fetch")
                await self.update_message(chat_id, "⚠️ Данные о ценах недоступны. Пожалуйста, подождите несколько минут.", create_main_keyboard(chat_id))
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
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data['24h']):>6.2f}%" if data['24h'] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"
            message += "\n◆ Сравнение L2 токенов (7 дней):\n"
            sorted_by_7d = sorted(
                token_data.items(),
                key=lambda x: float(x[1]["7d"]) if x[1]["7d"] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_by_7d:
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data['7d']):>6.2f}%" if data['7d'] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"
            message += "\n◆ Сравнение L2 токенов (месяц):\n"
            sorted_by_30d = sorted(
                token_data.items(),
                key=lambda x: float(x[1]["30d"]) if x[1]["30d"] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_by_30d:
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data['30d']):>6.2f}%" if data['30d'] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"
            message += "\n◆ Сравнение L2 токенов (все время):\n"
            sorted_by_all = sorted(
                token_data.items(),
                key=lambda x: float(x[1]["all"]) if x[1]["all"] not in ("Н/Д", None) else float('-inf'),
                reverse=True
            )
            for name, data in sorted_by_all:
                price_str = f"${float(data['price']):.3f}" if data['price'] not in ("Н/Д", None) else "Н/Д"
                change_str = f"{float(data['all']):>6.2f}%" if data['all'] not in ("Н/Д", None) else "Н/Д"
                message += f"◆ {name:<9}: {price_str} | {change_str}\n"
            message += "</pre>"
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
        except Exception as e:
            logger.error(f"Error fetching L2 comparison for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def get_fear_greed(self, chat_id):
        try:
            fg_data = await self.fetch_fear_greed()
            if not fg_data:
                await self.update_message(chat_id, "⚠️ Не удалось получить данные Fear & Greed от CoinMarketCap.", create_main_keyboard(chat_id))
                return
            current_value = fg_data["current"]["value"]
            yesterday_value = fg_data["yesterday"]["value"]
            week_ago_value = fg_data["week_ago"]["value"]
            month_ago_value = fg_data["month_ago"]["value"]
            max_year_value = fg_data["year_max"]["value"]
            max_year_date = fg_data["year_max"]["date"]
            min_year_value = fg_data["year_min"]["value"]
            min_year_date = fg_data["year_min"]["date"]
            bar_length = 20
            filled = int(current_value / 100 * bar_length)
            progress_bar = f"🔴 {'█' * filled}{'▁' * (bar_length - filled)} 🟢"
            message = (
                f"<pre>"
                f"◆ Индекс страха и жадности: {current_value}\n"
                f"\n"
                f"{progress_bar}\n"
                f"\n"
                f"История:\n"
                f"🕒 Вчера: {yesterday_value}\n"
                f"🕒 Прошлая неделя: {week_ago_value}\n"
                f"🕒 Прошлый месяц: {month_ago_value}\n"
                f"\n"
                f"Годовые экстремумы:\n"
                f"📈 Макс: {max_year_value} ({max_year_date})\n"
                f"📉 Мин: {min_year_value} ({min_year_date})"
                f"</pre>"
            )
            await self.update_message(chat_id, message, create_main_keyboard(chat_id))
        except Exception as e:
            logger.error(f"Error fetching Fear & Greed for chat_id={chat_id}: {str(e)}")
            await self.update_message(chat_id, f"<b>⚠️ Ошибка:</b> {str(e)}", create_main_keyboard(chat_id))

    async def get_admin_stats(self, chat_id):
        if chat_id != ADMIN_ID:
            logger.warning(f"Unauthorized access to admin stats by chat_id={chat_id}")
            return
        today = datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()
        # Инициализируем статистику для всех пользователей
        for user_id, _ in ALLOWED_USERS:
            if user_id not in self.user_stats:
                self.init_user_stats(user_id)
                logger.debug(f"Initialized user_stats for user_id={user_id} in get_admin_stats")
        message = "<b>Статистика использования бота за сегодня:</b>\n\n<pre>"
        has_activity = False
        for user_id, user_name in ALLOWED_USERS:
            if user_id == ADMIN_ID:
                continue
            stats = self.user_stats[user_id].get(today, {})
            logger.debug(f"Accessing stats for user_id={user_id}, stats={stats}")
            if any(stats.values()):
                message += f"{user_id} {user_name}\n"
                for action, count in stats.items():
                    if count > 0:
                        message += f"{action} - {count}\n"
                message += "\n"
                has_activity = True
        message += "</pre>"
        if not has_activity:
            message = "<b>Статистика использования бота за сегодня:</b>\n\nСегодня никто из пользователей (кроме админа) не использовал бота."
        await self.update_message(chat_id, message, create_main_keyboard(chat_id))

def create_main_keyboard(chat_id):
    if chat_id == ADMIN_ID:
        keyboard = [
            [types.KeyboardButton(text="Газ")],
            [types.KeyboardButton(text="Админ"), types.KeyboardButton(text="Меню")]
        ]
    else:
        keyboard = [
            [types.KeyboardButton(text="Газ"), types.KeyboardButton(text="Меню")]
        ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_menu_keyboard():
    keyboard = [
        [types.KeyboardButton(text="Manta Конвертер"), types.KeyboardButton(text="Газ Калькулятор")],
        [types.KeyboardButton(text="Manta Price"), types.KeyboardButton(text="Сравнение L2")],
        [types.KeyboardButton(text="Страх и Жадность"), types.KeyboardButton(text="Тихие Часы")],
        [types.KeyboardButton(text="Задать Уровни"), types.KeyboardButton(text="Уведомления")],
        [types.KeyboardButton(text="Назад")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_silent_hours_keyboard():
    keyboard = [
        [types.KeyboardButton(text="Отключить Тихие Часы"), types.KeyboardButton(text="Включить Тихие Часы")],
        [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_converter_keyboard():
    keyboard = [
        [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_gas_calculator_keyboard():
    keyboard = [
        [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_levels_menu_keyboard():
    keyboard = [
        [types.KeyboardButton(text="0.00001–0.01")],
        [types.KeyboardButton(text="Удалить уровни")],
        [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_level_input_keyboard():
    keyboard = [
        [types.KeyboardButton(text="Добавить еще уровень")],
        [types.KeyboardButton(text="Завершить")],
        [types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_delete_levels_keyboard(levels):
    keyboard = [[types.KeyboardButton(text=f"Удалить {level:.5f} Gwei")] for level in levels]
    keyboard.append([types.KeyboardButton(text="Назад"), types.KeyboardButton(text="Отмена")])
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=False)

scanner = Scanner()
state = BotState(scanner)

@state.dp.message(Command("start"))
async def start_command(message: types.Message):
    if not await state.check_access(message):
        return
    chat_id = message.chat.id
    logger.info(f"Start command received from chat_id={chat_id}")
    await state.update_message(chat_id, "<b>Бот для Manta Pacific запущен.</b>\nВыберите действие:", create_main_keyboard(chat_id))
    try:
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete start command message_id={message.message_id}: {e}")

@state.dp.message(lambda message: message.text in [
    "Газ", "Manta Price", "Сравнение L2", "Страх и Жадность",
    "Задать Уровни", "Уведомления", "Админ", "Тихие Часы", "Меню", "Назад",
    "Manta Конвертер", "Газ Калькулятор"
])
async def handle_main_button(message: types.Message):
    if not await state.check_access(message):
        return
    chat_id = message.chat.id
    text = message.text
    logger.debug(f"Button pressed: {text} by chat_id={chat_id}")

    today = datetime.now(pytz.timezone('Europe/Kyiv')).date().isoformat()
    if text not in ["Меню", "Назад"]:
        state.user_stats[chat_id][today][text] += 1

    if chat_id in state.pending_commands and text not in ["Задать Уровни", "Тихие Часы", "Manta Конвертер", "Газ Калькулятор"]:
        del state.pending_commands[chat_id]

    if text == "Газ":
        await state.get_manta_gas(chat_id, force_base_message=True)
    elif text == "Manta Price":
        await state.get_manta_price(chat_id)
    elif text == "Сравнение L2":
        await state.get_l2_comparison(chat_id)
    elif text == "Страх и Жадность":
        await state.get_fear_greed(chat_id)
    elif text == "Задать Уровни":
        state.pending_commands[chat_id] = {'step': 'range_selection'}
        await state.update_message(chat_id, "Выберите действие для уровней уведомлений:", create_levels_menu_keyboard())
    elif text == "Уведомления":
        await state.reset_notified_levels(chat_id)
        current_levels = state.user_states[chat_id]['current_levels']
        logger.debug(f"Notification levels for chat_id={chat_id}: {current_levels}")
        if current_levels:
            levels_text = "\n".join([f"◆ {level:.6f} Gwei" for level in current_levels])
            formatted_message = f"<b><pre>ТЕКУЩИЕ УВЕДОМЛЕНИЯ:\n\n{levels_text}</pre></b>"
            await state.update_message(chat_id, formatted_message, create_main_keyboard(chat_id))
        else:
            await state.update_message(chat_id, "Уровни не установлены.", create_main_keyboard(chat_id))
    elif text == "Админ":
        if chat_id == ADMIN_ID:
            await state.get_admin_stats(chat_id)
        else:
            await state.update_message(chat_id, "Доступ только для админа.", create_main_keyboard(chat_id))
    elif text == "Тихие Часы":
        silent_enabled = state.user_states[chat_id]['silent_enabled']
        status = "включены" if silent_enabled else "отключены"
        await state.update_message(
            chat_id,
            f"Тихие Часы {status}: 00:00–08:00",
            create_silent_hours_keyboard()
        )
        state.pending_commands[chat_id] = {'step': 'silent_hours_input'}
    elif text == "Manta Конвертер":
        state.pending_commands[chat_id] = {'step': 'converter_input'}
        await state.update_message(chat_id, "Введите количество MANTA для конвертации:", create_converter_keyboard())
    elif text == "Газ Калькулятор":
        state.pending_commands[chat_id] = {'step': 'gas_calculator_gas_input'}
        await state.update_message(chat_id, "Введите цену газа в Gwei (например, 0.0015):", create_gas_calculator_keyboard())
    elif text == "Меню":
        await state.update_message(chat_id, "Выберите действие:", create_menu_keyboard())
    elif text == "Назад":
        await state.update_message(chat_id, "Возврат в главное меню.", create_main_keyboard(chat_id))

    try:
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete user message_id={message.message_id}: {e}")

@state.dp.message()
async def process_value(message: types.Message):
    if not await state.check_access(message):
        return
    chat_id = message.chat.id
    text = message.text.strip()

    if chat_id not in state.pending_commands:
        await state.update_message(chat_id, "Выберите действие с помощью кнопок.", create_main_keyboard(chat_id))
    else:
        state_data = state.pending_commands[chat_id]

        if state_data['step'] == 'converter_input':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
            elif text == "Назад":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Возврат в меню.", create_menu_keyboard())
            else:
                try:
                    amount = float(text.replace(',', '.'))
                    if amount <= 0:
                        await state.update_message(chat_id, "Ошибка: введите положительное число.", create_converter_keyboard())
                        return
                    result = await state.convert_manta(chat_id, amount)
                    if result is None:
                        pass
                    del state.pending_commands[chat_id]
                except ValueError:
                    await state.update_message(chat_id, "Ошибка: введите корректное число.", create_converter_keyboard())

        elif state_data['step'] == 'gas_calculator_gas_input':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
            elif text == "Назад":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Возврат в меню.", create_menu_keyboard())
            else:
                try:
                    gas_price = float(text.replace(',', '.'))
                    if gas_price <= 0:
                        await state.update_message(chat_id, "Ошибка: введите положительное число.", create_gas_calculator_keyboard())
                        return
                    state_data['gas_price'] = gas_price
                    state_data['step'] = 'gas_calculator_tx_count_input'
                    await state.update_message(chat_id, "Введите количество транзакций (например, 100):", create_gas_calculator_keyboard())
                except ValueError:
                    await state.update_message(chat_id, "Ошибка: введите корректное число (используйте точку или запятую).", create_gas_calculator_keyboard())

        elif state_data['step'] == 'gas_calculator_tx_count_input':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
            elif text == "Назад":
                state_data['step'] = 'gas_calculator_gas_input'
                await state.update_message(chat_id, "Введите цену газа в Gwei (например, 0.0015):", create_gas_calculator_keyboard())
            else:
                try:
                    tx_count = int(text)
                    if tx_count <= 0:
                        await state.update_message(chat_id, "Ошибка: введите положительное целое число.", create_gas_calculator_keyboard())
                        return
                    result = await state.calculate_gas_cost(chat_id, state_data['gas_price'], tx_count)
                    if result is None:
                        pass
                    del state.pending_commands[chat_id]
                except ValueError:
                    await state.update_message(chat_id, "Ошибка: введите целое число.", create_gas_calculator_keyboard())

        elif state_data['step'] == 'silent_hours_input':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
            elif text == "Назад":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Возврат в главное меню.", create_main_keyboard(chat_id))
            elif text == "Отключить Тихие Часы":
                await state.set_silent_hours(chat_id, False)
                logger.info(f"Disabled silent hours for chat_id={chat_id}")
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Тихие Часы отключены.", create_main_keyboard(chat_id))
            elif text == "Включить Тихие Часы":
                await state.set_silent_hours(chat_id, True)
                logger.info(f"Enabled silent hours for chat_id={chat_id}")
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Тихие Часы включены: 00:00–08:00", create_main_keyboard(chat_id))
            else:
                await state.update_message(chat_id, "Тихие Часы фиксированы: 00:00–08:00. Выберите действие.", create_silent_hours_keyboard())

        elif state_data['step'] == 'range_selection':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
            elif text == "Назад":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Возврат в главное меню.", create_main_keyboard(chat_id))
            elif text == "0.00001–0.01":
                min_val, max_val = 0.00001, 0.01
                state_data['range'] = (min_val, max_val)
                state_data['levels'] = state.user_states[chat_id]['current_levels'].copy()
                state_data['step'] = 'level_input'
                await state.update_message(chat_id, "Введите уровень от 0,00001 до 0,01:", create_level_input_keyboard())
            elif text == "Удалить уровни":
                if not state.user_states[chat_id]['current_levels']:
                    await state.update_message(chat_id, "Уровни не установлены.", create_main_keyboard(chat_id))
                    del state.pending_commands[chat_id]
                else:
                    state_data['step'] = 'delete_level_selection'
                    await state.update_message(chat_id, "Выберите уровень для удаления:", create_delete_levels_keyboard(state.user_states[chat_id]['current_levels']))
            else:
                await state.update_message(chat_id, "Выберите действие из предложенных.", create_levels_menu_keyboard())

        elif state_data['step'] == 'level_input':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
            elif text == "Назад":
                state_data['step'] = 'range_selection'
                await state.update_message(chat_id, "Возврат к выбору диапазона.", create_levels_menu_keyboard())
            elif text == "Добавить еще уровень" or text == "Завершить":
                await state.update_message(chat_id, "Сначала введите уровень.", create_level_input_keyboard())
            else:
                try:
                    text_normalized = text.replace(',', '.')
                    level = Decimal(text_normalized)
                    min_val, max_val = state_data['range']
                    if not (min_val <= float(level) <= max_val):
                        await state.update_message(chat_id, f"Ошибка: введите значение в диапазоне {min_val}–{max_val}", create_level_input_keyboard())
                        return
                    if level not in state_data['levels']:
                        state_data['levels'].append(level)
                        await state.save_levels(chat_id, state_data['levels'])
                    if len(state_data['levels']) >= 100:
                        del state.pending_commands[chat_id]
                        await state.update_message(chat_id, "Достигнут лимит в 100 уровней. Уровни сохранены.", create_main_keyboard(chat_id))
                    else:
                        state_data['step'] = 'level_choice'
                        await state.update_message(chat_id, f"Уровень {level:.6f} добавлен. Что дальше?", create_level_input_keyboard())
                except ValueError:
                    await state.update_message(chat_id, "Ошибка: введите корректное число (используйте точку или запятую)", create_level_input_keyboard())

        elif state_data['step'] == 'level_choice':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Действие отменено.", create_main_keyboard(chat_id))
            elif text == "Назад":
                state_data['step'] = 'level_input'
                min_val, max_val = state_data['range']
                await state.update_message(chat_id, f"Введите уровень от {min_val} до {max_val}:", create_level_input_keyboard())
            elif text == "Добавить еще уровень":
                state_data['step'] = 'level_input'
                min_val, max_val = state_data['range']
                await state.update_message(chat_id, f"Введите следующий уровень (в пределах {min_val}–{max_val}):", create_level_input_keyboard())
            elif text == "Завершить":
                await state.save_levels(chat_id, state_data['levels'])
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Уровни сохранены.", create_main_keyboard(chat_id))
            else:
                try:
                    text_normalized = text.replace(',', '.')
                    level = Decimal(text_normalized)
                    min_val, max_val = state_data['range']
                    if not (min_val <= float(level) <= max_val):
                        await state.update_message(chat_id, f"Ошибка: введите значение в диапазоне {min_val}–{max_val}", create_level_input_keyboard())
                        return
                    if level not in state_data['levels']:
                        state_data['levels'].append(level)
                        await state.save_levels(chat_id, state_data['levels'])
                    if len(state_data['levels']) >= 100:
                        del state.pending_commands[chat_id]
                        await state.update_message(chat_id, "Достигнут лимит в 100 уровней. Уровни сохранены.", create_main_keyboard(chat_id))
                    else:
                        state_data['step'] = 'level_choice'
                        await state.update_message(chat_id, f"Уровень {level:.6f} добавлен. Что дальше?", create_level_input_keyboard())
                except ValueError:
                    await state.update_message(chat_id, "Ошибка: введите корректное число (используйте точку или запятую)", create_level_input_keyboard())

        elif state_data['step'] == 'delete_level_selection':
            if text == "Отмена":
                del state.pending_commands[chat_id]
                await state.update_message(chat_id, "Удаление уровней отменено.", create_main_keyboard(chat_id))
            elif text == "Назад":
                state_data['step'] = 'range_selection'
                await state.update_message(chat_id, "Возврат к выбору действия.", create_levels_menu_keyboard())
            elif text.startswith("Удалить "):
                level_str = text.replace("Удалить ", "").replace(" Gwei", "")
                try:
                    level_to_delete = Decimal(level_str)
                    if level_to_delete in state.user_states[chat_id]['current_levels']:
                        state.user_states[chat_id]['current_levels'].remove(level_to_delete)
                        if level_to_delete in state.user_states[chat_id]['user_added_levels']:
                            state.user_states[chat_id]['user_added_levels'].remove(level_to_delete)
                        await state.save_levels(chat_id, state.user_states[chat_id]['current_levels'])
                        del state.pending_commands[chat_id]
                        await state.update_message(chat_id, f"Уровень {level_to_delete:.6f} Gwei удалён.", create_main_keyboard(chat_id))
                    else:
                        await state.update_message(chat_id, "Уровень не найден.", create_main_keyboard(chat_id))
                        del state.pending_commands[chat_id]
                except ValueError:
                    await state.update_message(chat_id, "Ошибка при удалении уровня.", create_delete_levels_keyboard(state.user_states[chat_id]['current_levels']))
            else:
                await state.update_message(chat_id, "Выберите уровень для удаления.", create_delete_levels_keyboard(state.user_states[chat_id]['current_levels']))

    try:
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete user message_id={message.message_id}: {e}")

async def monitor_gas_callback(gas_value):
    logger.debug(f"monitor_gas_callback started with gas_value={gas_value:.6f}")
    if state.is_first_run:
        for user_id, _ in ALLOWED_USERS:
            state.user_states[user_id]['last_measured_gas'] = gas_value
            state.user_states[user_id]['prev_level'] = gas_value
            logger.info(f"First run: Set initial gas value for user_id={user_id}: {gas_value:.6f}")
        state.is_first_run = False
        logger.debug("monitor_gas_callback completed first run")
        return
    for user_id, _ in ALLOWED_USERS:
        try:
            await asyncio.sleep(1)
            logger.debug(f"Calling get_manta_gas for user_id={user_id} with force_base_message=False")
            await state.get_manta_gas(user_id, force_base_message=False)
            logger.debug(f"get_manta_gas completed for user_id={user_id}")
        except Exception as e:
            logger.error(f"Unexpected error in monitor_gas_callback for user_id={user_id}: {str(e)}")
    logger.debug("monitor_gas_callback completed")